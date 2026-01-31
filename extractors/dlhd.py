import asyncio
import logging
import re
import base64
import json
import os
import gzip
import zlib
import zstandard
import random
import time
from urllib.parse import urlparse, quote_plus
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector, FormData
from aiohttp_socks import ProxyConnector
from typing import Dict, Any, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class ExtractorError(Exception):
    pass

class DLHDExtractor:
    """DLHD Extractor con sessione persistente e gestione anti-bot avanzata"""

    # Constants
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    CHANNEL_ID_PATTERNS = [
        r'/premium(\d+)/mono',
        r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
        r'watch\.php\?id=(\d+)',
        r'(?:%2F|/)stream-(\d+)\.php',
        r'stream-(\d+)\.php',
        r'[?&]id=(\d+)',
        r'daddyhd\.php\?id=(\d+)',
    ]

    def __init__(self, request_headers: dict, proxies: list = None):
        self.request_headers = request_headers
        self.base_headers = {
            "user-agent": self.USER_AGENT,
        }
        self.session = None
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._session_lock = asyncio.Lock()
        self.proxies = proxies or []
        self._extraction_locks: Dict[str, asyncio.Lock] = {} # ✅ NUOVO: Lock per evitare estrazioni multiple
        self.cache_file = os.path.join(os.path.dirname(__file__), '.dlhd_cache')
        
        # Carica cache e inizializza host
        cache_data = self._load_cache()
        self._stream_data_cache: Dict[str, Dict[str, Any]] = cache_data.get('streams', {})
        
        # ✅ Lista host iframe (caricata da cache o vuota)
        self.iframe_hosts = cache_data.get('hosts', [])

        # ✅ Configurazione server dinamica dal worker (usando TEMPLATE completi)
        # Tutti i valori provengono dal worker, i fallback sono solo per il primo avvio
        self.auth_url = cache_data.get('auth_url', 'https://security.kiko2.ru/auth2.php')
        self.stream_cdn_template = cache_data.get('stream_cdn_template', 'https://top1.kiko2.ru/top1/cdn/{CHANNEL}/mono.css')
        self.stream_other_template = cache_data.get('stream_other_template', 'https://{SERVER_KEY}new.kiko2.ru/{SERVER_KEY}/{CHANNEL}/mono.css')
        self.server_lookup_url = cache_data.get('server_lookup_url', 'https://chevy.kiko2.ru/server_lookup')
        self.base_domain = cache_data.get('base_domain', 'kiko2.ru')
        
        logger.info(f"Hosts loaded at startup: {self.iframe_hosts}")
        logger.info(f"Auth URL: {self.auth_url}")
        logger.info(f"Stream CDN Template: {self.stream_cdn_template}")
        logger.info(f"Stream Other Template: {self.stream_other_template}")
        logger.info(f"Server Lookup URL: {self.server_lookup_url}")
        logger.info(f"Base Domain: {self.base_domain}")

    def _load_cache(self) -> Dict[str, Any]:
        """Carica la cache da un file codificato in Base64 all'avvio. Ritorna struttura completa."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    logger.info(f"💾 Loading cache from file: {self.cache_file}")
                    encoded_data = f.read()
                    if not encoded_data:
                         # Tenta di gestire il vecchio formato o ritorna default
                        return {'hosts': [], 'streams': {}}
                    
                    try:
                        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
                        data = json.loads(decoded_data)
                        
                        # Retrocompatibilità: se la cache è il vecchio formato (dict di stream), convertilo
                        if data and not ('hosts' in data and 'streams' in data):
                             # Assumiamo che keys siano channel_ids
                             # Verifica euristica: se le chiavi sono stringhe numeriche, è il vecchio formato
                             is_old_format = any(k.isdigit() for k in data.keys())
                             if is_old_format or not data:
                                 logger.info("Detected old cache format, converting...")
                                 return {'hosts': [], 'streams': data}
                        
                        return data
                    except Exception:
                        # Se fallisce decode, prova a leggerlo come JSON plain (fallback)
                        f.seek(0)
                        return json.load(f)

        except (IOError, json.JSONDecodeError, Exception) as e:
            logger.error(f"❌ Error loading cache: {e}. Starting with empty cache.")
        return {'hosts': [], 'streams': {}}

    def _get_random_proxy(self):
        """Restituisce un proxy casuale dalla lista."""
        return random.choice(self.proxies) if self.proxies else None

    async def _get_session(self):
        """✅ Sessione persistente con cookie jar automatico"""
        if self.session is None or self.session.closed:
            timeout = ClientTimeout(total=60, connect=30, sock_read=30)
            proxy = self._get_random_proxy()
            if proxy:
                logger.info(f"🔗 Using proxy {proxy} for DLHD session.")
                connector = ProxyConnector.from_url(proxy, ssl=False)
            else:
                connector = TCPConnector(
                    limit=0,
                    limit_per_host=0,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True,
                    force_close=False,
                    use_dns_cache=True
                )
                logger.info("ℹ️ No specific proxy for DLHD, using direct connection.")
            # ✅ FONDAMENTALE: Cookie jar per mantenere sessione come browser reale
            self.session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.base_headers,
                cookie_jar=aiohttp.CookieJar()
            )
        return self.session

    def _save_cache(self):
        """Salva lo stato corrente della cache su un file, codificando il contenuto in Base64."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                cache_data = {
                    'hosts': self.iframe_hosts,
                    'streams': self._stream_data_cache,
                    'auth_url': self.auth_url,
                    'stream_cdn_template': self.stream_cdn_template,
                    'stream_other_template': self.stream_other_template,
                    'server_lookup_url': self.server_lookup_url,
                    'base_domain': self.base_domain
                }
                json_data = json.dumps(cache_data)
                encoded_data = base64.b64encode(json_data.encode('utf-8')).decode('utf-8')
                f.write(encoded_data)
                logger.info(f"💾 Cache (stream + hosts) saved successfully.")
        except IOError as e:
            logger.error(f"❌ Error saving cache: {e}")

    @staticmethod
    def extract_channel_id(url: str) -> Optional[str]:
        """Extract channel ID from URL using predefined patterns."""
        for pattern in DLHDExtractor.CHANNEL_ID_PATTERNS:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _build_stream_url(self, server_key: str, channel_key: str) -> str:
        """Build stream URL using server key and channel key."""
        if server_key == 'top1/cdn':
            return self.stream_cdn_template.replace('{CHANNEL}', channel_key)
        else:
            return self.stream_other_template.replace('{SERVER_KEY}', server_key).replace('{CHANNEL}', channel_key)

    def _build_stream_headers(self, iframe_url: str, channel_key: str, auth_token: str, secret_key: str = None) -> dict:
        """Build standard stream headers."""
        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        headers = {
            'User-Agent': self.USER_AGENT,
            'Referer': iframe_url,
            'Origin': iframe_origin,
            'Authorization': f'Bearer {auth_token}',
            'X-Channel-Key': channel_key,
            'X-User-Agent': self.USER_AGENT,
        }
        if secret_key:
            headers['X-Secret-Key'] = secret_key
        return headers

    async def _fetch_server_key(self, channel_key: str, iframe_url: str) -> str:
        """Fetch server key for a given channel."""
        server_lookup_url = f"{self.server_lookup_url}?channel_id={channel_key}"
        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        lookup_headers = {
            'User-Agent': self.USER_AGENT,
            'Accept': '*/*',
            'Referer': iframe_url,
            'Origin': iframe_origin,
        }
        lookup_resp = await self._make_robust_request(server_lookup_url, headers=lookup_headers, retries=2)
        server_data = await lookup_resp.json()
        server_key = server_data.get('server_key')
        if not server_key:
            raise ExtractorError(f"No server_key in response: {server_data}")
        return server_key

    async def _fetch_iframe_hosts(self) -> bool:
        """Scarica la lista aggiornata degli host iframe."""
        # URL offuscato per evitare scraping statico
        encoded_url = "aHR0cHM6Ly9pZnJhbWUuZGxoZC5kcGRucy5vcmcv"
        url = base64.b64decode(encoded_url).decode('utf-8')
        
        logger.info(f"🔄 Updating iframe host list...")
        try:
            session = await self._get_session()
            async with session.get(url, ssl=False, timeout=ClientTimeout(total=10)) as response:
                if response.status == 200:
                    text = await response.text()
                    # ✅ Parsing con supporto per configurazione completa
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    new_hosts = []
                    
                    for line in lines:
                        if line.startswith('#AUTH_URL:'):
                            self.auth_url = line.replace('#AUTH_URL:', '').strip()
                            logger.info(f"✅ Auth URL aggiornato: {self.auth_url}")
                        elif line.startswith('#STREAM_CDN_TEMPLATE:'):
                            self.stream_cdn_template = line.replace('#STREAM_CDN_TEMPLATE:', '').strip()
                            logger.info(f"✅ Stream CDN Template aggiornato: {self.stream_cdn_template}")
                        elif line.startswith('#STREAM_OTHER_TEMPLATE:'):
                            self.stream_other_template = line.replace('#STREAM_OTHER_TEMPLATE:', '').strip()
                            logger.info(f"✅ Stream Other Template aggiornato: {self.stream_other_template}")
                        elif line.startswith('#SERVER_LOOKUP_URL:'):
                            self.server_lookup_url = line.replace('#SERVER_LOOKUP_URL:', '').strip()
                            logger.info(f"✅ Server Lookup URL aggiornato: {self.server_lookup_url}")
                        elif line.startswith('#BASE_DOMAIN:'):
                            self.base_domain = line.replace('#BASE_DOMAIN:', '').strip()
                            logger.info(f"✅ Base Domain aggiornato: {self.base_domain}")
                        elif not line.startswith('#'):
                            new_hosts.append(line)
                    
                    if new_hosts:
                        self.iframe_hosts = new_hosts
                        logger.info(f"✅ Host list updated: {self.iframe_hosts}")
                        self._save_cache()
                        return True
                    else:
                         logger.warning("⚠️ Host list downloaded but empty.")
                else:
                    logger.error(f"❌ HTTP error {response.status} while updating iframe host.")
        except Exception as e:
            logger.error(f"❌ Exception while updating iframe host: {e}")
        
        return False

    def _get_headers_for_url(self, url: str, base_headers: dict) -> dict:
        """Applica headers specifici per il dominio stream automaticamente"""
        headers = base_headers.copy()
        parsed_url = urlparse(url)

        # Usa base_domain dinamico dal worker
        stream_domain = self.base_domain

        if stream_domain in parsed_url.netloc:
            origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
            special_headers = {
                'User-Agent': self.USER_AGENT,
                'Referer': origin,
                'Origin': origin
            }
            headers.update(special_headers)

        return headers

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        """Gestisce la decompressione manuale del corpo della risposta (zstd, gzip, etc.)."""
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd':
                logger.info(f"Detected zstd compression for {response.url}. Decompressing...")
                try:
                    dctx = zstandard.ZstdDecompressor()
                    # ✅ MODIFICA: Utilizza stream_reader per gestire frame senza dimensione del contenuto.
                    # Questo risolve l'errore "could not determine content size in frame header".
                    with dctx.stream_reader(raw_body) as reader:
                        decompressed_body = reader.read()
                    return decompressed_body.decode(response.charset or 'utf-8')
                except zstandard.ZstdError as e:
                    logger.error(f"Zstd decompression error: {e}. Content may be incomplete or corrupted.")
                    raise ExtractorError(f"Fallimento decompressione zstd: {e}")
            elif content_encoding == 'gzip':
                logger.info(f"Detected gzip compression for {response.url}. Decompressing...")
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                logger.info(f"Detected deflate compression for {response.url}. Decompressing...")
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                # Nessuna compressione o compressione non gestita, prova a decodificare direttamente
                # ✅ FIX: Usa 'errors=replace' per evitare crash su byte non validi
                return raw_body.decode(response.charset or 'utf-8', errors='replace')
        except Exception as e:
            logger.error(f"Error during decompression/decoding of content from {response.url}: {e}")
            raise ExtractorError(f"Decompression failure for {response.url}: {e}")

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        """✅ Richieste con sessione persistente per evitare anti-bot"""
        final_headers = self._get_headers_for_url(url, headers or {})
        # Aggiungiamo zstd agli header accettati per segnalare al server che lo supportiamo
        # Rimosso 'br' perché non gestito in _handle_response_content
        final_headers['Accept-Encoding'] = 'gzip, deflate, zstd'
        
        for attempt in range(retries):
            try:
                # ✅ IMPORTANTE: Riusa sempre la stessa sessione
                session = await self._get_session()
                
                logger.info(f"Attempt {attempt + 1}/{retries} for URL: {url}")
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    class MockResponse:
                        def __init__(self, text_content, status, headers_dict):
                            self._text = text_content
                            self.status = status
                            self.headers = headers_dict
                            self.url = url
                        
                        async def text(self):
                            return self._text
                            
                        def raise_for_status(self):
                            if self.status >= 400:
                                raise aiohttp.ClientResponseError(
                                    request_info=None, 
                                    history=None,
                                    status=self.status
                                )
                        
                        async def json(self):
                            return json.loads(self._text)
                    
                    logger.info(f"✅ Request successful for {url} at attempt {attempt + 1}")
                    return MockResponse(content, response.status, response.headers)
                    
            except (
                aiohttp.ClientConnectionError, 
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientPayloadError,
                asyncio.TimeoutError,
                OSError,
                ConnectionResetError,
            ) as e:
                logger.warning(f"⚠️ Connection error attempt {attempt + 1} for {url}: {str(e)}")
                
                # ✅ Solo in caso di errore critico, chiudi la sessione
                if attempt == retries - 1:
                    if self.session and not self.session.closed:
                        try:
                            await self.session.close()
                        except:
                            pass
                    self.session = None
                
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.info(f"⏳ Waiting {delay} seconds before next attempt...")
                    await asyncio.sleep(delay)
                else:
                    raise ExtractorError(f"All {retries} attempts failed for {url}: {str(e)}")
                    
            except Exception as e:
                # Controlla se l'errore è dovuto a zstd e logga un messaggio specifico
                if 'zstd' in str(e).lower():
                    logger.critical(f"❌ Critical error with zstd decompression. Ensure 'zstandard' library is installed (`pip install zstandard`). Error: {e}")
                else: # type: ignore
                    logger.error(f"❌ Non-network error attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == retries - 1:
                    raise ExtractorError(f"Final error for {url}: {str(e)}")
        await asyncio.sleep(initial_delay)

    async def extract(self, url: str, force_refresh: bool = False, **kwargs) -> Dict[str, Any]:
        """Flusso di estrazione principale: estrae direttamente dall'iframe."""
        
        async def get_stream_data_direct(channel_id: str, hosts_to_try: list) -> Dict[str, Any]:
            """Estrazione diretta dall'iframe senza passare per la pagina principale."""
            last_error = None
            for iframe_host in hosts_to_try:
                try:
                    iframe_url = f'https://{iframe_host}/premiumtv/daddyhd.php?id={channel_id}'
                    logger.info(f"🔍 Attempting extraction from: {iframe_url}")
                    
                    embed_headers = {
                        'User-Agent': self.USER_AGENT,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Referer': 'https://dlhd.dad/',
                        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }
                    
                    # Step 1: Fetch iframe page
                    resp = await self._make_robust_request(iframe_url, headers=embed_headers, retries=2)
                    js_content = await resp.text()
                    
                    # Check if it's lovecdn
                    if 'lovecdn.ru' in js_content:
                        logger.info("Detected lovecdn.ru content - using alternative extraction")
                        result = await self._extract_lovecdn_stream(iframe_url, js_content)
                        return result
                    
                    # Step 2: Extract auth params
                    params = {}
                    patterns = {
                        'channel_key': r'(?:const|var|let)\s+(?:CHANNEL_KEY|channelKey)\s*=\s*["\']([^"\']+)["\']',
                        'auth_token': r'(?:const|var|let)\s+AUTH_TOKEN\s*=\s*["\']([^"\']+)["\']',
                        'auth_country': r'(?:const|var|let)\s+AUTH_COUNTRY\s*=\s*["\']([^"\']+)["\']',
                        'auth_ts': r'(?:const|var|let)\s+AUTH_TS\s*=\s*["\']([^"\']+)["\']',
                        'auth_expiry': r'(?:const|var|let)\s+AUTH_EXPIRY\s*=\s*["\']([^"\']+)["\']',
                    }
                    for key, pattern in patterns.items():
                        match = re.search(pattern, js_content)
                        params[key] = match.group(1) if match else None
                    
                    missing_params = [k for k, v in params.items() if not v]
                    if missing_params:
                        logger.warning(f"⚠️ Missing parameters from {iframe_host}: {missing_params}. Attempting new heuristic flow...")
                        try:
                            # Se mancano i parametri standard, prova il nuovo flusso euristico/obfuscated
                            result = await self._extract_new_auth_flow(iframe_url, js_content)
                            return result
                        except Exception as e:
                            logger.warning(f"⚠️ Nuovo flusso fallito: {e}")
                            last_error = ExtractorError(f"Missing params: {missing_params} and New Flow failed")
                            continue
                    
                    logger.info(f"✅ Parameters extracted: channel_key={params['channel_key']}")
                    
                    # Step 3: Auth POST
                    # ✅ DINAMICO: usa self.auth_url completo
                    auth_url = self.auth_url
                    logger.info(f"🔐 Using auth_url: {auth_url}")
                    iframe_origin = f"https://{iframe_host}"
                    
                    form_data = FormData()
                    form_data.add_field('channelKey', params['channel_key'])
                    form_data.add_field('country', params['auth_country'])
                    form_data.add_field('timestamp', params['auth_ts'])
                    form_data.add_field('expiry', params['auth_expiry'])
                    form_data.add_field('token', params['auth_token'])
                    
                    auth_headers = {
                        'User-Agent': self.USER_AGENT,
                        'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Origin': iframe_origin,
                        'Referer': iframe_url,
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'cross-site',
                        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }
                    
                    session = await self._get_session()
                    async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False) as auth_resp:
                        auth_text = await auth_resp.text()
                        logger.info(f"Auth response: {auth_resp.status} - {auth_text[:100]}")
                        
                        if auth_resp.status != 200 or 'Blocked' in auth_text or 'bad params' in auth_text.lower():
                            logger.warning(f"⚠️ Auth bloccato da {iframe_host}: {auth_text[:50]}")
                            
                            
                            # ✅ NUOVO: Se è il primo host e auth fallisce, prova a refreshare config
                            if iframe_host == hosts_to_try[0] and not getattr(self, '_config_refreshed', False):
                                logger.info("🔄 Auth fallito, provo ad aggiornare config dal worker...")
                                self._config_refreshed = True
                                if await self._fetch_iframe_hosts():
                                    # Aggiorna auth_url per il prossimo tentativo
                                    auth_url = self.auth_url
                                    logger.info(f"✅ Config aggiornata, nuovo auth_url: {auth_url}")
                            
                            # ✅ TENTATIVO NUOVO FLUSSO se Auth fallisce (es. token invalidi)
                            logger.warning("⚠️ Auth fallito con metodo standard. Tento nuovo flusso euristico...")
                            try:
                                result = await self._extract_new_auth_flow(iframe_url, js_content)
                                return result
                            except Exception as e:
                                logger.warning(f"⚠️ Nuovo flusso (fallback) fallito: {e}")
                                last_error = ExtractorError(f"Auth blocked: {auth_text} AND New Flow failed: {e}")
                                continue
                        
                        try:
                            auth_data = json.loads(auth_text)
                            if not (auth_data.get('success') or auth_data.get('valid')):
                                logger.warning(f"⚠️ Auth fallito: {auth_data}")
                                last_error = ExtractorError(f"Auth failed: {auth_data}")
                                continue
                        except json.JSONDecodeError:
                            last_error = ExtractorError(f"Auth response not JSON: {auth_text}")
                            continue
                    
                    logger.info("✅ Auth successful!")
                    
                    # ✅ DEBUG: Log cookies e headers dalla risposta auth
                    auth_cookies = auth_resp.cookies
                    logger.info(f"🍪 Cookies dalla risposta auth: {dict(auth_cookies)}")
                    logger.info(f"📋 Headers dalla risposta auth: {dict(auth_resp.headers)}")
                    
                    # Log tutti i cookies nella session dopo auth
                    all_session_cookies = list(session.cookie_jar)
                    logger.info(f"🍪 Tutti i cookies nella sessione dopo auth: {all_session_cookies}")
                    
                    # Step 4: Server Lookup
                    server_key = await self._fetch_server_key(params['channel_key'], iframe_url)
                    logger.info(f"✅ Server key: {server_key}")

                    channel_key = params['channel_key']
                    auth_token = params['auth_token']

                    # Build final URL using helper method
                    stream_url = self._build_stream_url(server_key, channel_key)
                    logger.info(f"✅ Stream URL costruito: {stream_url}")

                    # Build headers using helper method
                    stream_headers = self._build_stream_headers(iframe_url, channel_key, auth_token)
                    stream_headers['X-User-Agent'] = self.USER_AGENT  # Add for compatibility

                    # ✅ Aggiungi cookies dalla sessione corrente
                    if self.session:
                        # Log all cookies for debugging
                        all_cookies = list(self.session.cookie_jar)
                        logger.info(f"🍪 All cookies in jar: {all_cookies}")
                        
                        cookies = self.session.cookie_jar.filter_cookies(stream_url)
                        cookie_str = "; ".join([f"{k}={v.value}" for k, v in cookies.items()])
                        if cookie_str:
                            stream_headers['Cookie'] = cookie_str
                            logger.info(f"🍪 Cookies added to headers: {cookie_str[:50]}...")

                    expires_at = None
                    try:
                        if params.get('auth_expiry'):
                            expires_at = float(params['auth_expiry'])
                            logger.info(f"⏳ Auth Expiry: {expires_at} (Current time: {time.time()})")
                    except (ValueError, TypeError):
                        pass
                    
                    # ✅ Reset flag per permettere futuri refresh
                    self._config_refreshed = False
                    
                    return {
                        "destination_url": stream_url,
                        "request_headers": stream_headers,
                        "mediaflow_endpoint": self.mediaflow_endpoint,
                        "expires_at": expires_at
                    }
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error with {iframe_host}: {e}")
                    last_error = e
                    continue
            
            raise ExtractorError(f"All iframe hosts failed. Last error: {last_error}")

        try:
            channel_id = self.extract_channel_id(url)
            if not channel_id:
                raise ExtractorError(f"Unable to extract channel ID from {url}")

            logger.info(f"📺 Extraction for channel ID: {channel_id}")

            # Controlla la cache prima di procedere
            if not force_refresh and channel_id in self._stream_data_cache:
                logger.info(f"✅ Found cached data for channel ID: {channel_id}. Verifying validity...")
                cached_data = self._stream_data_cache[channel_id]
                stream_url = cached_data.get("destination_url")
                stream_headers = cached_data.get("request_headers", {})
                expires_at = cached_data.get("expires_at")

                is_valid = False
                
                # ✅ Check expiry first (con buffer di 30 secondi)
                if expires_at and time.time() > (expires_at - 30):
                     logger.warning(f"⚠️ Cache expired for channel ID {channel_id} (expires_at: {expires_at}).")
                     is_valid = False
                elif stream_url:
                    try:
                        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as validation_session:
                            async with validation_session.head(stream_url, headers=stream_headers, ssl=False) as response:
                                if response.status == 200:
                                    is_valid = True
                                    logger.info(f"✅ Cache for channel ID {channel_id} is valid.")
                                else:
                                    logger.warning(f"⚠️ Cache for channel ID {channel_id} not valid. Status: {response.status}. Proceeding with extraction.")
                    except Exception as e:
                        logger.warning(f"⚠️ Error during cache validation for {channel_id}: {e}. Proceeding with extraction.")
                
                if not is_valid:
                    if channel_id in self._stream_data_cache:
                        del self._stream_data_cache[channel_id]
                    self._save_cache()
                    logger.info(f"🗑️ Cache invalidated for channel ID {channel_id}.")
                else:
                    return cached_data

            # Usa un lock per prevenire estrazioni simultanee per lo stesso canale
            if channel_id not in self._extraction_locks:
                self._extraction_locks[channel_id] = asyncio.Lock()
            
            lock = self._extraction_locks[channel_id]
            async with lock:
                # Ricontrolla la cache dopo aver acquisito il lock
                if not force_refresh and channel_id in self._stream_data_cache:
                    cached_data = self._stream_data_cache[channel_id]
                    expires_at = cached_data.get("expires_at")
                    
                    # Se è scaduta anche la nuova cache (improbabile ma possibile), procedi con estrazione
                    if expires_at and time.time() > (expires_at - 30):
                        logger.info(f"⚠️ Cache (rechecked) expired for {channel_id}, proceeding with new extraction.")
                    else:
                        logger.info(f"✅ Data for channel {channel_id} found in cache after waiting for lock.")
                        return self._stream_data_cache[channel_id]

                # Procedi con l'estrazione diretta
                # Procedi con l'estrazione diretta
                logger.info(f"⚙️ No valid cache for {channel_id}, starting direct extraction...")
                
                try:
                    result = await get_stream_data_direct(channel_id, self.iframe_hosts)
                except ExtractorError:
                    # Se fallisce con gli host correnti, prova ad aggiornarli
                    logger.warning("⚠️ All current hosts failed. Attempting to update host list...")
                    if await self._fetch_iframe_hosts():
                         logger.info(f"🔄 Retrying with new hosts: {self.iframe_hosts}")
                         result = await get_stream_data_direct(channel_id, self.iframe_hosts)
                    else:
                        raise
                
                # Salva in cache
                self._stream_data_cache[channel_id] = result
                self._save_cache()
                
                return result
            
        except Exception as e:
            # Per errori 403, non loggare il traceback perché sono errori attesi (servizio temporaneamente non disponibile)
            error_message = str(e)
            if "403" in error_message or "Forbidden" in error_message:
                logger.error(f"DLHD extraction completely failed for URL {url}")
            else:
                logger.exception(f"DLHD extraction completely failed for URL {url}")
            raise ExtractorError(f"DLHD extraction completely failed: {str(e)}")

    async def _extract_lovecdn_stream(self, iframe_url: str, iframe_content: str) -> Dict[str, Any]:
        """
        Estrattore alternativo per iframe lovecdn.ru che usa un formato diverso.
        """
        try:
            # Cerca pattern di stream URL diretto
            m3u8_patterns = [
                r'["\']([^"\']*\.m3u8[^"\']*)["\']',
                r'source[:\s]+["\']([^"\']+)["\']',
                r'file[:\s]+["\']([^"\']+\.m3u8[^"\']*)["\']',
                r'hlsManifestUrl[:\s]*["\']([^"\']+)["\']',
            ]
            
            stream_url = None
            for pattern in m3u8_patterns:
                matches = re.findall(pattern, iframe_content)
                for match in matches:
                    if '.m3u8' in match and match.startswith('http'):
                        stream_url = match
                        logger.info(f"Found direct m3u8 URL: {stream_url}")
                        break
                if stream_url:
                    break
            
            # Pattern 2: Cerca costruzione dinamica URL
            if not stream_url:
                channel_match = re.search(r'(?:stream|channel)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                server_match = re.search(r'(?:server|domain|host)["\s:=]+["\']([^"\']+)["\']', iframe_content)
                
                if channel_match:
                    channel_name = channel_match.group(1)
                    # Usa base_domain dinamico
                    server = server_match.group(1) if server_match else self.base_domain
                    stream_url = f"https://{server}/{channel_name}/mono.m3u8"
                    logger.info(f"Constructed stream URL: {stream_url}")
            
            if not stream_url:
                # Fallback: cerca qualsiasi URL che sembri uno stream
                url_pattern = r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*'
                matches = re.findall(url_pattern, iframe_content)
                if matches:
                    stream_url = matches[0]
                    logger.info(f"Found fallback stream URL: {stream_url}")
            
            if not stream_url:
                raise ExtractorError(f"Could not find stream URL in lovecdn.ru iframe")
            
            # Usa iframe URL come referer
            iframe_origin = f"https://{urlparse(iframe_url).netloc}"
            stream_headers = {
                'User-Agent': self.USER_AGENT,
                'Referer': iframe_url,
                'Origin': iframe_origin
            }
            
            # Determina endpoint in base al dominio dello stream
            # Se è un m3u8 standard, usa hls_manifest_proxy, se richiede chiavi speciali, potrebbe servire altro
            # Ma per ora manteniamo hls_manifest_proxy come nel codice originale
            
            logger.info(f"Using lovecdn.ru stream extraction")
            
            return {
                "destination_url": stream_url,
                "request_headers": stream_headers,
                "mediaflow_endpoint": self.mediaflow_endpoint,
            }
            
        except Exception as e:
            raise ExtractorError(f"Failed to extract lovecdn.ru stream: {e}")

    def _extract_secret_key(self, iframe_html: str, channel_key: str | None = None) -> str | None:
        """
        Extract the HMAC secret key from the iframe HTML response.

        This function dynamically finds the secret key by:
        1. Locating the nonce calculation code block
        2. Extracting the variable name used in HMAC-SHA256 for the resource
        3. Finding the definition of that variable and decoding its base64 value

        Args:
            iframe_html: The HTML content from the iframe URL
            channel_key: The channel key to exclude from results (avoid matching it)

        Returns:
            The decoded secret key or None if not found
        """

        # Step 1: Find the nonce calculation block to identify the secret variable name
        # Pattern: CryptoJS.HmacSHA256(resource,_SECRET_VAR).toString()
        # This appears in the nonce calculation loop
        hmac_pattern = r'CryptoJS\.HmacSHA256\(resource,\s*([a-zA-Z_$][\w$]*)\)'
        hmac_match = re.search(hmac_pattern, iframe_html)

        if not hmac_match:
            # Fallback: try finding HMAC with a variable in any context
            hmac_pattern_general = r'HmacSHA256\([^,]+,\s*([a-zA-Z_$][\w$]*)\)'
            hmac_match = re.search(hmac_pattern_general, iframe_html)

        if not hmac_match:
            return None

        secret_var_name = hmac_match.group(1)

        # Step 2: Find the line containing "let _varname=" and extract base64 from that line
        # Two cases:
        # Case A: let _var="part1"+"part2"+...;
        # Case B: const _array=["part1","part2",...];let _var=_array.map(x=>x).join('');

        # Pattern to find the line with "const _varname=" or "let _varname="
        const_pattern = rf'(?:let|const)\s+{re.escape(secret_var_name)}\s*='
        let_match = re.search(const_pattern, iframe_html)

        if not let_match:
            return None

        # Get the line containing the let statement
        # Find the start of the line (newline before the match)
        line_start = let_match.start()
        while line_start > 0 and iframe_html[line_start - 1] not in '\n\r':
            line_start -= 1

        # Find the end of the line (semicolon or newline)
        line_end = iframe_html.find(';', let_match.end())
        if line_end == -1:
            # Try to find newline
            line_end = iframe_html.find('\n', let_match.end())
            if line_end == -1:
                line_end = len(iframe_html)

        line_content = iframe_html[line_start:line_end + 1]

        # Extract all quoted base64 strings from this line
        base64_parts = re.findall(r'\"([A-Za-z0-9+/=]+)\"', line_content)

        if not base64_parts:
            return None

        combined_b64 = "".join(base64_parts)

        try:
            decoded = base64.b64decode(combined_b64).decode("utf-8")

            # Basic validation - secret keys are typically hex strings of reasonable length
            if len(decoded) < 8 or len(decoded) > 128:
                return None

            # Skip if it matches the channel_key (that's not the secret)
            if channel_key and decoded == channel_key:
                return None

            return decoded
        except Exception:
            pass

        return None

    def _extract_eplayer_auth_data(self, iframe_html: str) -> dict | None:
        """
        Extract auth_token, channel_key, and channel_salt from EPlayerAuth.init block.

        The EPlayerAuth.init block has this format:
        EPlayerAuth.init({
            authToken: 'premium860|IT|1769803458|1769889858|98cdbdf2b86c8c63a5fe59d779b3a33e976613d0ec726e74225ed469567eb3c3',
            channelKey: 'premium860',
            country: 'IT',
            timestamp: 1769803458,
            validDomain: 'codepcplay.fun',
            channelSalt: '5a64ab0085fb21c0402b6fcf3d753c3a91b660644c5afa186e046c9ca702bf4a'
        });

        Args:
            iframe_html: The HTML content from the iframe URL

        Returns:
            Dict with auth_token, channel_key, channel_salt, server_lookup_url or None
        """
        # Pattern to extract EPlayerAuth.init block
        auth_pattern = r"EPlayerAuth\.init\s*\(\s*\{\s*authToken:\s*'([^']+)'"
        channel_key_pattern = r"channelKey:\s*'([^']+)'"
        channel_salt_pattern = r"channelSalt:\s*'([^']+)'"

        # Pattern to extract server lookup base URL from fetchWithRetry call
        lookup_pattern = r"fetchWithRetry\s*\(\s*'([^']+server_lookup\?channel_id=)"

        auth_match = re.search(auth_pattern, iframe_html)
        channel_key_match = re.search(channel_key_pattern, iframe_html)
        channel_salt_match = re.search(channel_salt_pattern, iframe_html)
        lookup_match = re.search(lookup_pattern, iframe_html)

        if auth_match and channel_key_match and channel_salt_match:
            result = {
                "auth_token": auth_match.group(1),
                "channel_key": channel_key_match.group(1),
                "channel_salt": channel_salt_match.group(1),
            }
            if lookup_match:
                result["server_lookup_url"] = lookup_match.group(1) + result["channel_key"]

            return result

        return None

    def _extract_obfuscated_session_data(self, iframe_html: str) -> dict | None:
        """
        Extract session_token, channel_key, and secret_key from obfuscated JS.

        Handles the pattern where variables use obfuscated names like var_[a-f0-9]+.

        Args:
            iframe_html: The HTML content from the iframe URL

        Returns:
            Dict with session_token, channel_key, secret_key, server_lookup_url or None
        """
        # Pattern to match obfuscated variable names with JWT token (session_token)
        # First const after the block start, value starts with "eyJ"
        token_pattern = r'const\s+var_[a-f0-9]+\s*=\s*"(eyJ[^"]+)"'
        # Pattern to match channel_key: second const, right after the JWT token line
        key_pattern = r'const\s+var_[a-f0-9]+\s*=\s*"eyJ[^"]+";[\s\n]*const\s+var_[a-f0-9]+\s*=\s*"([^"]+)"'

        # Pattern to extract server lookup base URL from fetchWithRetry call
        lookup_pattern = r"fetchWithRetry\s*\(\s*'([^']+server_lookup\?channel_id=)"

        token_match = re.search(token_pattern, iframe_html)
        key_match = re.search(key_pattern, iframe_html)
        lookup_match = re.search(lookup_pattern, iframe_html)

        if token_match and key_match:
            result = {
                "session_token": token_match.group(1),
                "channel_key": key_match.group(1),
            }
            if lookup_match:
                result["server_lookup_url"] = lookup_match.group(1) + result["channel_key"]

            # Extract the HMAC secret key for computing nonce
            secret_key = self._extract_secret_key(iframe_html, result["channel_key"])
            if secret_key:
                result["secret_key"] = secret_key

            return result

        return None

    async def _extract_new_auth_flow(self, iframe_url: str, iframe_content: str) -> Dict[str, Any]:
        """Gestisce il nuovo flusso di autenticazione con estrazione euristica e supporto per nonce."""

        logger.info("Tentativo rilevamento nuovo flusso auth obfuscated...")

        # 1. First try EPlayerAuth.init extraction (new method)
        eplayer_data = self._extract_eplayer_auth_data(iframe_content)

        params = {}
        secret_key = None
        data_found = False

        if eplayer_data:
            logger.info("✅ Rilevato pattern EPlayerAuth.init")
            params['auth_token'] = eplayer_data.get('auth_token')
            params['channel_key'] = eplayer_data.get('channel_key')
            secret_key = eplayer_data.get('channel_salt')
            data_found = True
            if secret_key:
                logger.info(f"✅ Channel salt estratto per calcolo nonce")
        else:
            # 2. Fallback to obfuscated extraction (var_[a-f0-9]+)
            obfuscated_data = self._extract_obfuscated_session_data(iframe_content)

            if obfuscated_data:
                logger.info("✅ Rilevato pattern obfuscated (var_xxx)")
                params['auth_token'] = obfuscated_data.get('session_token')
                params['channel_key'] = obfuscated_data.get('channel_key')
                secret_key = obfuscated_data.get('secret_key')
                data_found = True
                if secret_key:
                    logger.info(f"✅ Secret key estratta per calcolo nonce")
            else:
                # Fallback: estrazione euristica originale
                logger.info("Pattern obfuscated non trovato, provo estrazione euristica...")

                # Cerca il JWT (inizia con eyJ...)
                jwt_match = re.search(r'["\'](eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+)["\']', iframe_content)
                if jwt_match:
                    params['auth_token'] = jwt_match.group(1)
                    logger.info("✅ Trovato possibile JWT Token")

                # Cerca Channel Key (es: premium853) - o stringa alfanumerica di media lunghezza
                key_matches = re.finditer(r'["\']([a-z]+[0-9]+)["\']', iframe_content)
                for m in key_matches:
                    val = m.group(1)
                    # Filtro euristico: deve sembrare una chiave canale
                    if re.match(r'^(premium|dad|sport|live)[0-9]+$', val):
                        params['channel_key'] = val
                        logger.info(f"✅ Trovata possibile Channel Key: {val}")
                        break

                # Prova a estrarre secret_key anche con estrazione euristica
                if params.get('channel_key'):
                    secret_key = self._extract_secret_key(iframe_content, params['channel_key'])
                    if secret_key:
                        logger.info(f"✅ Secret key estratta (fallback)")

        # Cerca Country (2 lettere maiuscole)
        country_match = re.search(r'["\']([A-Z]{2})["\']', iframe_content)
        if country_match:
            params['auth_country'] = country_match.group(1)
        else:
            params['auth_country'] = 'DE'  # Fallback

        # Cerca Timestamp (10 cifre)
        ts_matches = re.findall(r'["\']([0-9]{10})["\']', iframe_content)
        if ts_matches:
            ts_values = sorted([int(x) for x in ts_matches])
            params['auth_ts'] = str(ts_values[0])
            if len(ts_values) > 1:
                params['auth_expiry'] = str(ts_values[-1])
            else:
                params['auth_expiry'] = str(ts_values[0] + 3600)

        # Validazione minima
        if not params.get('auth_token'):
            raise ExtractorError("Unable to extract JWT from new flow.")

        # Se manca channel key, prova a estrarla dall'URL
        channel_key = params.get('channel_key')
        if not channel_key:
            m_url = re.search(r'id=([0-9]+)', iframe_url)
            if m_url:
                channel_key = f"premium{m_url.group(1)}"
                logger.info(f"⚠️ Channel Key not found in JS, guessed from URL: {channel_key}")
            else:
                raise ExtractorError("Channel Key missing and not derivable.")

        logger.info(f"✅ Extracted parameters: channel_key={channel_key}, has_secret_key={secret_key is not None}")

        # 2. SKIP auth2.php POST - Il nuovo flusso usa direttamente il token
        logger.info("🚀 Skipping auth2.php POST (new flow detected). Proceeding directly to server lookup.")

        auth_token = params['auth_token']

        # 3. Server Lookup - use helper method
        server_key = await self._fetch_server_key(channel_key, iframe_url)
        logger.info(f"✅ Server key: {server_key}")

        # Build Stream URL - use helper method
        stream_url = self._build_stream_url(server_key, channel_key)
        logger.info(f"✅ Stream URL built: {stream_url}")

        # Build headers - use helper method
        stream_headers = self._build_stream_headers(iframe_url, channel_key, auth_token, secret_key)
        stream_headers['X-User-Agent'] = self.USER_AGENT  # Add for compatibility

        if secret_key:
            logger.info("✅ Secret key added to headers for nonce calculation.")

        return {
            "destination_url": stream_url,
            "request_headers": stream_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
            "expires_at": float(params.get('auth_expiry', 0)),
            "secret_key": secret_key  # Incluso anche nel result per uso diretto
        }

    async def invalidate_cache_for_url(self, url: str):
        """
        Invalida la cache per un URL specifico.
        Questa funzione viene chiamata da app.py quando rileva un errore (es. fallimento chiave AES).
        """
        channel_id = self.extract_channel_id(url)
        if channel_id and channel_id in self._stream_data_cache:
            del self._stream_data_cache[channel_id]
            self._save_cache()
            logger.info(f"🗑️ Cache for channel ID {channel_id} invalidated due to external error (e.g. AES key).")

    async def close(self):
        """Chiude definitivamente la sessione"""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except:
                pass
        self.session = None
