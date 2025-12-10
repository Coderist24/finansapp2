import streamlit as st

# ============================================
# TEST MODU: Login Bypass (Geliştirme İçin)
# ============================================
TEST_MODE = False  # False yaparak normal login'i aktifleştirin
# ============================================
# Uyarı/success/info bastırma ayarı (varsayılan: kapalı). Hatalar (st.error) ASLA bastırılmaz.
SUPPRESS_NON_ERROR_ALERTS = False
try:
    _original_st_info = st.info
    _original_st_warning = st.warning
    _original_st_success = st.success
    _original_st_error = st.error

    # İsteğe bağlı olarak info/warning/success mesajlarını bastır (UI kalabalığını azaltmak için)
    if SUPPRESS_NON_ERROR_ALERTS:
        st.info = lambda *args, **kwargs: None
        st.warning = lambda *args, **kwargs: None
        st.success = lambda *args, **kwargs: None
    # st.error kesinlikle bastırılmaz; kırmızı hata kutuları görünür kalır
except Exception:
    # Streamlit yoksa veya override başarısızsa, hataya düşmeden devam et
    pass

# Hukuki dökümanlar
try:
    from legal_documents import get_document
except ImportError:
    def get_document(doc_type):
        return ""

import os
import tempfile

# Yahoo Finance timezone cache - import'tan sonra hemen ayarla (cache henüz oluşturulmamıştır)
import yfinance as yf
yf.set_tz_cache_location(os.path.join(tempfile.gettempdir(), "yfinance_cache"))

import pandas as pd
from datetime import datetime, timedelta, time as datetime_time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import hashlib
import json

# Yahoo Finance için özel session oluştur (curl_cffi ile)
try:
    from curl_cffi import requests as curl_requests
    USE_CURL_CFFI = True
except ImportError:
    USE_CURL_CFFI = False
    curl_requests = None

def create_yf_session():
    """Yahoo Finance için retry mekanizmalı session oluştur"""
    if USE_CURL_CFFI:
        # curl_cffi session (yfinance'ın yeni gereksinimi)
        session = curl_requests.Session()
    else:
        # Fallback: normal requests session
        session = requests.Session()
        
        # Retry stratejisi
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    
    # User-Agent header ekle (önemli!)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    
    return session

# Global yfinance session
YF_SESSION = create_yf_session()

# Chrome/Selenium patches for Azure container environment
import sys
try:
    # Patch 1: ChromeDriver path
    from webdriver_manager.chrome import ChromeDriverManager
    original_install = ChromeDriverManager.install
    
    def patched_install(self):
        chromedriver_path = '/usr/local/bin/chromedriver'
        if os.path.exists(chromedriver_path) and os.access(chromedriver_path, os.X_OK):
            return chromedriver_path
        return original_install(self)
    
    ChromeDriverManager.install = patched_install
    
    # Patch 2: Chrome profile management
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.webdriver import WebDriver as _OriginalChromeWebDriver

    def ChromeWithInMemoryProfile(*args, **kwargs):
        """In-memory Chrome profile for containerized environments"""
        # Determine or create Options
        if 'options' in kwargs:
            existing_options = kwargs['options']
        elif args and isinstance(args[0], ChromeOptions):
            existing_options = args[0]
        else:
            existing_options = None

        # Create a completely NEW options object
        new_options = ChromeOptions()

        # If there were existing options, copy relevant settings
        if existing_options:
            # Copy experimental options
            if hasattr(existing_options, '_experimental_options'):
                for key, value in existing_options._experimental_options.items():
                    new_options.set_capability(key, value)
            
            # Copy existing arguments BUT SKIP --user-data-dir entirely
            try:
                current_args = list(existing_options.arguments)
            except Exception:
                current_args = list(getattr(existing_options, '_arguments', []))
            
            for arg in current_args:
                # Skip --user-data-dir and --disk-cache-dir
                if not arg.startswith('--user-data-dir=') and not arg.startswith('--disk-cache-dir='):
                    new_options.add_argument(arg)

        # Force Chrome to run in memory-only mode
        # Use /dev/shm (shared memory filesystem) which is always available in Linux containers
        unique_profile = f'/dev/shm/chrome_profile_{os.getpid()}_{int(time.time()*1000000)}'
        try:
            os.makedirs(unique_profile, exist_ok=True)
            os.makedirs(os.path.join(unique_profile, 'cache'), exist_ok=True)
            os.chmod(unique_profile, 0o700)
        except Exception:
            pass
        new_options.add_argument(f'--user-data-dir={unique_profile}')
        new_options.add_argument(f'--disk-cache-dir={unique_profile}/cache')

        # Container-safe Chrome flags
        safe_flags = [
            '--no-sandbox',
            '--disable-extensions',
            '--disable-gpu',
            '--no-first-run',
            '--disable-background-networking',
            '--disable-features=VizDisplayCompositor',
            '--remote-debugging-port=0',
            '--incognito',  # Incognito mode for better isolation
            '--disable-application-cache',
            '--disable-cache',
            '--disk-cache-size=0',
            '--media-cache-size=0',
            '--window-size=1920,1080',
        ]
        
        # Avoid duplicate arguments
        try:
            new_args = list(new_options.arguments)
        except Exception:
            new_args = []
        
        for flag in safe_flags:
            if flag not in new_args:
                new_options.add_argument(flag)

        kwargs['options'] = new_options
        if args and isinstance(args[0], ChromeOptions):
            args = args[1:]

        return _OriginalChromeWebDriver(*args, **kwargs)

    webdriver.Chrome = ChromeWithInMemoryProfile

    from selenium.webdriver.remote.webdriver import WebDriver as _RemoteWebDriver

    def _safe_maximize(self):
        try:
            return self.set_window_size(1920, 1080)
        except Exception:
            return None

    def _safe_minimize(self):
        return None

    _RemoteWebDriver.maximize_window = _safe_maximize
    _RemoteWebDriver.minimize_window = _safe_minimize
    
    # Patch 3: TEFAS element wait handling
    def _patch_tefasfon_waits():
        try:
            import tefasfon.data_fetcher as tefas_fetcher
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            
            original_fetch = tefas_fetcher.fetch_tefas_data
            
            def patched_fetch(*args, **kwargs):
                from tefasfon import setup_webdriver
                original_setup = setup_webdriver
                
                def setup_with_wait(lang):
                    driver = original_setup(lang)
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.ID, "ui-id-1"))
                        )
                    except Exception:
                        pass
                    return driver
                
                import tefasfon.data_fetcher
                tefasfon.data_fetcher.setup_webdriver = setup_with_wait
                try:
                    return original_fetch(*args, **kwargs)
                finally:
                    tefasfon.data_fetcher.setup_webdriver = original_setup
            
            tefas_fetcher.fetch_tefas_data = patched_fetch
        except Exception:
            pass
    
    _patch_tefasfon_waits()
    
except ImportError:
    pass

from tefasfon import fetch_tefas_data
import openpyxl
import threading
import schedule
import shutil
import queue
from openpyxl import Workbook
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List, Optional, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
import string
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import logging
import warnings
import sys

# Cookie manager for Remember Me functionality
try:
    import extra_streamlit_components as stx
    COOKIES_AVAILABLE = True
except ImportError:
    stx = None
    COOKIES_AVAILABLE = False

# Load environment variables
load_dotenv()


# Configure logging to suppress Azure SDK errors
class AzureErrorFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        # Filter out specific Azure SDK error messages
        azure_errors = [
            'ContentDecodePolicy.deserialize_from_http_generics',
            'Unexpected return type',
            'http_generics',
            'deserialize_from_http_generics'
        ]
        return not any(error in message for error in azure_errors)

# Apply filter to all Azure-related loggers
azure_loggers = [
    'azure.storage.blob',
    'azure.storage',
    'azure.core',
    'azure.identity',
    'azure',
    'urllib3',
    'requests'
]

for logger_name in azure_loggers:
    logger = logging.getLogger(logger_name)
    logger.addFilter(AzureErrorFilter())
    logger.setLevel(logging.ERROR)

# Suppress all Azure SDK warnings
warnings.filterwarnings("ignore", category=UserWarning, module="azure")
warnings.filterwarnings("ignore", message=".*ContentDecodePolicy.*")
warnings.filterwarnings("ignore", message=".*deserialize_from_http_generics.*")

# Redirect stderr temporarily to filter Azure messages
class FilteredStderr:
    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        
    def write(self, message):
        # Filter out Azure SDK error messages from stderr
        if not any(error in str(message) for error in [
            'ContentDecodePolicy.deserialize_from_http_generics',
            'Unexpected return type',
            'http_generics',
            'deserialize_from_http_generics'
        ]):
            self.original_stderr.write(message)
            
    def flush(self):
        self.original_stderr.flush()

# Also filter stdout for completeness
class FilteredStdout:
    def __init__(self, original_stdout):
        self.original_stdout = original_stdout
        
    def write(self, message):
        # Filter out Azure SDK error messages from stdout
        if not any(error in str(message) for error in [
            'ContentDecodePolicy.deserialize_from_http_generics',
            'Unexpected return type',
            'http_generics',
            'deserialize_from_http_generics'
        ]):
            self.original_stdout.write(message)
            
    def flush(self):
        self.original_stdout.flush()

# Replace stderr and stdout with filtered versions
sys.stderr = FilteredStderr(sys.stderr)
sys.stdout = FilteredStdout(sys.stdout)

# Also override the built-in print function to filter Azure messages
_original_print = print

def filtered_print(*args, **kwargs):
    """Filter out Azure SDK error messages from print statements"""
    message = ' '.join(str(arg) for arg in args)
    if not any(error in message for error in [
        'ContentDecodePolicy.deserialize_from_http_generics',
        'Unexpected return type',
        'http_generics',
        'deserialize_from_http_generics'
    ]):
        _original_print(*args, **kwargs)

# Replace the built-in print function
import builtins
builtins.print = filtered_print

# Geçici fallback fonksiyonu (eğer tefasfon import edilemezse)
def fallback_fetch_tefas_data(**kwargs):
    """Geçici veri döndürür (test amaçlı) - Gerçek API formatına uygun"""
    
    debug_logger.warning('TEFAS_API', 'Using fallback TEFAS data (test mode)', {
        'kwargs': kwargs,
        'reason': 'fetch_tefas_data import failed or not available'
    })
    
    # Test verisi oluştur - Gerçek API formatını taklit et
    test_data = [
        {
            'Tarih': '5082025',
            'Fon Kodu': 'HPD', 
            'Fon Adı': 'Halk Portföy Değişken Fon',
            'Fiyat': 28.269417,
            'Tedavüldeki Pay Sayısı': 1436507.0,
            'Kişi Sayısı': 783,
            'Fon Toplam Değer': 40609215.84
        },
        {
            'Tarih': '5082025',
            'Fon Kodu': 'GPD', 
            'Fon Adı': 'Gedik Portföy Değişken Fon',
            'Fiyat': 25.345678,
            'Tedavüldeki Pay Sayısı': 2500000.0,
            'Kişi Sayısı': 1200,
            'Fon Toplam Değer': 63364195.0
        },
        {
            'Tarih': '5082025',
            'Fon Kodu': 'ZPD', 
            'Fon Adı': 'Ziraat Portföy Değişken Fon',
            'Fiyat': 30.123456,
            'Tedavüldeki Pay Sayısı': 1800000.0,
            'Kişi Sayısı': 950,
            'Fon Toplam Değer': 54222220.8
        }
    ]
    
    return pd.DataFrame(test_data)

# ============================================
# LOGGER UTILITY
# ============================================
class DebugLogger:
    """Centralized logging for application monitoring"""
    
    def __init__(self):
        self.logs = []
        self.max_logs = 1000
        self.enabled = False  # Disable verbose logging in production
        
    def log(self, level, module, message, data=None):
        """Log a debug message with context"""
        if not self.enabled:
            return
            
        import traceback
        from datetime import datetime
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'module': module,
            'message': message,
            'data': data,
            'platform': sys.platform,
            'python_version': sys.version.split()[0]
        }
        
        # Add to memory
        self.logs.append(log_entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)
        
        # Simplified console output
        if level == 'ERROR':
            import traceback
            print(f"[{level}] [{module}] {message}")
            print(f"Traceback: {traceback.format_exc()}")
    
    def info(self, module, message, data=None):
        self.log('INFO', module, message, data)
    
    def warning(self, module, message, data=None):
        self.log('WARNING', module, message, data)
    
    def error(self, module, message, data=None):
        self.log('ERROR', module, message, data)
    
    def debug(self, module, message, data=None):
        self.log('DEBUG', module, message, data)
    
    def get_logs(self, level=None, module=None, limit=100):
        """Retrieve logs with optional filtering"""
        filtered = self.logs
        
        if level:
            filtered = [l for l in filtered if l['level'] == level]
        if module:
            filtered = [l for l in filtered if l['module'] == module]
        
        return filtered[-limit:]
    
    def save_to_blob(self, blob_storage_instance):
        """Save logs to Azure Blob Storage"""
        try:
            from datetime import datetime
            log_filename = f"debug_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            log_data = json.dumps(self.logs, indent=2, default=str).encode('utf-8')
            blob_storage_instance.upload_file(log_filename, log_data)
            print(f"✅ Debug logs saved to blob: {log_filename}")
        except Exception as e:
            print(f"❌ Failed to save logs to blob: {str(e)}")

# Initialize global debug logger
debug_logger = DebugLogger()

# Sayfa konfigürasyonu
st.set_page_config(
    page_title="Benim Portfoyüm",
    page_icon="📊",
    layout="wide"
)

# 🔐 Cookie Manager for Remember Me (extra-streamlit-components kullanıyor)
def get_cookie_manager():
    """Cookie manager - Azure'da da çalışır"""
    if COOKIES_AVAILABLE and stx:
        return stx.CookieManager()
    return None

cookie_manager = get_cookie_manager()

# ✅ Ortak cookie ayarları (Azure üretimde Secure + domain/samesite)
COOKIE_DOMAIN = os.environ.get("COOKIE_DOMAIN") or None
_cookie_samesite_raw = os.environ.get("COOKIE_SAMESITE", "lax").strip().lower()
if _cookie_samesite_raw == "none":
    COOKIE_SAMESITE = None
elif _cookie_samesite_raw in ("lax", "strict"):
    COOKIE_SAMESITE = _cookie_samesite_raw
else:
    COOKIE_SAMESITE = "lax"

# Azure App Service ortamında WEBSITE_HOSTNAME mevcut → Secure flag gereksinimi
COOKIE_SECURE = bool(os.environ.get("WEBSITE_HOSTNAME"))

# Debug: Cookie ayarlarını logla
print("[COOKIE_CONFIG_DEBUG] ===========================================")
print(f"[COOKIE_CONFIG_DEBUG] Cookie Configuration:")
print(f"[COOKIE_CONFIG_DEBUG]   COOKIE_DOMAIN: {COOKIE_DOMAIN}")
print(f"[COOKIE_CONFIG_DEBUG]   COOKIE_SAMESITE: {COOKIE_SAMESITE}")
print(f"[COOKIE_CONFIG_DEBUG]   COOKIE_SECURE: {COOKIE_SECURE}")
print(f"[COOKIE_CONFIG_DEBUG]   COOKIES_AVAILABLE: {COOKIES_AVAILABLE}")
print(f"[COOKIE_CONFIG_DEBUG]   WEBSITE_HOSTNAME: {os.environ.get('WEBSITE_HOSTNAME', 'NOT_SET')}")
print(f"[COOKIE_CONFIG_DEBUG] ===========================================")

def set_remember_cookie(name, value, expires_at, key):
    """Tek noktadan cookie yaz; domain/secure/samesite tutarlı olsun."""
    try:
        debug_info = {
            "cookie_name": name,
            "value_length": len(str(value)) if value else 0,
            "expires_at": str(expires_at),
            "key": key,
            "domain": COOKIE_DOMAIN,
            "secure": COOKIE_SECURE,
            "samesite": COOKIE_SAMESITE,
            "cookies_available": COOKIES_AVAILABLE,
            "cookie_manager_exists": cookie_manager is not None,
            "website_hostname": os.environ.get("WEBSITE_HOSTNAME", "NOT_SET"),
        }
        print(f"[COOKIE_SET_DEBUG] Attempting to set cookie: {json.dumps(debug_info, indent=2)}")
        
        if COOKIES_AVAILABLE and cookie_manager is not None:
            cookie_manager.set(
                name,
                value,
                expires_at=expires_at,
                key=key,
                path="/",
                domain=COOKIE_DOMAIN,
                secure=COOKIE_SECURE,
                same_site=COOKIE_SAMESITE,
            )
            print(f"[COOKIE_SET_DEBUG] ✅ Cookie set successfully: {name}")
        else:
            print(f"[COOKIE_SET_DEBUG] ❌ Cannot set cookie - COOKIES_AVAILABLE={COOKIES_AVAILABLE}, cookie_manager={cookie_manager is not None}")
    except Exception as e:
        print(f"[COOKIE_SET_DEBUG] ❌ Exception setting cookie {name}: {str(e)}")
        import traceback
        traceback.print_exc()


def inject_dark_theme():
    """Apply the global dark-finance theme across the app UI."""
    st.markdown(
        """
        <style>
        :root {
            --bg-gradient-start: #0b1327;
            --bg-gradient-mid: #050b16;
            --bg-gradient-end: #01030b;
            --card-bg: rgba(14, 22, 36, 0.92);
            --card-border: rgba(59, 130, 246, 0.18);
            --muted-border: rgba(100, 116, 139, 0.18);
            --accent-start: #2563eb;
            --accent-end: #1d4ed8;
            --accent-soft: rgba(37, 99, 235, 0.18);
            --text-primary: #e2e8f0;
            --text-secondary: #94a3b8;
            --sidebar-bg: rgba(8, 13, 23, 0.92);
            --sidebar-border: rgba(59, 130, 246, 0.2);
            --metric-bg: linear-gradient(135deg, rgba(59, 130, 246, 0.18) 0%, rgba(15, 23, 42, 0.85) 100%);
        }

        body {
            background: radial-gradient(circle at 20% 20%, var(--bg-gradient-start) 0%, var(--bg-gradient-mid) 45%, var(--bg-gradient-end) 100%) !important;
            color: var(--text-primary) !important;
            font-family: "Inter", "Segoe UI", sans-serif;
        }

        [data-testid="stAppViewContainer"] > .main {
            background: transparent;
            color: var(--text-primary);
        }

        .stApp {
            background: radial-gradient(circle at 20% 20%, var(--bg-gradient-start) 0%, var(--bg-gradient-mid) 45%, var(--bg-gradient-end) 100%);
            color: var(--text-primary);
        }

        [data-testid="stSidebar"] {
            background: var(--sidebar-bg) !important;
        }

        [data-testid="stSidebar"] > div:first-child {
            background: var(--sidebar-bg);
            border-right: 1px solid var(--sidebar-border);
            box-shadow: inset -1px 0 0 rgba(15, 23, 42, 0.65);
        }

        [data-testid="stSidebarNav"] li {
            margin: 6px 12px;
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
        }

        [data-testid="stSidebarNav"] li::before,
        [data-testid="stSidebarNav"] li::after {
            content: none !important;
        }

        [data-testid="stSidebarNav"] li * {
            background: transparent !important;
            box-shadow: none !important;
            border: none !important;
            filter: none !important;
        }

        [data-testid="stSidebarNav"] a {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary) !important;
            font-weight: 500;
            padding: 4px 0;
            border-radius: 0 !important;
            background: transparent !important;
            box-shadow: none !important;
            border: none !important;
            transition: color 0.2s ease;
        }

        [data-testid="stSidebarNav"] a * {
            background: transparent !important;
            box-shadow: none !important;
        }

        [data-testid="stSidebarNav"] a:hover,
        [data-testid="stSidebarNav"] a:focus,
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            color: var(--text-primary) !important;
            background: transparent !important;
        }

        [data-testid="stSidebarNav"] a:hover *,
        [data-testid="stSidebarNav"] a:focus *,
        [data-testid="stSidebarNav"] a[aria-current="page"] * {
            background: transparent !important;
        }

        [data-testid="stSidebar"] [role="listbox"],
        [data-testid="stSidebar"] div[class*="menu"],
        [data-testid="stSidebar"] div[class*="dropdown"],
        [data-testid="stSidebar"] div[class*="select"] {
            background: var(--sidebar-bg) !important;
            color: #fff !important;
            border: none !important;
            box-shadow: none !important;
        }

        [data-testid="stSidebar"] [role="listbox"] * {
            background: var(--sidebar-bg) !important;
            color: #fff !important;
            border: none !important;
            box-shadow: none !important;
        }

        [data-testid="stSidebar"] [role="option"],
        [data-testid="stSidebar"] [role="option"] *,
        [data-testid="stSidebar"] div[class*="option"] {
            background: transparent !important;
            color: var(--text-primary) !important;
            font-weight: 400 !important;
            line-height: 1.4 !important;
            letter-spacing: normal !important;
            text-shadow: none !important;
            filter: none !important;
            padding: 8px 12px !important;
            text-transform: none !important;
            border-radius: 0 !important;
            display: block !important;
            margin: 0 !important;
        }

        [data-testid="stSidebar"] [role="option"]:hover,
        [data-testid="stSidebar"] div[class*="option"]:hover {
            background: rgba(59, 130, 246, 0.12) !important;
            color: var(--text-primary) !important;
        }

        /* Ensure 'Stop' / 'Durdur' buttons show white text */
        button[aria-label*="Periyodik G\u00fcncellemeyi Durdur"],
        button[aria-label*="Durdur"],
        button[aria-label*="Stop"] {
            color: #ffffff !important;
        }

        button[aria-label*="Periyodik G\u00fcncellemeyi Durdur"] *,
        button[aria-label*="Durdur"] *,
        button[aria-label*="Stop"] * {
            color: #ffffff !important;
        }

        /* Make top toolbar 'Stop' button text white */
        [data-testid="stToolbar"] button,
        [data-testid="stToolbar"] button *,
        [data-testid="stToolbar"] [role="button"],
        [data-testid="stToolbar"] [role="button"] *,
        header[data-testid="stHeader"] button,
        header[data-testid="stHeader"] button * {
            color: #ffffff !important;
        }

        /* Hide Deploy button in toolbar */
        [data-testid="stToolbar"] {
            display: none !important;
        }
        
        button[data-testid="stToolbarActionButton"],
        button[kind="header"],
        [data-testid="stHeader"] button[kind="header"] {
            display: none !important;
        }

        /* Target the specific Stop button in toolbar */
        button[title*="Stop"],
        button[title*="stop"],
        button[aria-label*="Stop"],
        button[aria-label*="stop"] {
            color: #ffffff !important;
        }

        button[title*="Stop"] *,
        button[title*="stop"] *,
        button[aria-label*="Stop"] *,
        button[aria-label*="stop"] * {
            color: #ffffff !important;
        }

        [data-testid="stSidebarNav"] ul {
            padding-top: 1rem;
        }

        [data-testid="stHeader"] {
            background: transparent;
        }

        div.block-container {
            padding-top: 1.5rem;
        }

        h1, h2, h3, h4, h5, h6 {
            color: var(--text-primary) !important;
        }

        p, li, label, span, div {
            color: var(--text-primary);
        }

        .stMarkdown, .stText, .stTextInput, .stSelectbox, .stDateInput, .stNumberInput {
            color: var(--text-primary);
        }

        .stTextInput > div > div > input,
        .stNumberInput input,
        .stDateInput input,
        .stSelectbox div[data-baseweb="select"] > div:first-child {
            background: rgba(15, 23, 42, 0.85);
            border: 1px solid var(--muted-border);
            color: var(--text-primary);
            border-radius: 10px;
        }

        .stTextInput > div > div > input:focus,
        .stNumberInput input:focus,
        .stDateInput input:focus,
        .stSelectbox div[data-baseweb="select"]:focus-within {
            border-color: rgba(37, 99, 235, 0.45); 
            box-shadow: 0 0 0 1px rgba(37, 99, 235, 0.25);
        }

        .stSelectbox div[data-baseweb="select"] > div:nth-child(2) {
            background: rgba(15, 23, 42, 0.95);
            border: 1px solid var(--muted-border);
            color: var(--text-primary);
        }

        /* Darken dropdown/listbox option menus including portal-appended dropdowns */
        /* Target ARIA listbox/option patterns and common portal wrappers */
        [role="listbox"],
        [role="listbox"] *,
        .main [role="listbox"],
        .block-container [role="listbox"],
        .stSelectbox [role="listbox"],
        .stMultiSelect [role="listbox"],
        .stSelectbox div[role="presentation"] div[role="listbox"],
        body > div[role="presentation"] [role="listbox"] {
            background: #1a1a1a !important;
            color: #ffffff !important;
            border: 1px solid #333333 !important;
            box-shadow: none !important;
            border-radius: 4px !important;
            padding: 2px !important;
            min-width: auto !important;
            max-width: none !important;
            width: auto !important;
        }

        /* Individual option styling - BASİT DÜZ METIN */
        [role="option"],
        [role="option"] *,
        [role="option"] div,
        [role="option"] span,
        .stSelectbox [role="option"],
        .stMultiSelect [role="option"],
        .stMultiSelect [role="option"] > div,
        .stMultiSelect [role="option"] * {
            background: transparent !important;
            color: #ffffff !important;
            padding: 6px 12px !important;
            font-weight: 400 !important;
            font-size: 13px !important;
            line-height: 1.4 !important;
            letter-spacing: 0 !important;
            text-transform: none !important;
            border-radius: 0 !important;
            display: block !important;
            margin: 0 !important;
            box-shadow: none !important;
            opacity: 1 !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            min-height: auto !important;
            height: auto !important;
            border: none !important;
            transition: none !important;
            transform: none !important;
        }
        
        /* Özel multiselect option text styling */
        [role="listbox"] [role="option"],
        [role="listbox"] [role="option"] *,
        div[data-baseweb="popover"] [role="option"],
        div[data-baseweb="popover"] [role="option"] * {
            color: #ffffff !important;
            background-color: transparent !important;
            font-weight: 400 !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }

        /* Hover/selected states for options - BASİT */
        [role="option"]:hover,
        [role="option"][aria-selected="true"],
        .stSelectbox [role="option"]:hover,
        .stSelectbox [role="option"][aria-selected="true"],
        .stMultiSelect [role="option"]:hover,
        .stMultiSelect [role="option"][aria-selected="true"] {
            background: #2563eb !important;
            color: #ffffff !important;
            text-decoration: none !important;
            font-weight: 400 !important;
            box-shadow: none !important;
            border-radius: 0 !important;
            transform: none !important;
        }
        .stSelectbox [role="listbox"]::-webkit-scrollbar,
        body > div[role="presentation"] [role="listbox"]::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        [role="listbox"]::-webkit-scrollbar-thumb,
        .stSelectbox [role="listbox"]::-webkit-scrollbar-thumb {
            background: #333333 !important;
            border-radius: 4px;
        }
        
        /* Base-web (Streamlit) popover ve dropdown stilleri - BASİT */
        div[data-baseweb="popover"],
        div[data-baseweb="popover"] ul,
        div[data-baseweb="menu"],
        div[data-baseweb="select"] ul {
            background: #1a1a1a !important;
            color: #ffffff !important;
            border: 1px solid #333333 !important;
            min-width: auto !important;
            max-width: none !important;
            width: auto !important;
            box-shadow: none !important;
            padding: 0 !important;
        }
        
        /* Base-web list item stilleri - BASİT DÜZ METIN */
        div[data-baseweb="popover"] li,
        div[data-baseweb="menu"] li,
        ul[role="listbox"] > li {
            color: #ffffff !important;
            background: transparent !important;
            font-size: 13px !important;
            font-weight: 400 !important;
            padding: 6px 12px !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            min-height: auto !important;
            height: auto !important;
            line-height: 1.4 !important;
            margin: 0 !important;
            border: none !important;
            border-radius: 0 !important;
            box-shadow: none !important;
            transition: none !important;
            transform: none !important;
        }
        
        ul[role="listbox"] > li:hover {
            background: #2563eb !important;
            color: #ffffff !important;
            box-shadow: none !important;
            transform: none !important;
        }

        /* Strong catch-all for portal-appended dropdown containers (white boxes appended to body)
           and common 3rd-party select libraries (react-select, rc-select). */
        body > div[role="presentation"],
        body > div[class*="overlay"],
        body > div[class*="portal"],
        body > div[class*="Portal"],
        body > div[class*="react-select"],
        body > div[class*="rc-select"],
        .react-select__menu,
        .react-select__menu-list,
        .rc-select-dropdown,
        .rc-virtual-list-holder,
        .rc-virtual-list-holder-inner,
        .rc-virtual-list {
            background: #1a1a1a !important;
            color: #fff !important;
            border: 1px solid #333333 !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin: 0 !important;
        }

        /* react-select option items - BASİT */
        .react-select__option,
        .react-select__option:hover,
        .rc-select-dropdown .rc-virtual-list-holder-inner li,
        .rc-select-dropdown .rc-virtual-list-holder-inner li:hover {
            background: transparent !important;
            color: #fff !important;
            padding: 6px 12px !important;
            font-size: 13px !important;
            font-weight: 400 !important;
            margin: 0 !important;
            border: none !important;
            border-radius: 0 !important;
            box-shadow: none !important;
            transition: none !important;
            transform: none !important;
        }
        
        .react-select__option:hover,
        .rc-select-dropdown .rc-virtual-list-holder-inner li:hover {
            background: #2563eb !important;
        }

        /* Replacement for any remaining inline-white containers directly under body */
        body > div[style*="background: white"],
        body > div[style*="background:#fff"],
        body > div[style*="background-color: #fff"],
        body > div[style*="background-color: white"] {
            background: #1a1a1a !important;
            color: #fff !important;
            border: 1px solid #333333 !important;
            box-shadow: none !important;
        }

        .stCheckbox, .stRadio, .stDateInput label {
            color: var(--text-primary);
        }

        div[data-testid="metric-container"] {
            background: var(--metric-bg);
            border: 1px solid var(--card-border);
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.45);
            padding: 16px;
            border-radius: 18px;
        }

        div[data-testid="metric-container"] label,
        div[data-testid="metric-container"] [data-testid="stMetricValue"],
        div[data-testid="metric-container"] [data-testid="stMetricDeltaValue"] {
            color: var(--text-primary);
        }

        div[data-testid="stDataFrame"] {
            border-radius: 16px;
            border: 1px solid rgba(148, 163, 184, 0.15);
            background: rgba(13, 20, 34, 0.92);
            box-shadow: 0 12px 32px rgba(8, 11, 19, 0.45);
        }

        div[data-testid="stDataFrame"] thead tr th {
            background: rgba(22, 30, 46, 0.92) !important;
            color: var(--text-primary) !important;
            border-bottom: 1px solid rgba(59, 130, 246, 0.25) !important;
        }

        div[data-testid="stDataFrame"] tbody tr td {
            color: var(--text-primary) !important;
            background: rgba(11, 18, 30, 0.72) !important;
            border-color: rgba(59, 130, 246, 0.15) !important;
        }

        div[data-testid="stDataFrame"] tbody tr:nth-child(even) td {
            background: rgba(7, 12, 22, 0.82) !important;
        }

        div[data-testid="stDataFrame"] table {
            background: transparent !important;
        }

        /* Additional table/grid coverage: make any HTML <table> in the main area dark */
        .main table, .block-container table, div[data-testid="stTable"] table, .stTable table {
            background: rgba(7,12,22,0.92) !important;
            color: var(--text-primary) !important;
            border-collapse: separate !important;
            border-spacing: 0 !important;
        }

        .main table thead th, .block-container table thead th, div[data-testid="stTable"] thead th, .stTable thead th {
            background: rgba(22, 30, 46, 0.96) !important;
            color: var(--text-primary) !important;
            border-bottom: 1px solid rgba(59,130,246,0.12) !important;
            padding: 10px 12px !important;
        }

        .main table tbody td, .block-container table tbody td, div[data-testid="stTable"] tbody td, .stTable tbody td {
            background: rgba(11, 18, 30, 0.78) !important;
            color: var(--text-primary) !important;
            border-top: 1px solid rgba(59,130,246,0.04) !important;
            padding: 10px 12px !important;
        }

        .main table tbody tr:nth-child(even) td,
        .block-container table tbody tr:nth-child(even) td {
            background: rgba(7, 12, 22, 0.86) !important;
        }

        /* AG Grid (used by some Streamlit components) */
        .ag-root, .ag-root-wrapper, .ag-theme-alpine, .ag-theme-balham, .ag-theme-material {
            background-color: rgba(7,12,22,0.92) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.06) !important;
            box-shadow: 0 12px 28px rgba(8,11,19,0.45) !important;
        }

        .ag-header, .ag-header-row, .ag-header-cell, .ag-header-cell-label {
            background-color: rgba(22,30,46,0.96) !important;
            color: var(--text-primary) !important;
            border-bottom: 1px solid rgba(59,130,246,0.12) !important;
        }

        .ag-row, .ag-cell {
            background-color: rgba(11,18,30,0.78) !important;
            color: var(--text-primary) !important;
            border-bottom: 1px solid rgba(59,130,246,0.04) !important;
        }

        .ag-row-alt, .ag-row:nth-child(even) {
            background-color: rgba(7,12,22,0.86) !important;
        }

        /* Ensure any floating table containers (modals, dialogs) also darken */
        .stDialog, .stModal, .modal, .dialog, .css-1kyxreq { /* generic fallbacks */
            background: rgba(7,12,22,0.95) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.06) !important;
            box-shadow: 0 18px 36px rgba(8,11,19,0.6) !important;
        }

        /* Catch any inline or default white backgrounds left by Streamlit widgets
           Force them to use the dark dashboard palette so cards and headers match charts */
        div[style*="background: white"],
        div[style*="background:#fff"],
        div[style*="background:#ffffff"],
        div[style*="background-color: white"],
        div[style*="background-color:#fff"],
        section[style*="background: white"],
        section[style*="background-color: white"] {
            background: rgba(9, 13, 24, 0.0) !important;
            color: var(--text-primary) !important;
            border-color: rgba(59, 130, 246, 0.12) !important;
            box-shadow: none !important;
        }

        /* Fallback: any element with a white background gets a dark replacement */
        *[style*="background: rgb(255, 255, 255)"],
        *[style*="background-color: rgb(255, 255, 255)"] {
            background: rgba(7, 12, 22, 0.92) !important;
            color: var(--text-primary) !important;
        }

        .stTabs [role="tablist"] {
            border-bottom: 1px solid rgba(59, 130, 246, 0.2);
            gap: 0.5rem;
        }

        .stTabs [role="tab"] {
            background: rgba(17, 24, 39, 0.78);
            border: 1px solid rgba(59, 130, 246, 0.2);
            color: var(--text-primary);
            border-radius: 12px;
            padding: 0.6rem 1.2rem;
            font-weight: 600;
            transition: all 0.3s ease;
        }

        .stTabs [role="tab"]:hover {
            border-color: rgba(59, 130, 246, 0.45);
            box-shadow: 0 6px 18px rgba(37, 99, 235, 0.25);
            color: var(--text-primary);
        }

        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, var(--accent-start) 0%, var(--accent-end) 100%);
            color: white !important;
            border: none;
        }

        div[data-testid="stExpander"] {
            background: linear-gradient(140deg, rgba(12, 20, 34, 0.92) 0%, rgba(7, 12, 22, 0.88) 100%);
            border-radius: 16px;
            border: 1px solid rgba(59, 130, 246, 0.2);
            box-shadow: 0 18px 36px rgba(8, 13, 24, 0.55);
            overflow: hidden;
        }

        .streamlit-expanderHeader,
        div[data-testid="stExpander"] div[role="button"] {
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.92) 100%) !important;
            color: var(--text-primary) !important;
            border-radius: 16px 16px 0 0;
            border: none;
            padding: 16px 18px;
            font-weight: 600;
        }

        div[data-testid="stExpander"] div[role="button"] svg {
            color: rgba(148, 163, 184, 0.8) !important;
        }

        /* Style Streamlit's spinner / running status and short info boxes to match dark theme */
        .stSpinner, .st-spinner, .stProgress, .stAlert, .stInfo, .stSuccess, .stWarning {
            background: rgba(11, 18, 30, 0.96) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59, 130, 246, 0.12) !important;
            box-shadow: 0 6px 20px rgba(8, 11, 19, 0.6) !important;
            border-radius: 8px !important;
        }

        /* Make warning text white */
        .stWarning, .stWarning *, .stWarning p, .stWarning span, .stWarning div, .stWarning h1, .stWarning h2, .stWarning h3, .stWarning h4, .stWarning h5, .stWarning h6 {
            color: #ffffff !important;
        }

        /* Streamlit's small "running" badge inside buttons/spinners */
        div[role="status"] > div, div[role="status"] {
            background: rgba(9, 13, 24, 0.95) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59, 130, 246, 0.12) !important;
            box-shadow: none !important;
            border-radius: 6px !important;
        }

        /* Target the small code-like spinner text (e.g., `Running get_portfolio_summary(...)`) */
        code, pre, .stCodeBlock, .stMarkdown code {
            background: rgba(11, 18, 30, 0.9) !important;
            color: var(--text-primary) !important;
            border-radius: 6px !important;
            padding: 2px 6px !important;
            border: 1px solid rgba(59, 130, 246, 0.08) !important;
        }

        /* Extra catch-all rules for remaining white headers/cards (including rgba white) */
        div[style*="background: rgba(255, 255, 255"],
        div[style*="background: rgba(255,255,255"],
        div[style*="background: #fff"],
        div[style*="background:#fff"],
        section[style*="background: rgba(255, 255, 255"],
        section[style*="background: #fff"] {
            background: rgba(7, 12, 22, 0.92) !important;
            color: var(--text-primary) !important;
            border-color: rgba(59,130,246,0.08) !important;
            box-shadow: 0 8px 24px rgba(8,11,19,0.45) !important;
        }

        /* Streamlit expander/button header catch (covers many Streamlit class names) */
        .streamlit-expanderHeader, .stExpanderHeader, div[role="button"] {
            background: linear-gradient(135deg, rgba(12,20,34,0.96), rgba(7,12,22,0.9)) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59, 130, 246, 0.08) !important;
            box-shadow: 0 6px 18px rgba(8, 11, 19, 0.5) !important;
            border-radius: 10px !important;
        }

        /* Make sure small inline status boxes (like 'Running ...') look dark */
        .stPlainText, .stText, .stCodeBlock, div[role="status"] span, div[role="status"] {
            background: rgba(9,13,24,0.95) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.06) !important;
            border-radius: 6px !important;
            padding: 2px 6px !important;
        }

        div[data-testid="stExpander"] div[role="button"]:hover {
            background: linear-gradient(135deg, rgba(37, 99, 235, 0.35) 0%, rgba(15, 23, 42, 0.92) 100%) !important;
            box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.35);
        }

        .streamlit-expanderContent,
        div[data-testid="stExpander"] div[data-testid="stExpanderContent"] {
            background: rgba(7, 12, 22, 0.92);
            border-radius: 0 0 16px 16px;
            border-top: 1px solid rgba(59, 130, 246, 0.2);
            color: var(--text-primary);
        }

        .stAlert {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid rgba(59, 130, 246, 0.22);
            border-radius: 14px;
        }

        .stAlert p {
            color: var(--text-primary);
        }

        .stDownloadButton > button {
            background: linear-gradient(135deg, #22d3ee 0%, #0ea5e9 100%);
            color: #0b1120;
            border-radius: 12px;
            font-weight: 600;
            border: none;
        }

        .stDownloadButton > button:hover {
            box-shadow: 0 10px 24px rgba(14, 165, 233, 0.35);
            transform: translateY(-1px) scale(1.01);
        }
        /* Aggressive catch-all for any remaining white rounded cards, header bars or inline containers
           This covers many Streamlit wrapper patterns without relying on auto-generated class names. */
        /* Elements with an inline white background or white-containing gradients */
        *[style*="background: white"],
        *[style*="background:#fff"],
        *[style*="background:#ffffff"],
        *[style*="background-color: white"],
        *[style*="background-color:#fff"],
        *[style*="background-color:#ffffff"],
        *[style*="linear-gradient"][style*="255, 255, 255"],
        *[style*="linear-gradient"][style*="#fff"] {
            background: linear-gradient(140deg, rgba(12,20,34,0.96) 0%, rgba(7,12,22,0.92) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.08) !important;
            box-shadow: 0 12px 28px rgba(8,11,19,0.5) !important;
            border-radius: 12px !important;
        }

        /* Elements that have a visible box-shadow + rounded corners often represent the white cards in Streamlit
           Force those to dark theme as well */
        *[style*="box-shadow"][style*="border-radius"],
        *[style*="box-shadow"][style*="background"] {
            background: rgba(7,12,22,0.92) !important;
            color: var(--text-primary) !important;
            border-color: rgba(59,130,246,0.06) !important;
            box-shadow: 0 12px 28px rgba(8,11,19,0.5) !important;
        }

        /* Explicit catch for top-of-expander header pill that can be rendered as a sibling container */
        div[data-testid="stExpander"] > div:first-child,
        section[data-testid="stExpander"] > div:first-child,
        div[data-testid="stExpander"] > div[role="button"] {
            background: linear-gradient(135deg, rgba(30,41,59,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.1) !important;
            box-shadow: 0 10px 22px rgba(8,11,19,0.45) !important;
            border-radius: 12px !important;
        }

        /* Ensure any role=button header elements inside the main content or expanders are dark from first paint.
           This catches Streamlit variations that render expander headers as buttons, spans or divs with role="button".
           We scope to .main and .block-container to avoid changing unrelated UI like OS-level buttons. */
        .main [role="button"],
        div.block-container [role="button"],
        div[data-testid="stExpander"] [role="button"],
        section[data-testid="stExpander"] [role="button"] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.08) !important;
            box-shadow: 0 8px 20px rgba(8,11,19,0.45) !important;
            border-radius: 12px !important;
            padding: 10px 14px !important;
            font-weight: 600 !important;
        }

        /* Also target ARIA expanded/collapsed containers to make sure the header is styled regardless of state */
        [aria-expanded="false"] > [role="button"],
        [aria-expanded="true"] > [role="button"] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
        }

        /* If Streamlit renders the header as a <button> element, ensure it's dark too */
        .main button[role="button"],
        div.block-container button[role="button"] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.08) !important;
            box-shadow: 0 6px 16px rgba(8,11,19,0.35) !important;
        }

        /* Keep expander headers dark even after expanding: target expanded states and any inline-style changes */
        /* 1) ARIA-expanded on the button or parent */
        div[data-testid="stExpander"] [aria-expanded="true"],
        div[data-testid="stExpander"] [aria-expanded="true"] [role="button"],
        div[data-testid="stExpander"][aria-expanded="true"] > div[role="button"],
        section[data-testid="stExpander"][aria-expanded="true"] > div[role="button"] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.08) !important;
            box-shadow: 0 10px 22px rgba(8,11,19,0.45) !important;
        }

        /* 2) If Streamlit injects inline style (common), force dark background on any role=button children or first-child divs */
        div[data-testid="stExpander"] > div:first-child[style],
        div[data-testid="stExpander"] > div[role="button"][style],
        div[data-testid="stExpander"] > button[style],
        section[data-testid="stExpander"] > div:first-child[style] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border-color: rgba(59,130,246,0.08) !important;
            box-shadow: 0 10px 22px rgba(8,11,19,0.45) !important;
        }

        /* 3) If the header becomes a sibling element when expanded, ensure sibling selectors keep it dark */
        div[data-testid="stExpander"][data-expanded="true"] > div[role="button"],
        div[data-testid="stExpander"][data-expanded="true"] > .streamlit-expanderHeader,
        div[data-testid="stExpander"] .streamlit-expanderHeader[aria-expanded="true"] {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
        }

        /* 4) Ultimate fallback: any element that looks like an expander header (wide, pill-shaped) inside main — force it dark */
        .main .streamlit-expanderHeader,
        .main .stExpanderHeader,
        .block-container .streamlit-expanderHeader,
        .block-container .stExpanderHeader {
            background: linear-gradient(135deg, rgba(24,34,50,0.98) 0%, rgba(12,20,34,0.94) 100%) !important;
            color: var(--text-primary) !important;
            border: 1px solid rgba(59,130,246,0.06) !important;
            box-shadow: 0 8px 18px rgba(8,11,19,0.4) !important;
        }

        /* Strong rule: make ALL expander headers dark and immutable across expand/collapse */
        /* Targets Streamlit variations: role=button, summary/details, aria-expanded, and common header classes */
        .main div[data-testid="stExpander"] > div[role="button"],
        .main div[data-testid="stExpander"] > div[role="button"] *,
        .main section[data-testid="stExpander"] > div[role="button"],
        .main .streamlit-expanderHeader,
        .main .stExpanderHeader,
        .main details > summary,
        .main summary,
        .main details[open] > summary,
        .main [role="button"][aria-expanded],
        .block-container div[data-testid="stExpander"] > div[role="button"],
        .block-container details > summary {
            background: linear-gradient(135deg, rgba(18,28,44,0.98) 0%, rgba(10,16,28,0.94) 100%) !important;
            color: var(--text-primary) !important;
            font-size: 16px !important;
            font-weight: 700 !important;
            padding: 12px 16px !important;
            border-radius: 12px !important;
            border: 1px solid rgba(59,130,246,0.06) !important;
            box-shadow: 0 8px 20px rgba(8,11,19,0.45) !important;
        }

        /* Ensure icons/text inside headers inherit the colors and don't get white backgrounds */
        .main div[data-testid="stExpander"] > div[role="button"] svg,
        .main div[data-testid="stExpander"] > div[role="button"] i,
        .main details > summary svg,
        .main details > summary i {
            color: rgba(148,163,184,0.9) !important;
        }

        /* Keep the same style when aria-expanded toggles or details open */
        .main div[data-testid="stExpander"] [aria-expanded="true"],
        .main div[data-testid="stExpander"] [aria-expanded="false"],
        .main details[open] > summary,
        .main details > summary:focus,
        .main details > summary:hover {
            background: linear-gradient(135deg, rgba(18,28,44,0.98) 0%, rgba(10,16,28,0.94) 100%) !important;
            color: var(--text-primary) !important;
            box-shadow: 0 8px 20px rgba(8,11,19,0.45) !important;
        }

        /* Prevent Streamlit from adding white inner spans or wrappers on expand */
        .main div[data-testid="stExpander"] > div[role="button"] span,
        .main div[data-testid="stExpander"] > div[role="button"] div {
            background: transparent !important;
            color: inherit !important;
        }

        /* Remove blue pill backgrounds and borders for buttons so only text remains */
        /* Sidebar buttons and page-level action buttons */
        .stSidebar [role="button"],
        .stSidebar button,
        .stButton > button,
        .stDownloadButton > button,
        .stDownloadButton button,
        .main .stButton > button,
        .block-container .stButton > button,
        .main button[role="button"],
        .block-container button[role="button"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin: 0 !important;
            color: var(--text-primary) !important;
            font-weight: 600 !important;
            border-radius: 0 !important;
        }

        /* Keep hover/focus from reintroducing blue backgrounds or outlines */
        .stSidebar [role="button"]:hover,
        .stSidebar button:hover,
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        .main .stButton > button:hover,
        .block-container .stButton > button:hover,
        .main button[role="button"]:hover,
        .block-container button[role="button"]:hover {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            color: var(--accent-start) !important; /* keep hover color if desired */
        }

        /* Remove focus outline */
        .stButton > button:focus,
        .stSidebar button:focus,
        .stDownloadButton > button:focus,
        button[role="button"]:focus {
            outline: none !important;
            box-shadow: none !important;
        }

        /* Portal / dropdown overlays appended to <body> - AGGRESSIVE BLACK BACKGROUND */
        body > div[style*="position: absolute"],
        body > div[style*="z-index"],
        body > div[class*="css-"],
        div[role="listbox"],
        [role="option"],
        .react-select__menu,
        .react-select__menu-list,
        div[data-baseweb="popover"],
        div[data-baseweb="menu"],
        ul[role="listbox"],
        div[class*="menu"],
        div[class*="dropdown"],
        div[class*="select"] {
            background: #000000 !important;
            background-color: #000000 !important;
            color: #ffffff !important;
            border: 1px solid #333333 !important;
            box-shadow: 0 12px 28px rgba(0,0,0,0.8) !important;
            border-radius: 8px !important;
        }

        /* Underline for document links */
        button[key*="btn_user_terms"],
        button[key*="btn_privacy"],
        button[key*="btn_cookie"],
        .document-link {
            text-decoration: underline !important;
            background: transparent !important;
            border: none !important;
            color: #ffffff !important;
            cursor: pointer !important;
            padding: 0 !important;
            margin: 0 !important;
            font-weight: 500 !important;
        }

        button[key*="btn_user_terms"]:hover,
        button[key*="btn_privacy"]:hover,
        button[key*="btn_cookie"]:hover,
        .document-link:hover {
            color: #e2e8f0 !important;
            text-decoration: underline !important;
        }

        /* Document content styling - white text for all elements */
        div[data-testid="stExpander"] {
            background: linear-gradient(140deg, rgba(12, 20, 34, 0.92) 0%, rgba(7, 12, 22, 0.88) 100%) !important;
        }

        /* All text in expandable document sections must be white */
        div[data-testid="stExpander"] *,
        div[data-testid="stExpander"] h1,
        div[data-testid="stExpander"] h2,
        div[data-testid="stExpander"] h3,
        div[data-testid="stExpander"] h4,
        div[data-testid="stExpander"] h5,
        div[data-testid="stExpander"] h6,
        div[data-testid="stExpander"] p,
        div[data-testid="stExpander"] li,
        div[data-testid="stExpander"] ul,
        div[data-testid="stExpander"] ol,
        div[data-testid="stExpander"] span,
        div[data-testid="stExpander"] div,
        div[data-testid="stExpander"] a,
        div[data-testid="stExpander"] strong,
        div[data-testid="stExpander"] em,
        .streamlit-expanderContent,
        .streamlit-expanderContent *,
        .streamlit-expanderContent h1,
        .streamlit-expanderContent h2,
        .streamlit-expanderContent h3,
        .streamlit-expanderContent p,
        .streamlit-expanderContent li,
        .streamlit-expanderContent span,
        .streamlit-expanderContent div,
        .streamlit-expanderContent a {
            color: #ffffff !important;
            background: transparent !important;
        }

        /* Document headings - larger and bolder */
        div[data-testid="stExpander"] h1,
        div[data-testid="stExpander"] h2,
        .streamlit-expanderContent h1,
        .streamlit-expanderContent h2 {
            font-size: 24px !important;
            font-weight: 700 !important;
            color: #ffffff !important;
            margin-top: 16px !important;
            margin-bottom: 12px !important;
            line-height: 1.3 !important;
        }

        /* Document section numbers and titles */
        div[data-testid="stExpander"] h3,
        .streamlit-expanderContent h3 {
            font-size: 18px !important;
            font-weight: 600 !important;
            color: #ffffff !important;
            margin-top: 12px !important;
            margin-bottom: 8px !important;
        }

        /* Document paragraph text */
        div[data-testid="stExpander"] p,
        .streamlit-expanderContent p {
            font-size: 14px !important;
            font-weight: 400 !important;
            color: #ffffff !important;
            line-height: 1.5 !important;
            margin-bottom: 10px !important;
        }

        /* Document list items */
        div[data-testid="stExpander"] li,
        .streamlit-expanderContent li {
            font-size: 14px !important;
            font-weight: 400 !important;
            color: #ffffff !important;
            line-height: 1.5 !important;
            margin-bottom: 6px !important;
        }

        /* Force all dropdown options and items to black background, white text */
        div[role="listbox"] li,
        div[role="listbox"] div,
        div[role="listbox"] span,
        [role="option"],
        [role="option"] *,
        ul[role="listbox"] li,
        div[class*="option"],
        div[class*="menu"] div,
        div[class*="dropdown"] div,
        body > div div[style*="background"],
        body > div[style*="position"] div,
        body > div[style*="z-index"] div {
            background: #000000 !important;
            background-color: #000000 !important;
            color: #ffffff !important;
            text-shadow: none !important;
        }

        /* Override any white or light backgrounds in portals */
        body > div[style*="background: white"],
        body > div[style*="background: rgb(255"],
        body > div[style*="background-color: white"],
        body > div[style*="background-color: rgb(255"] {
            background: #000000 !important;
            background-color: #000000 !important;
        }

        /* Hover states for dropdown options */
        [role="option"]:hover,
        div[class*="option"]:hover,
        ul[role="listbox"] li:hover {
            background: #222222 !important;
            background-color: #222222 !important;
            color: #ffffff !important;
        }

        /* ===== DATE INPUT SIMPLE DARK THEME ===== */
        /* Basit ve sade tarih seçici stilleri - tüm efektler kaldırıldı */
        
        /* Date input container ve tüm elementleri */
        [data-testid="stDateInput"],
        [data-testid="stDateInput"] *,
        div[data-baseweb="datepicker"],
        div[data-baseweb="datepicker"] * {
            background: transparent !important;
            color: #e2e8f0 !important;
            border: none !important;
            box-shadow: none !important;
        }

        /* Date input label (Başlangıç Tarihi, Bitiş Tarihi yazıları) */
        [data-testid="stDateInput"] label,
        div[data-baseweb="datepicker"] label {
            color: #e2e8f0 !important;
            font-weight: normal !important;
        }

        /* Date input field */
        [data-testid="stDateInput"] input,
        div[data-baseweb="datepicker"] input {
            background: transparent !important;
            color: #e2e8f0 !important;
            border: 1px solid rgba(100, 116, 139, 0.3) !important;
            border-radius: 4px !important;
            padding: 6px 10px !important;
            box-shadow: none !important;
        }

        /* Hover - efektsiz */
        [data-testid="stDateInput"] input:hover,
        div[data-baseweb="datepicker"] input:hover {
            border-color: rgba(100, 116, 139, 0.5) !important;
            box-shadow: none !important;
        }

        /* Focus - efektsiz */
        [data-testid="stDateInput"] input:focus,
        div[data-baseweb="datepicker"] input:focus {
            border-color: rgba(100, 116, 139, 0.7) !important;
            box-shadow: none !important;
            outline: none !important;
        }

        /* Calendar popup - basit dark */
        div[data-baseweb="calendar"],
        div[data-baseweb="calendar"] * {
            background: #1e293b !important;
            color: #e2e8f0 !important;
            border: 1px solid rgba(100, 116, 139, 0.3) !important;
            box-shadow: none !important;
        }

        /* Calendar header */
        div[data-baseweb="calendar-header"] {
            background: #0f172a !important;
            border-bottom: 1px solid rgba(100, 116, 139, 0.3) !important;
        }

        /* Calendar günler */
        div[data-baseweb="calendar"] button {
            background: transparent !important;
            color: #e2e8f0 !important;
            border: none !important;
            box-shadow: none !important;
        }

        /* Seçili gün - sade mavi */
        div[data-baseweb="calendar"] button[aria-selected="true"] {
            background: #3b82f6 !important;
            color: #ffffff !important;
        }

        /* Hover - minimal */
        div[data-baseweb="calendar"] button:hover {
            background: rgba(59, 130, 246, 0.2) !important;
        }
        </style>

        <script>
        (function(){
            const darkBg = '#000000'; // Pure black
            const darkBorder = '1px solid #333333';
            const darkBox = '0 12px 28px rgba(0,0,0,0.8)';
            const textColor = '#ffffff'; // Pure white

            function isWhiteStyle(el){
                try{
                    const cs = window.getComputedStyle(el);
                    const bg = cs.backgroundColor || '';
                    if(!bg) return false;
                    // treat very light backgrounds as white (more aggressive threshold)
                    const m = bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
                    if(!m) return false;
                    const r = +m[1], g = +m[2], b = +m[3];
                    return r>200 && g>200 && b>200; // Lower threshold for more aggressive catching
                }catch(e){return false}
            }

            function patchElement(el){
                try{
                    if(!(el instanceof HTMLElement)) return;
                    el.style.setProperty('background', darkBg, 'important');
                    el.style.setProperty('background-color', darkBg, 'important');
                    el.style.setProperty('color', textColor, 'important');
                    el.style.setProperty('border', darkBorder, 'important');
                    el.style.setProperty('box-shadow', darkBox, 'important');
                    // Force all children to have black background and white text
                    el.querySelectorAll('*').forEach(c=>{ 
                        try{ 
                            c.style.setProperty('background','#000000','important'); 
                            c.style.setProperty('background-color','#000000','important'); 
                            c.style.setProperty('color', textColor,'important');
                            c.style.setProperty('text-shadow', 'none','important');
                        }catch(e){} 
                    });
                }catch(e){}
            }

            function scanNode(node){
                try{
                    if(!(node instanceof HTMLElement)) return;
                    
                    // Skip if element is inside sidebar
                    if(node.closest('[data-testid="stSidebar"]')) return;
                    
                    // Check if this node itself needs patching
                    if(isWhiteStyle(node)) { patchElement(node); return; }
                    
                    // Check for portal patterns (body direct children)
                    if(node.parentElement === document.body) {
                        const style = node.style.cssText || '';
                        if(style.includes('position: absolute') || style.includes('z-index') || node.getAttribute('role') === 'listbox') {
                            patchElement(node);
                            return;
                        }
                    }
                    
                    // check common dropdown containers appended to body
                    const candidates = node.querySelectorAll('div, ul, section');
                    candidates.forEach(c=>{ 
                        if(!c.closest('[data-testid="stSidebar"]') && isWhiteStyle(c)) patchElement(c); 
                    });

                    // Additional heuristics for react-select / portal dropdowns
                    // - elements with role=listbox or role=option
                    // - elements with class names that start with 'css-' (emotion) or contain 'menu'/'portal'
                    const extra = node.querySelectorAll('[role="listbox"], [role="option"], div[class*="menu"], div[class*="portal"], div[class*="react-select"], div[class^="css-"], div[class*="dropdown"], div[class*="option"]');
                    extra.forEach(c=>{ 
                        if(!c.closest('[data-testid="stSidebar"]') && (isWhiteStyle(c) || /menu|portal|react-select|css-|dropdown|option/.test(c.className))) {
                            patchElement(c); 
                        }
                    });
                }catch(e){}
            }

            // initial pass
            scanNode(document.body);

            // Observe added nodes (portals/dropdowns appended to body)
            const mo = new MutationObserver(muts=>{
                muts.forEach(m=>{
                    m.addedNodes.forEach(n=>{ if(n.nodeType===1) scanNode(n); });
                });
            });
            mo.observe(document.body, { childList:true, subtree:true });
        })();
        </script>

        """,
        unsafe_allow_html=True,
    )


# Türk altın enstrümanları listesi - tüm fonksiyonlarda kullanılır
TURKISH_GOLD_INSTRUMENTS = [
    "ALTIN_GRAM", "ALTIN_CEYREK", "ALTIN_YARIM", "ALTIN_TAM", "ALTIN_ONS_TRY", 
    "ALTIN_RESAT", "ALTIN_CUMHURIYET", "ALTIN_ATA", "ALTIN_HAMIT", 
    "ALTIN_IKIBUCUK", "ALTIN_BESLI", "ALTIN_14AYAR", "ALTIN_18AYAR", "ALTIN_22AYAR_BILEZIK"
]

# Türk altın çevrimleri (gram cinsinden)
TURKISH_GOLD_CONVERSIONS = {
    "ALTIN_GRAM": 1.0,
    "ALTIN_CEYREK": 1.75,
    "ALTIN_YARIM": 3.5,
    "ALTIN_TAM": 7.0,
    "ALTIN_RESAT": 7.216,
    "ALTIN_CUMHURIYET": 7.216,
    "ALTIN_ATA": 7.216,
    "ALTIN_HAMIT": 3.608,
    "ALTIN_IKIBUCUK": 4.26,
    "ALTIN_BESLI": 8.52,
    "ALTIN_14AYAR": 0.583,  # 14/24 saflık
    "ALTIN_18AYAR": 0.75,   # 18/24 saflık
    "ALTIN_22AYAR_BILEZIK": 0.916,  # 22/24 saflık
    "ALTIN_ONS_TRY": 31.1035  # 1 ons = 31.1035 gram
}

# ================ KULLANICI YÖNETİMİ VE PORTFÖYler ================

# Kullanıcı veritabanı dosyası
USERS_FILE = "users.json"
PORTFOLIOS_FILE = "portfolios.json"
JOB_SETTINGS_FILE = "job_settings.json"
SUBSCRIPTIONS_FILE = "subscriptions.json"

# ================ ABONELİK SİSTEMİ AYARLARI ================
ADMIN_EMAILS = ["erdalural@gmail.com"]  # Admin kullanıcıları

# Yeni üyelere otomatik deneme süresi (gün)
TRIAL_PERIOD_DAYS = 30

# Abonelik planları (ay, fiyat TL)
SUBSCRIPTION_PLANS = {
    "trial": {
        "name": "Deneme (Ücretsiz)",
        "months": 1,
        "price": 0,
        "monthly_price": 0
    },
    "3_months": {
        "name": "3 Aylık Abonelik",
        "months": 3,
        "price": 90,  # 3 x 30 TL
        "monthly_price": 30
    },
    "12_months": {
        "name": "12 Aylık Abonelik", 
        "months": 12,
        "price": 360,  # 12 x 30 TL
        "monthly_price": 30
    }
}

# Ödeme bilgileri
PAYMENT_INFO = {
    "iban": "TR10 0001 5001 5800 7299 1739 08",
    "bank_name": "Vakıfbank",
    "account_holder": "Erdi Ural",
    "description": "Abonelik ödemesi için lütfen açıklama kısmına e-posta adresinizi yazın.<br><br>⚠️ Bu platform sanal olarak portföyünüzün değerini gösterir. Platformda hiç bir gerçek(fiili) alış/satış işlemi yapılamamaktadır. Bu yüzden abonelik ücreti dışında bir para göndermeyiniz."
}

# Thread-safe queue for background job logs to avoid calling Streamlit from worker threads
LOG_QUEUE = queue.Queue()

def enqueue_job_log(key: str, message: str):
    """Put a log message into the queue for the main thread to flush into st.session_state"""
    try:
        LOG_QUEUE.put_nowait((key, message))
    except Exception:
        pass

def flush_job_logs():
    """Drain the LOG_QUEUE and append messages into st.session_state logs (main thread only)."""
    try:
        while not LOG_QUEUE.empty():
            key, message = LOG_QUEUE.get_nowait()
            if key not in st.session_state:
                st.session_state[key] = []
            st.session_state[key].append(message)
            # Keep only last 100 entries
            if len(st.session_state[key]) > 100:
                st.session_state[key] = st.session_state[key][-100:]
    except Exception:
        pass

def safe_parse_time(t: str, default=None):
    """Try multiple time formats to parse a saved time string.
    Returns a datetime.time object or the provided default.
    """
    if default is None:
        default = datetime_time(9, 0)
    if not t:
        return default
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            return datetime.strptime(t, fmt).time()
        except Exception:
            continue
    return default


def format_quantity_display(x, decimals=4):
    """Format a numeric quantity: show up to `decimals` decimals but
    strip trailing zeros and the decimal point when not needed.
    Preserves thousands separator for large numbers.
    Examples: 1.0000 -> '1', 1.2300 -> '1.23', 12345.0000 -> '12,345'
    """
    try:
        if x is None:
            return ""
        val = float(x)
        s = f"{val:,.{decimals}f}"
        # strip trailing zeros and possibly trailing decimal point
        s = s.rstrip('0').rstrip('.')
        return s
    except Exception:
        return str(x)

# ================ AZURE BLOB STORAGE YAPILANDIRMASI ================

class AzureBlobStorage:
    """Azure Blob Storage yönetimi için sınıf"""
    
    def __init__(self):
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = os.getenv('AZURE_STORAGE_CONTAINER_NAME', 'finansapp')
        self.blob_service_client = None
        self.container_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Azure Blob Storage istemcisini başlat - Sessiz bağlantı"""
        try:
            debug_logger.info('AZURE_BLOB', 'Initializing Azure Blob Storage client', {
                'has_connection_string': bool(self.connection_string),
                'container_name': self.container_name,
                'has_account_url': bool(os.getenv('AZURE_STORAGE_ACCOUNT_URL'))
            })
            
            if self.connection_string:
                self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
                debug_logger.info('AZURE_BLOB', 'Connected using connection string')
            else:
                # Managed Identity kullanarak
                account_url = os.getenv('AZURE_STORAGE_ACCOUNT_URL')
                if account_url:
                    credential = DefaultAzureCredential()
                    self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
                    debug_logger.info('AZURE_BLOB', 'Connected using Managed Identity', {'account_url': account_url})
            
            if self.blob_service_client:
                self.container_client = self.blob_service_client.get_container_client(self.container_name)
                debug_logger.info('AZURE_BLOB', 'Container client created')
                
                # Container yoksa oluştur
                try:
                    self.container_client.create_container()
                    debug_logger.info('AZURE_BLOB', f'Container created: {self.container_name}')
                except Exception:
                    # Container zaten mevcut - normal
                    debug_logger.debug('AZURE_BLOB', f'Container already exists: {self.container_name}')
                    pass
                
                # Bağlantıyı test et - sessizce
                try:
                    props = self.container_client.get_container_properties()
                    debug_logger.info('AZURE_BLOB', 'Connection test successful', {
                        'container_name': self.container_name,
                        'last_modified': str(props.last_modified) if hasattr(props, 'last_modified') else 'N/A'
                    })
                except Exception as test_error:
                    debug_logger.error('AZURE_BLOB', 'Connection test failed', {
                        'error': str(test_error),
                        'error_type': type(test_error).__name__
                    })
                    self.blob_service_client = None
            else:
                debug_logger.warning('AZURE_BLOB', 'Blob service client not initialized')
                    
        except Exception as e:
            debug_logger.error('AZURE_BLOB', 'Client initialization failed', {
                'error': str(e),
                'error_type': type(e).__name__
            })
            self.blob_service_client = None
    
    def upload_file(self, file_content: bytes = None, blob_name: str = None, file_name: str = None, data: bytes = None, silent: bool = False) -> bool:
        """Dosyayı blob storage'a yükle - Gelişmiş hata yönetimi ile"""
        try:
            if not self.blob_service_client:
                if not silent:
                    print("❌ Azure Blob Storage bağlantısı yok")
                return False
            
            # Parametreleri normalize et
            if blob_name is not None and file_content is not None:
                # Yeni format
                file_name = blob_name
                data = file_content
            elif file_name is not None and data is not None:
                # Eski format (değişiklik yok)
                pass
            else:
                if not silent:
                    print("❌ upload_file: geçersiz parametreler")
                return False
            
            # Veri kontrolü
            if not data or len(data) == 0:
                if not silent:
                    print(f"❌ Upload edilecek veri boş: {file_name}")
                return False
            
            # Blob client oluştur - Kısaltılmış URL ile
            try:
                blob_client = self.blob_service_client.get_blob_client(
                    container=self.container_name, 
                    blob=file_name
                )
            except Exception as client_error:
                if not silent:
                    print(f"❌ Blob client oluşturulamadı: {str(client_error)}")
                return False
            
            # Container'ın varlığını kontrol et
            try:
                container_client = self.blob_service_client.get_container_client(self.container_name)
                container_client.get_container_properties()
            except Exception as container_error:
                if not silent:
                    print(f"❌ Container '{self.container_name}' erişilemez: {str(container_error)}")
                return False
            
            # Dosyayı yükle - çoklu yöntem dene
            upload_success = False
            
            # Method 1: Normal upload with error filtering
            try:
                blob_client.upload_blob(data, overwrite=True)
                upload_success = True
                if not silent:
                    print(f"✅ Blob '{file_name}' başarıyla yüklendi ({len(data)} bytes)")
            except Exception as upload_error:
                error_msg = str(upload_error)
                # Tüm spam hatalarını filtrele
                if ("request url too long" in error_msg.lower() or 
                    "http error 414" in error_msg.lower()):
                    if not silent:
                        pass  # Bu mesajları artık gösterme
                elif ("<!doctype html" in error_msg.lower() or
                      "contentdecodepolicy" in error_msg.lower() or
                      "http_generics" in error_msg.lower() or
                      "unexpected return type" in error_msg.lower()):
                    # Bu hataları tamamen gizle
                    pass
                else:
                    if not silent:
                        print(f"⚠️ Method 1 upload hatası: {error_msg[:200]}...")  # İlk 200 karakter
            
            # Method 2: Smaller chunk upload (URL sorunları için)
            if not upload_success:
                try:
                    # Daha küçük chunk size ve single thread
                    blob_client.upload_blob(
                        data, 
                        overwrite=True, 
                        max_concurrency=1,
                        blob_type="BlockBlob"
                    )
                    upload_success = True
                    if not silent:
                        print(f"✅ Blob '{file_name}' Method 2 ile yüklendi")
                except Exception as chunked_error:
                    error_msg = str(chunked_error)
                    if not silent and not any(x in error_msg.lower() for x in ["request url too long", "<!doctype html", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                        print(f"⚠️ Method 2 hatası: {error_msg[:100]}...")
            
            # Method 3: Stream upload with proper bytes handling
            if not upload_success:
                try:
                    from io import BytesIO
                    
                    # Veriyi bytes'a çevir
                    if isinstance(data, str):
                        data_bytes = data.encode('utf-8')
                    else:
                        data_bytes = data
                    
                    data_stream = BytesIO(data_bytes)
                    blob_client.upload_blob(data_stream, overwrite=True, blob_type="BlockBlob")
                    upload_success = True
                    if not silent:
                        print(f"✅ Blob '{file_name}' stream upload ile yüklendi")
                except Exception as stream_error:
                    error_msg = str(stream_error)
                    if not silent and not any(x in error_msg.lower() for x in ["request url too long", "<!doctype html", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                        print(f"❌ Method 3 stream hatası: {error_msg[:100]}...")
            
            # Method 4: Simple upload with basic settings
            if not upload_success:
                try:
                    # En basit ayarlarla - SDK uyumluluğu için
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        timeout=300  # 5 dakika timeout
                    )
                    upload_success = True
                    if not silent:
                        print(f"✅ Blob '{file_name}' Method 4 ile yüklendi")
                except Exception as final_error:
                    error_msg = str(final_error)
                    if not silent and not any(x in error_msg.lower() for x in ["request url too long", "<!doctype html", "contentdecodepolicy", "http_generics", "unexpected return type", "http error 414"]):
                        print(f"❌ Method 4 final hatası: {error_msg[:100]}...")
            
            return upload_success
            
        except Exception as e:
            # Tüm spam hataları tamamen gizle
            error_msg = str(e).lower()
            if not silent and not any(x in error_msg for x in ["request url too long", "http error 414", "<!doctype html", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                print(f"❌ Azure upload kritik hatası: {str(e)[:100]}...")
            return False
    
    def download_file(self, file_name: str, silent: bool = False) -> Optional[bytes]:
        """Dosyayı blob storage'dan indir - Deserialization hatası tamamen bypass"""
        try:
            if not self.blob_service_client:
                return None
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=file_name
            )
            
            # Önce blob'un var olup olmadığını kontrol et
            try:
                blob_properties = blob_client.get_blob_properties()
            except Exception as e:
                if "BlobNotFound" in str(e):
                    if not silent:
                        print(f"📄 Blob '{file_name}' bulunamadı (ilk kez çalıştırılıyor olabilir)")
                    return None
                else:
                    if not silent:
                        print(f"Blob properties hatası: {str(e)}")
                    return None
            
            # Deserialization hatası bypass - çoklu yöntem dene
            blob_data = None
            
            # Method 1: download_blob().readall() with error suppression
            try:
                download_stream = blob_client.download_blob()
                blob_data = download_stream.readall()
                
                # Type check and conversion
                if isinstance(blob_data, str):
                    blob_data = blob_data.encode('utf-8')
                elif not isinstance(blob_data, bytes):
                    blob_data = str(blob_data).encode('utf-8')
                
                return blob_data
                
            except Exception as method1_error:
                # Tüm spam hatalarını tamamen gizle
                error_msg = str(method1_error).lower()
                if not silent and not any(x in error_msg for x in ["deserialize", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                    print(f"Method 1 failed for {file_name}: {str(method1_error)}")
            
            # Method 2: content_as_bytes()
            try:
                blob_data = blob_client.download_blob().content_as_bytes()
                return blob_data
            except Exception as method2_error:
                # Tüm spam hatalarını tamamen gizle
                error_msg = str(method2_error).lower()
                if not silent and not any(x in error_msg for x in ["deserialize", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                    print(f"Method 2 failed for {file_name}: {str(method2_error)}")
            
            # Method 3: Raw stream reading
            try:
                download_stream = blob_client.download_blob(max_concurrency=1)
                chunks = []
                for chunk in download_stream.chunks():
                    if isinstance(chunk, str):
                        chunks.append(chunk.encode('utf-8'))
                    else:
                        chunks.append(chunk)
                blob_data = b''.join(chunks)
                return blob_data
            except Exception as method3_error:
                # Tüm spam hatalarını tamamen gizle
                error_msg = str(method3_error).lower()
                if not silent and not any(x in error_msg for x in ["deserialize", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                    print(f"Method 3 failed for {file_name}: {str(method3_error)}")
            
            # Method 4: Stream with encoding override
            try:
                download_stream = blob_client.download_blob()
                raw_data = download_stream.content_as_text(encoding='utf-8')
                return raw_data.encode('utf-8')
            except Exception as method4_error:
                # Tüm spam hatalarını tamamen gizle
                error_msg = str(method4_error).lower()
                if not silent and not any(x in error_msg for x in ["deserialize", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                    print(f"Method 4 failed for {file_name}: {str(method4_error)}")
            
            return None
            
        except Exception as e:
            # Tüm spam hatalarını tamamen gizle
            error_msg = str(e).lower()
            if not silent and not any(x in error_msg for x in ["deserialize", "contentdecodepolicy", "http_generics", "unexpected return type"]):
                print(f"Azure download kritik hatası: {str(e)}")
            return None
    
    def file_exists(self, file_name: str) -> bool:
        """Dosyanın blob storage'da var olup olmadığını kontrol et"""
        try:
            if not self.blob_service_client:
                return False
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=file_name
            )
            
            exists = blob_client.exists()
            return exists
            
        except Exception as e:
            return False
    
    def delete_file(self, file_name: str) -> bool:
        """Dosyayı blob storage'dan sil"""
        try:
            if not self.blob_service_client:
                return False
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=file_name
            )
            
            blob_client.delete_blob(delete_snapshots="include")
            return True
            
        except Exception as e:
            return False

# Azure Blob Storage istemcisini başlat - Singleton pattern
@st.cache_resource
def get_azure_blob_storage():
    """Singleton Azure Blob Storage client - sadece bir kez oluştur"""
    return AzureBlobStorage()

blob_storage = get_azure_blob_storage()

# Şifre hash fonksiyonu
def hash_password(password):
    """Şifreyi güvenli bir şekilde hash'le"""
    return hashlib.sha256(password.encode()).hexdigest()

# ============================================
# 🔐 GÜVENLİ "BENİ HATIRLA" (REMEMBER ME) SİSTEMİ
# ============================================
# Token-based authentication with rotation
# - Cookie'de ŞİFRE SAKLANMAZ, sadece token
# - Token her login'de rotate edilir
# - Token çalınma tespiti yapılır
# - Azure Blob Storage'da persistent_logins.json tutulur

PERSISTENT_LOGINS_FILE = "persistent_logins.json"
REMEMBER_ME_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 gün (saniye)

def generate_secure_token(length=32):
    """256-bit (32 byte) kriptografik olarak güvenli rastgele token üret"""
    import secrets
    return secrets.token_hex(length)

def generate_series_id():
    """128-bit (16 byte) series ID üret - kullanıcı için sabit kalır"""
    import secrets
    return secrets.token_hex(16)

def hash_token(token):
    """Token'ı SHA-256 ile hashle (DB'de düz metin saklanmaz)"""
    return hashlib.sha256(token.encode()).hexdigest()

def get_user_id_from_email(email):
    """Email'den benzersiz user_id oluştur (email saklanmaz cookie'de)"""
    return hashlib.sha256(email.lower().encode()).hexdigest()[:16]

def load_persistent_logins():
    """Persistent login kayıtlarını cookie'den yükle"""
    try:
        print(f"[COOKIE_GET_DEBUG] Loading persistent logins - COOKIES_AVAILABLE={COOKIES_AVAILABLE}, cookie_manager={cookie_manager is not None}")
        if COOKIES_AVAILABLE and cookie_manager is not None:
            logins_json = cookie_manager.get("finapp_persistent_logins")
            print(f"[COOKIE_GET_DEBUG] Retrieved cookie value: {logins_json[:100] if logins_json else 'NONE'}...")
            if logins_json:
                import base64
                decoded = base64.b64decode(logins_json.encode()).decode('utf-8')
                result = json.loads(decoded)
                print(f"[COOKIE_GET_DEBUG] ✅ Successfully loaded {len(result)} user(s) from cookie")
                return result
            else:
                print(f"[COOKIE_GET_DEBUG] ⚠️ Cookie is empty or not found")
        else:
            print(f"[COOKIE_GET_DEBUG] ❌ Cookie manager not available")
    except Exception as e:
        print(f"[REMEMBER ME] Load hatası: {e}")
        import traceback
        traceback.print_exc()
    return {}

def save_persistent_logins(logins):
    """Persistent login kayıtlarını cookie'ye kaydet"""
    try:
        if COOKIES_AVAILABLE and cookie_manager is not None:
            import base64
            json_data = json.dumps(logins, ensure_ascii=False)
            encoded = base64.b64encode(json_data.encode('utf-8')).decode()
            set_remember_cookie(
                "finapp_persistent_logins",
                encoded,
                datetime.now() + timedelta(days=30),
                "set_logins_save",
            )
            return True
    except Exception as e:
        print(f"[REMEMBER ME] Save hatası: {e}")
    return False

def create_remember_me_token(email, ip_address="", user_agent=""):
    """
    Yeni remember me token oluştur ve veritabanına kaydet
    
    Returns:
        str: base64 encoded cookie value (userId:seriesId:token)
        None: Hata durumunda
    """
    try:
        user_id = get_user_id_from_email(email)
        series_id = generate_series_id()
        token = generate_secure_token()
        token_hash = hash_token(token)
        
        # Yeni kayıt oluştur
        new_login = {
            "series_id": series_id,
            "token_hash": token_hash,
            "expires_at": (datetime.now() + timedelta(days=30)).isoformat(),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "email": email
        }
        
        # Cookie değerini oluştur (base64 encoded)
        import base64
        cookie_value = f"{user_id}:{series_id}:{token}"
        encoded_cookie = base64.b64encode(cookie_value.encode()).decode()
        
        # Login bilgisini session_state'e kaydet (cookie save login sırasında yapılacak)
        st.session_state['pending_login_data'] = {
            'user_id': user_id,
            'login_entry': new_login
        }
        
        return encoded_cookie
        
    except Exception as e:
        print(f"[REMEMBER ME] Token oluşturma hatası: {e}")
        return None

def validate_and_rotate_token(cookie_value, ip_address="", user_agent=""):
    """
    Cookie'den gelen token'ı doğrula ve rotate et
    
    Returns:
        tuple: (success: bool, email: str or None, new_cookie: str or None, warning: str or None)
    """
    try:
        import base64
        
        # Cookie'yi decode et
        decoded = base64.b64decode(cookie_value.encode()).decode()
        parts = decoded.split(":")
        
        if len(parts) != 3:
            return False, None, None, "Geçersiz cookie formatı"
        
        user_id, series_id, token = parts
        token_hash = hash_token(token)
        
        # Cookie'den logins yükle
        logins = load_persistent_logins()
        
        if user_id not in logins:
            return False, None, None, "Kullanıcı bulunamadı"
        
        # Series ID ile eşleşen kaydı bul
        matching_login = None
        for login in logins[user_id]:
            if login.get('series_id') == series_id:
                matching_login = login
                break
        
        if not matching_login:
            return False, None, None, "Oturum bulunamadı"
        
        # Token hash kontrolü
        if matching_login.get('token_hash') != token_hash:
            # ⚠️ TOKEN ÇALINMIŞ OLABİLİR!
            # Series ID doğru ama token yanlış = çalıntı token kullanımı
            logins[user_id] = []
            save_persistent_logins(logins)
            return False, None, None, "⚠️ Güvenlik uyarısı: Şüpheli aktivite tespit edildi. Lütfen tekrar giriş yapın."
        
        # Süre kontrolü
        expires_at = datetime.fromisoformat(matching_login.get('expires_at', '2000-01-01'))
        if datetime.now() > expires_at:
            logins[user_id] = [l for l in logins[user_id] if l.get('series_id') != series_id]
            save_persistent_logins(logins)
            return False, None, None, "Oturum süresi dolmuş"
        
        # IP veya User-Agent değişimi kontrolü (opsiyonel uyarı)
        warning = None
        if matching_login.get('ip_address') and matching_login.get('ip_address') != ip_address:
            warning = "IP adresi değişmiş"
        
        # ✅ Token geçerli - ROTATION yap
        email = matching_login.get('email', '')
        
        # Yeni token üret
        new_token = generate_secure_token()
        new_token_hash = hash_token(new_token)
        
        # Kaydı güncelle
        for login in logins[user_id]:
            if login.get('series_id') == series_id:
                login['token_hash'] = new_token_hash
                login['updated_at'] = datetime.now().isoformat()
                login['ip_address'] = ip_address
                login['user_agent'] = user_agent
                break
        
        save_persistent_logins(logins)
        
        # Yeni cookie değeri
        new_cookie_value = f"{user_id}:{series_id}:{new_token}"
        new_encoded_cookie = base64.b64encode(new_cookie_value.encode()).decode()
        
        return True, email, new_encoded_cookie, warning
        
    except Exception as e:
        print(f"[REMEMBER ME] Token doğrulama hatası: {e}")
        return False, None, None, str(e)

def revoke_remember_me_token(email=None, user_id=None, series_id=None):
    """
    Remember me token'ını iptal et
    
    Args:
        email: Kullanıcı email'i (tüm tokenları siler)
        user_id: User ID (tüm tokenları siler)
        series_id: Belirli bir series (sadece o token'ı siler)
    """
    try:
        logins = load_persistent_logins()
        
        if email:
            user_id = get_user_id_from_email(email)
        
        if user_id:
            if series_id:
                # Sadece belirli series'i sil
                if user_id in logins:
                    logins[user_id] = [l for l in logins[user_id] if l.get('series_id') != series_id]
            else:
                # Tüm tokenları sil
                logins[user_id] = []
            
            save_persistent_logins(logins)
            return True
            
    except Exception as e:
        print(f"[REMEMBER ME] Token iptal hatası: {e}")
    
    return False

def cleanup_expired_tokens():
    """Süresi dolmuş tüm tokenları temizle (bakım fonksiyonu)"""
    try:
        logins = load_persistent_logins()
        now = datetime.now()
        
        for user_id in logins:
            logins[user_id] = [
                login for login in logins[user_id]
                if datetime.fromisoformat(login.get('expires_at', '2000-01-01')) > now
            ]
        
        save_persistent_logins(logins)
        return True
    except Exception:
        return False

def get_client_info():
    """İstemci IP ve User-Agent bilgilerini al"""
    try:
        # Streamlit'te bu bilgilere doğrudan erişim sınırlı
        # Gerçek bir production ortamında reverse proxy header'larından alınır
        ip_address = "unknown"
        user_agent = "unknown"
        
        # Streamlit session'dan deneyebiliriz
        if hasattr(st, 'context'):
            # Streamlit 1.31+ için
            pass
        
        return ip_address, user_agent
    except Exception:
        return "unknown", "unknown"

# Eski fonksiyonları güncelle (uyumluluk için)
def save_remembered_credentials(email, password):
    """
    Remember Me token oluştur (ŞİFRE SAKLANMAZ!)
    Gerçek implementasyon JavaScript tarafında cookie ile yapılır
    """
    try:
        ip_address, user_agent = get_client_info()
        cookie_value = create_remember_me_token(email, ip_address, user_agent)
        
        if cookie_value:
            # Session state'e sadece geçici olarak sakla (JS'e iletmek için)
            st.session_state['remember_me_cookie'] = cookie_value
            st.session_state['remembered_email'] = email
            return True
    except Exception:
        pass
    return False

def load_remembered_credentials():
    """Session state'den email yükle (şifre SAKLANMAZ)"""
    try:
        email = st.session_state.get('remembered_email', '')
        # Şifre artık saklanmıyor, boş döner
        return email, ""
    except Exception:
        pass
    return "", ""

def clear_remembered_credentials():
    """Remember me token'ını iptal et ve session'ı temizle"""
    try:
        email = st.session_state.get('remembered_email', '')
        if email:
            revoke_remember_me_token(email=email)
        
        # Session state'den temizle
        for key in ['remembered_email', 'remember_me_cookie']:
            if key in st.session_state:
                st.session_state.pop(key)
        
        return True
    except Exception:
        pass
    return False

# Kullanıcı veritabanını yükle
@st.cache_data(ttl=60)  # 1 dakika cache
def load_users():
    """Kullanıcı veritabanını Azure Blob Storage'dan yükle"""
    # Önce Azure Blob Storage'dan dene
    if blob_storage.blob_service_client:
        blob_data = blob_storage.download_file(USERS_FILE)
        if blob_data:
            try:
                users = json.loads(blob_data.decode('utf-8'))
                return users
            except Exception as e:
                pass  # Hata durumunda boş dict döndür
    
    return {}

# Kullanıcı veritabanını kaydet
def save_users(users):
    """Kullanıcı veritabanını Azure Blob Storage'a kaydet"""
    # JSON string'e çevir
    json_data = json.dumps(users, ensure_ascii=False, indent=2)
    
    # Azure Blob Storage'a kaydet
    if blob_storage.blob_service_client:
        try:
            # Doğru parametrelerle upload_file fonksiyonunu çağır
            success = blob_storage.upload_file(file_name=USERS_FILE, data=json_data.encode('utf-8'))
            if success:
                print(f"✅ Kullanıcı verisi Azure Blob Storage'a kaydedildi")
                # Cache'i temizle ki değişiklikler hemen görünsün
                st.cache_data.clear()
            else:
                print(f"❌ Azure blob kaydetme başarısız")
        except Exception as e:
            print(f"Azure blob kaydetme hatası: {str(e)}")
    else:
        print("❌ Azure Blob Storage bağlantısı yok")

# Portföy veritabanını yükle
@st.cache_data(ttl=60)  # 1 dakika cache
def load_portfolios():
    """Portföy veritabanını Azure Blob Storage'dan yükle"""
    # Azure Blob Storage'dan dene
    if blob_storage.blob_service_client:
        blob_data = blob_storage.download_file(PORTFOLIOS_FILE)
        if blob_data:
            try:
                # download_file bytes döndürür, decode etmek gerekli
                portfolios = json.loads(blob_data.decode('utf-8'))
                return portfolios
            except Exception as e:
                pass  # Hata durumunda boş dict döndür
    
    return {}

# Portföy veritabanını kaydet
def save_portfolios(portfolios):
    """Portföy veritabanını Azure Blob Storage'a kaydet"""
    # JSON string'e çevir
    json_data = json.dumps(portfolios, ensure_ascii=False, indent=2)
    
    # Azure Blob Storage'a kaydet
    if blob_storage.blob_service_client:
        try:
            success = blob_storage.upload_file(file_name=PORTFOLIOS_FILE, data=json_data.encode('utf-8'))
            if success:
                # Cache'i temizle ki değişiklikler hemen görünsün
                st.cache_data.clear()
        except Exception as e:
            pass  # Hata durumunda sessizce devam et


# Job settings yükle/kaydet (scheduler ayarları)
@st.cache_data(ttl=30)
def load_job_settings():
    """Job (scheduler) ayarlarını Azure Blob Storage'dan yükle"""
    if blob_storage.blob_service_client:
        blob_data = blob_storage.download_file(JOB_SETTINGS_FILE)
        if blob_data:
            try:
                return json.loads(blob_data.decode('utf-8'))
            except Exception:
                pass
    return {}

def save_job_settings(settings: dict):
    """Job ayarlarını Azure Blob Storage'a kaydet"""
    json_data = json.dumps(settings, ensure_ascii=False, indent=2)
    if blob_storage.blob_service_client:
        try:
            success = blob_storage.upload_file(file_name=JOB_SETTINGS_FILE, data=json_data.encode('utf-8'))
            if success:
                st.cache_data.clear()
                return True
        except Exception:
            pass
    return False


# ================ ABONELİK YÖNETİM FONKSİYONLARI ================

def load_subscriptions():
    """Abonelik verilerini Azure Blob Storage'dan yükle"""
    if blob_storage.blob_service_client:
        blob_data = blob_storage.download_file(SUBSCRIPTIONS_FILE)
        if blob_data:
            try:
                return json.loads(blob_data.decode('utf-8'))
            except Exception:
                pass
    return {}

def save_subscriptions(subscriptions):
    """Abonelik verilerini Azure Blob Storage'a kaydet"""
    json_data = json.dumps(subscriptions, ensure_ascii=False, indent=2)
    if blob_storage.blob_service_client:
        try:
            success = blob_storage.upload_file(file_name=SUBSCRIPTIONS_FILE, data=json_data.encode('utf-8'))
            if success:
                return True
        except Exception:
            pass
    return False

def is_admin(email):
    """Kullanıcının admin olup olmadığını kontrol et"""
    return email.lower() in [e.lower() for e in ADMIN_EMAILS]

def get_user_subscription(email):
    """Kullanıcının abonelik bilgilerini getir"""
    subscriptions = load_subscriptions()
    return subscriptions.get(email.lower(), None)

def set_user_subscription(email, plan_key, start_date=None, end_date=None):
    """Kullanıcıya abonelik tanımla"""
    subscriptions = load_subscriptions()
    
    if start_date is None:
        start_date = datetime.now()
    
    if end_date is None and plan_key in SUBSCRIPTION_PLANS:
        months = SUBSCRIPTION_PLANS[plan_key]["months"]
        end_date = start_date + timedelta(days=months * 30)
    
    subscriptions[email.lower()] = {
        "plan": plan_key,
        "plan_name": SUBSCRIPTION_PLANS.get(plan_key, {}).get("name", "Özel Plan"),
        "start_date": start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else start_date,
        "end_date": end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else end_date,
        "status": "active",
        "is_active": True,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return save_subscriptions(subscriptions)

def cancel_subscription(email):
    """Kullanıcının aboneliğini iptal et"""
    subscriptions = load_subscriptions()
    if email.lower() in subscriptions:
        subscriptions[email.lower()]["status"] = "cancelled"
        subscriptions[email.lower()]["cancelled_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return save_subscriptions(subscriptions)
    return False

def is_subscription_active(email):
    """Kullanıcının aktif aboneliği var mı kontrol et"""
    # Admin her zaman erişebilir
    if is_admin(email):
        return True
    
    subscription = get_user_subscription(email)
    if not subscription:
        return False
    
    # İptal edilmiş abonelik kontrolü
    if subscription.get("status") == "cancelled":
        return False
    
    # Bitiş tarihi kontrolü
    end_date_str = subscription.get("end_date")
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            # Bitiş tarihi geçmişse aktif değil
            if datetime.now() > end_date:
                return False
            # Bitiş tarihi geçmemişse ve status active ise aktif
            if subscription.get("status") == "active":
                return True
        except:
            pass
    
    # is_active alanı varsa ona bak
    if subscription.get("is_active", False):
        return True
    
    # Status active ise kabul et (eski kayıtlar için)
    if subscription.get("status") == "active":
        return True
    
    return False

def get_subscription_days_remaining(email):
    """Abonelik bitimine kalan gün sayısını döndür"""
    subscription = get_user_subscription(email)
    if not subscription:
        return 0
    
    end_date_str = subscription.get("end_date")
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            remaining = (end_date - datetime.now()).days
            return max(0, remaining)
        except:
            pass
    return 0


def read_logs_from_blob(blob_name: str):
    """Read a JSON log array from blob; return list (empty on error)."""
    try:
        if blob_storage and blob_storage.blob_service_client:
            data = blob_storage.download_file(blob_name)
            if data:
                return json.loads(data.decode('utf-8'))
    except Exception:
        pass
    return []


def write_logs_to_blob(blob_name: str, logs: list):
    """Write JSON array to blob; return True on success."""
    json_data = json.dumps(logs, ensure_ascii=False, indent=2)
    try:
        if blob_storage and blob_storage.blob_service_client:
            success = blob_storage.upload_file(file_name=blob_name, data=json_data.encode('utf-8'))
            if success:
                return True
    except Exception:
        pass
    return False

# Kullanıcı doğrulama
def authenticate_user(email, password):
    """Kullanıcı girişini doğrula"""
    
    # TEST KULLANICISI - erdalural@gmail.com için özel giriş
    if email == "erdalural@gmail.com" and password == "Eura654321?":
        return True
    
    users = load_users()
    if email in users:
        return users[email]['password'] == hash_password(password)
    return False

# E-posta doğrulama fonksiyonları
def generate_verification_code():
    """6 haneli rastgele doğrulama kodu oluştur"""
    return ''.join(random.choices(string.digits, k=6))

def send_verification_email(email, verification_code):
    """Doğrulama kodunu e-posta ile gönder"""
    try:
        # SMTP ayarları environment variables'dan al (test_email.py ile aynı)
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        sender_email = os.getenv('SMTP_USERNAME') or os.getenv('EMAIL_FROM')
        sender_password = os.getenv('SMTP_PASSWORD', '')
        
        # Password boşluklarını temizle (Gmail App Password formatı)
        sender_password = sender_password.replace(' ', '')
        
        # E-posta bilgileri eksikse hata ver
        if not sender_email or not sender_password:
            raise ValueError("E-posta bilgileri environment variables'da tanımlı değil")
        
        # E-posta içeriği
        subject = "🔐 Hesap Doğrulama Kodu - Finans Platformu"
        body = f"""
        Merhaba,
        
        Finans Platformu hesabınızı doğrulamak için aşağıdaki kodu kullanın:
        
        Doğrulama Kodu: {verification_code}
        
        Bu kod 10 dakika geçerlidir.
        
        Eğer bu hesabı siz oluşturmadıysanız, lütfen bu e-postayı görmezden gelin.
        
        İyi günler,
        Finans Platformu Ekibi
        """
        
        # E-posta oluştur
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # E-postayı gönder (test_email.py ile aynı)
        print(f"[EMAIL] SMTP Server: {smtp_server}:{smtp_port}")
        print(f"[EMAIL] From: {sender_email} → To: {email}")
        print(f"[EMAIL] Password length: {len(sender_password)}")
        
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        
        print(f"[EMAIL] ✅ Email başarıyla gönderildi!")
        
        return True, "Doğrulama kodu e-posta adresinize gönderildi!"
        
    except Exception as e:
        # Hata mesajını logla ve kullanıcıya göster
        error_msg = f"E-posta gönderme hatası: {str(e)}"
        print(f"[EMAIL ERROR] {error_msg}")
        print(f"SMTP Config - Server: {smtp_server}, Port: {smtp_port}, User: {sender_email}, Pass: {'*' * len(sender_password) if sender_password else 'NONE'}")
        
        # Test için kodu göster
        st.error(f"❌ {error_msg}")
        st.info(f"🧪 Test modu: Doğrulama kodunuz: {verification_code}")
        return False, f"E-posta gönderilemedi: {str(e)}"

def send_feedback_email(feedback_type, subject, message, user_email, user_name):
    """Kullanıcı geri bildirimini adminie gönder ve Azure'a kaydet"""
    try:
        # SMTP ayarları
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        sender_email = os.getenv('SMTP_USERNAME') or os.getenv('EMAIL_FROM')
        sender_password = os.getenv('SMTP_PASSWORD', '')
        sender_password = sender_password.replace(' ', '')
        
        if not sender_email or not sender_password:
            raise ValueError("E-posta bilgileri environment variables'da tanımlı değil")
        
        # Admin email
        admin_email = "infofinansapp@gmail.com"
        
        # Türkçe başlık
        feedback_types = {
            "sikayet": "🔴 ŞİKAYET",
            "oneri": "💡 ÖNERİ",
            "bilgi_talebi": "❓ BİLGİ TALEBİ"
        }
        
        feedback_type_label = feedback_types.get(feedback_type, feedback_type)
        
        # Admin'e gönderilecek email
        admin_subject = f"[{feedback_type_label}] {subject} - Kullanıcı: {user_name}"
        admin_body = f"""
        Geri Bildirim Türü: {feedback_type_label}
        Kullanıcı Adı: {user_name}
        Kullanıcı E-posta: {user_email}
        
        Konsu: {subject}
        
        Mesaj:
        {message}
        
        ---
        Gönderim Tarihi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        # User'a gönderilecek confirmation email
        user_subject = "✅ Geri Bildirimiz Alındı - Finans Platformu"
        user_body = f"""
        Merhaba {user_name},
        
        Geri bildiriminiz başarıyla alınmıştır. 
        
        Bildirim Türü: {feedback_type_label}
        Konsu: {subject}
        
        En kısa zamanda sizin ile iletişime geçilecektir.
        
        Teşekkür ederiz,
        Finans Platformu Ekibi
        """
        
        # Admin'e email gönder
        msg_admin = MIMEMultipart()
        msg_admin['From'] = sender_email
        msg_admin['To'] = admin_email
        msg_admin['Subject'] = admin_subject
        msg_admin['Reply-To'] = user_email
        msg_admin.attach(MIMEText(admin_body, 'plain', 'utf-8'))
        
        # User'a confirmation email gönder
        msg_user = MIMEMultipart()
        msg_user['From'] = sender_email
        msg_user['To'] = user_email
        msg_user['Subject'] = user_subject
        msg_user.attach(MIMEText(user_body, 'plain', 'utf-8'))
        
        # Email'leri gönder
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg_admin)
        server.send_message(msg_user)
        server.quit()
        
        # Azure'a kaydet
        try:
            blob_client = get_blob_service_client()
            feedback_data = {
                'timestamp': datetime.now().isoformat(),
                'type': feedback_type,
                'subject': subject,
                'message': message,
                'user_email': user_email,
                'user_name': user_name,
                'status': 'received'
            }
            
            # Feedback listesini oku veya yeni oluştur
            try:
                feedback_blob = blob_client.get_blob_client(container="logs", blob="feedback_submissions.json")
                feedback_list = json.loads(feedback_blob.download_blob().readall().decode('utf-8'))
            except:
                feedback_list = []
            
            # Yeni feedback'i ekle
            feedback_list.append(feedback_data)
            
            # Geri kaydet
            feedback_blob = blob_client.get_blob_client(container="logs", blob="feedback_submissions.json")
            feedback_blob.upload_blob(json.dumps(feedback_list, ensure_ascii=False, indent=2), overwrite=True)
        except Exception as e:
            print(f"[FEEDBACK STORAGE] Azure depolama hatası: {str(e)}")
        
        print(f"[FEEDBACK] ✅ Geri bildirim başarıyla gönderildi!")
        return True, "✅ Geri bildiriminiz başarıyla alındı. En kısa zamanda sizinle iletişime geçilecektir."
        
    except Exception as e:
        error_msg = f"Geri bildirim gönderme hatası: {str(e)}"
        print(f"[FEEDBACK ERROR] {error_msg}")
        return False, f"❌ {error_msg}"

def send_new_user_notification(user_email, user_name):
    """Yeni kullanıcı kaydı yapıldığında admin'e bilgilendirme maili gönder"""
    try:
        # SMTP ayarları
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        sender_email = os.getenv('SMTP_USERNAME') or os.getenv('EMAIL_FROM')
        sender_password = os.getenv('SMTP_PASSWORD', '')
        sender_password = sender_password.replace(' ', '')
        
        if not sender_email or not sender_password:
            raise ValueError("E-posta bilgileri environment variables'da tanımlı değil")
        
        # Admin email
        admin_email = "infofinansapp@gmail.com"
        
        # Admin'e gönderilecek email
        admin_subject = f"🆕 Yeni Kullanıcı Kaydı - {user_name}"
        admin_body = f"""
        Yeni bir kullanıcı platformaya kayıt olmuştur.
        
        Kullanıcı Bilgileri:
        ─────────────────
        Ad/Soyadı: {user_name}
        E-posta: {user_email}
        Kayıt Tarihi: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Abonelik: 30 Günlük Deneme (Otomatik Tanımlandı)
        Deneme Bitiş: {(datetime.now() + timedelta(days=TRIAL_PERIOD_DAYS)).strftime('%Y-%m-%d')}
        
        ---
        Finans Platformu Otomatik Bildirimi
        """
        
        # Admin'e email gönder
        msg_admin = MIMEMultipart()
        msg_admin['From'] = sender_email
        msg_admin['To'] = admin_email
        msg_admin['Subject'] = admin_subject
        msg_admin.attach(MIMEText(admin_body, 'plain', 'utf-8'))
        
        # Email'i gönder
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg_admin)
        server.quit()
        
        print(f"[NEW USER] ✅ Admin bilgilendirme maili gönderildi: {admin_email}")
        return True
        
    except Exception as e:
        error_msg = f"Yeni kullanıcı maili gönderme hatası: {str(e)}"
        print(f"[NEW USER ERROR] {error_msg}")
        return False

def store_verification_code(email, code):
    """Doğrulama kodunu geçici olarak sakla"""
    if 'verification_codes' not in st.session_state:
        st.session_state['verification_codes'] = {}
    
    st.session_state['verification_codes'][email] = {
        'code': code,
        'timestamp': datetime.now(),
        'verified': False
    }

def verify_code(email, entered_code):
    """Girilen kodu doğrula"""
    if 'verification_codes' not in st.session_state:
        return False, "Doğrulama kodu bulunamadı!"
    
    if email not in st.session_state['verification_codes']:
        return False, "Bu e-posta için doğrulama kodu bulunamadı!"
    
    stored_data = st.session_state['verification_codes'][email]
    stored_code = stored_data['code']
    timestamp = stored_data['timestamp']
    
    # Kod 10 dakika geçerli
    if datetime.now() - timestamp > timedelta(minutes=10):
        del st.session_state['verification_codes'][email]
        return False, "Doğrulama kodu süresi doldu! Lütfen yeni kod isteyin."
    
    if stored_code == entered_code:
        st.session_state['verification_codes'][email]['verified'] = True
        return True, "E-posta doğrulandı!"
    else:
        return False, "Doğrulama kodu hatalı!"

# Kullanıcı kayıt logunu tut
def log_user_registration(email, name, accepted_docs):
    """Kullanıcı kayıt logunu Azure Blob Storage'a kaydet"""
    try:
        # Log kaydı oluştur
        registration_log = {
            'timestamp': datetime.now().isoformat(),
            'email': email,
            'name': name,
            'accepted_documents': accepted_docs or {},
            'registration_type': 'new_user'
        }
        
        # Log dosya adı (günlük log dosyaları)
        log_date = datetime.now().strftime('%Y-%m-%d')
        log_filename = f"registration_logs_{log_date}.json"
        
        # Mevcut logları yükle
        existing_logs = []
        if blob_storage.file_exists(log_filename):
            log_data = blob_storage.download_file(log_filename, silent=True)
            if log_data:
                try:
                    existing_logs = json.loads(log_data.decode('utf-8'))
                except:
                    existing_logs = []
        
        # Yeni logu ekle
        existing_logs.append(registration_log)
        
        # Logları geri kaydet
        log_json = json.dumps(existing_logs, indent=2, ensure_ascii=False, default=str).encode('utf-8')
        blob_storage.upload_file(log_filename, log_json, silent=True)
        
        # Yerel CSV logunu da tut
        log_csv_filename = f"registration_logs_{log_date}.csv"
        import csv
        import io
        
        # CSV verisi hazırla
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Header (sadece ilk kaydında)
        if len(existing_logs) == 1:
            csv_writer.writerow(['Tarih & Saat', 'Email', 'Ad Soyad', 'Kabul Edilen Dökümanlar'])
        
        # Veri satırı
        accepted_docs_str = ', '.join([f"{k}: {v}" for k, v in (accepted_docs or {}).items()])
        csv_writer.writerow([
            registration_log['timestamp'],
            email,
            name,
            accepted_docs_str
        ])
        
        # CSV'yi blob'a kaydet
        csv_data = csv_buffer.getvalue().encode('utf-8')
        blob_storage.upload_file(log_csv_filename, csv_data, silent=True)
        
        debug_logger.info('REGISTRATION_LOG', f'User registration logged: {email}', {
            'name': name,
            'log_file': log_filename
        })
        
    except Exception as e:
        debug_logger.error('REGISTRATION_LOG', f'Failed to log registration for {email}', {
            'error': str(e)
        })
        pass  # Loglama başarısız olsa bile kayıt devam etsin
        return False, "Yanlış doğrulama kodu!"

def is_email_verified(email):
    """E-posta doğrulanmış mı kontrol et"""
    if 'verification_codes' not in st.session_state:
        return False
    
    if email not in st.session_state['verification_codes']:
        return False
    
    return st.session_state['verification_codes'][email]['verified']

# Kullanıcı kaydı
def register_user(email, password, name, accepted_docs=None):
    """Yeni kullanıcı kaydı oluştur"""
    # E-posta doğrulaması kontrol et
    if not is_email_verified(email):
        return False, "Lütfen önce e-posta adresinizi doğrulayın!"
    
    users = load_users()
    if email in users:
        return False, "Bu email adresi zaten kayıtlı!"
    
    users[email] = {
        'password': hash_password(password),
        'name': name,
        'created_at': datetime.now().isoformat(),
        'email_verified': True,
        'accepted_docs': accepted_docs or {}
    }
    save_users(users)
    
    # Kullanıcı için boş portföy oluştur
    portfolios = load_portfolios()
    portfolios[email] = {
        'transactions': [],
        'created_at': datetime.now().isoformat()
    }
    save_portfolios(portfolios)
    
    # Yeni kullanıcıya 1 aylık ücretsiz deneme aboneliği tanımla
    try:
        subscriptions = load_subscriptions()
        start_date = datetime.now()
        end_date = start_date + timedelta(days=TRIAL_PERIOD_DAYS)
        subscriptions[email.lower()] = {
            "plan": "trial",
            "plan_name": "Deneme (Ücretsiz)",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "status": "active",
            "is_active": True,
            "is_trial": True,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_by": "system_auto_trial"
        }
        save_subscriptions(subscriptions)
        print(f"[TRIAL SUBSCRIPTION] 30 gün deneme tanımlandı: {email}")
    except Exception as e:
        print(f"[TRIAL ERROR] Deneme aboneliği tanımlanamadı: {e}")
    
    # Doğrulama kodunu temizle
    if 'verification_codes' in st.session_state and email in st.session_state['verification_codes']:
        del st.session_state['verification_codes'][email]
    
    # Kullanıcı kayıt logunu tut
    log_user_registration(email, name, accepted_docs)
    
    # Admin'e yeni kullanıcı bilgilendirme maili gönder
    try:
        send_new_user_notification(email, name)
    except Exception as e:
        print(f"[NEW USER EMAIL] Admin bilgilendirme maili gönderilemedi: {str(e)}")
    
    return True, "Hesap başarıyla oluşturuldu!"

# Güncel kur çeviricisi
def get_currency_rate(from_currency, to_currency, date=None):
    """Belirtilen tarihte para birimi kurunu al (Frankfurter API kullanarak)"""
    max_retries = 3
    retry_delay = 1  # saniye
    
    try:
        if from_currency == to_currency:
            logging.debug(f"Currency same: {from_currency} = {to_currency}, returning 1.0")
            return 1.0
        
        # Özel durumlar için dönüşüm
        currency_mapping = {
            '₺': 'TRY',
            '$': 'USD',
            '€': 'EUR',
            '£': 'GBP'
        }
        
        from_curr = currency_mapping.get(from_currency, from_currency)
        to_curr = currency_mapping.get(to_currency, to_currency)
        
        logging.info(f"Converting currency: {from_curr} → {to_curr} (date: {date})")
        
        # Frankfurter.app API kullan (ücretsiz, rate limit yok, Azure uyumlu)
        # NOT: Frankfurter sadece EUR base currency kullanıyor, TRY desteklemiyor
        # Bu yüzden TRY için Yahoo Finance fallback gerekli
        
        # TRY kurları için özel işlem (Frankfurter TRY desteklemiyor)
        if from_curr == 'TRY' or to_curr == 'TRY':
            # TCMB EVDS API veya alternatif kaynak kullan
            return get_try_exchange_rate(from_curr, to_curr, date)
        
        # Diğer para birimleri için Frankfurter API
        for attempt in range(max_retries):
            try:
                if date is not None:
                    # Tarih string ise date objesine çevir (hem DD/MM/YYYY hem YYY-MM-DD formatını destekle)
                    if isinstance(date, str):
                        try:
                            date_obj = datetime.strptime(date, '%d/%m/%Y').date()
                        except:
                            try:
                                date_obj = datetime.strptime(date, '%Y-%m-%d').date()
                            except:
                                date_obj = None
                    elif isinstance(date, datetime):
                        date_obj = date.date()
                    else:
                        date_obj = date
                    
                    # Geçmiş kur al
                    if date_obj:
                        url = f"https://api.frankfurter.app/{date_obj.strftime('%Y-%m-%d')}"
                    else:
                        url = "https://api.frankfurter.app/latest"
                else:
                    # Güncel kur al
                    url = "https://api.frankfurter.app/latest"
                
                params = {
                    'from': from_curr,
                    'to': to_curr
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    rate = data['rates'][to_curr]
                    logging.info(f"Currency rate fetched from Frankfurter: {from_curr} → {to_curr} = {rate}")
                    return float(rate)
                else:
                    logging.warning(f"Frankfurter API error: {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return 1.0
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Kur alma hatası (deneme {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Kur alınamadı ({from_curr}/{to_curr}): {str(e)}")
                    return 1.0
        
        return 1.0
        
    except Exception as e:
        logging.error(f"Kur bilgisi alınırken hata: {str(e)}")
        return 1.0

def get_try_exchange_rate(from_curr, to_curr, date=None):
    """TRY kurları için özel fonksiyon (TCMB - Türkiye Cumhuriyet Merkez Bankası API)"""
    max_retries = 3
    retry_delay = 1
    
    try:
        # TCMB Döviz Kurları API - Resmi, ücretsiz, limit yok
        # Kaynak: https://www.tcmb.gov.tr/kurlar/today.xml
        
        for attempt in range(max_retries):
            try:
                # Tarihsel veri için güncel kur kullan (TCMB tarihsel API karmaşık)
                # Geçmiş tarihler için 404 hatası alınabiliyor
                if date is not None:
                    logging.info(f"TCMB: Tarihsel kur yerine güncel kur kullanılıyor (tarih: {date})")
                
                # Her zaman güncel kur kullan
                url = "https://www.tcmb.gov.tr/kurlar/today.xml"
                
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    # XML parse et
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(response.content)
                    
                    # Kur kodlarını map et (TCMB formatı)
                    tcmb_currency_map = {
                        'USD': 'USD',
                        'EUR': 'EUR', 
                        'GBP': 'GBP',
                        'TRY': 'TRY'
                    }
                    
                    # TRY dönüşümleri için mantık
                    if from_curr == 'TRY':
                        # TRY → Diğer para birimi (örn: TRY → USD)
                        # TCMB'den USD/TRY alıp tersini al
                        target = tcmb_currency_map.get(to_curr)
                        if not target:
                            logging.error(f"TCMB desteklemiyor: {to_curr}")
                            return 1.0
                        
                        for currency in root.findall('Currency'):
                            code = currency.get('CurrencyCode')
                            if code == target:
                                # ForexSelling = Döviz Satış (TRY cinsinden)
                                forex_selling = currency.find('ForexSelling')
                                if forex_selling is not None and forex_selling.text:
                                    try_to_foreign = float(forex_selling.text)
                                    # TRY → USD = 1 / USD_TRY
                                    rate = 1.0 / try_to_foreign
                                    logging.info(f"TRY rate from TCMB: {from_curr} → {to_curr} = {rate}")
                                    return rate
                        
                        logging.error(f"TCMB'de kur bulunamadı: {to_curr}")
                        return 1.0
                        
                    elif to_curr == 'TRY':
                        # Diğer para birimi → TRY (örn: USD → TRY)
                        source = tcmb_currency_map.get(from_curr)
                        if not source:
                            logging.error(f"TCMB desteklemiyor: {from_curr}")
                            return 1.0
                        
                        for currency in root.findall('Currency'):
                            code = currency.get('CurrencyCode')
                            if code == source:
                                # ForexBuying = Döviz Alış (TRY cinsinden)
                                forex_buying = currency.find('ForexBuying')
                                if forex_buying is not None and forex_buying.text:
                                    rate = float(forex_buying.text)
                                    logging.info(f"TRY rate from TCMB: {from_curr} → {to_curr} = {rate}")
                                    return rate
                        
                        logging.error(f"TCMB'de kur bulunamadı: {from_curr}")
                        return 1.0
                    else:
                        # TRY içermeyen dönüşüm (USD → EUR gibi)
                        # Frankfurter API kullan, TCMB'den çapraz kur hesaplama
                        logging.info(f"TCMB: TRY içermeyen çapraz kur, Frankfurter'a yönlendiriliyor")
                        
                        # Frankfurter API'ye fallback
                        try:
                            url_frank = "https://api.frankfurter.app/latest"
                            params = {'from': from_curr, 'to': to_curr}
                            resp_frank = requests.get(url_frank, params=params, timeout=10)
                            
                            if resp_frank.status_code == 200:
                                data = resp_frank.json()
                                rate = float(data['rates'][to_curr])
                                logging.info(f"Cross rate from Frankfurter: {from_curr} → {to_curr} = {rate}")
                                return rate
                        except Exception as e:
                            logging.warning(f"Frankfurter fallback failed: {e}")
                        
                        # Frankfurter başarısız, TCMB üzerinden çapraz hesapla
                        usd_to_try = None
                        target_to_try = None
                        
                        for currency in root.findall('Currency'):
                            code = currency.get('CurrencyCode')
                            forex_buying = currency.find('ForexBuying')
                            
                            if code == from_curr and forex_buying is not None:
                                usd_to_try = float(forex_buying.text)
                            if code == to_curr and forex_buying is not None:
                                target_to_try = float(forex_buying.text)
                        
                        if usd_to_try and target_to_try:
                            # USD → EUR = (USD/TRY) / (EUR/TRY)
                            rate = usd_to_try / target_to_try
                            logging.info(f"Cross rate from TCMB: {from_curr} → {to_curr} = {rate}")
                            return rate
                        
                        logging.error(f"TCMB çapraz kur hesaplanamadı: {from_curr}/{to_curr}")
                        return 1.0
                        
                else:
                    logging.warning(f"TCMB API error: {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return 1.0
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"TCMB kur hatası (deneme {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"TCMB kur alınamadı ({from_curr}/{to_curr}): {str(e)}")
                    return 1.0
        
        return 1.0
        
    except Exception as e:
        logging.error(f"TCMB kur işleminde hata: {str(e)}")
        return 1.0

# ================ TEFAS HIZLI VERİ YÖNETİMİ (PARQUET) ================

# Parquet dosya yolları - 10-50x daha hızlı!
TEFAS_DATA_FILE = "tefas_data.parquet"
TEFAS_FUNDS_FILE = "tefas_funds.parquet" 
TEFAS_SUMMARY_FILE = "tefas_summary.parquet"
TEFAS_CACHE_FILE = "tefas_cache.json"  # Hızlı erişim için memory cache
BIST_STOCKS_FILE = "bist_stocks.parquet"  # BIST hisse listesi için Parquet dosyası
NASDAQ_STOCKS_FILE = "nasdaq_stocks.parquet"  # NASDAQ hisse listesi için Parquet dosyası

# Veri tipleri optimize edilmiş şema
TEFAS_SCHEMA = pa.schema([
    pa.field('Tarih', pa.date32()),
    pa.field('Fon_Kodu', pa.string()),
    pa.field('Fon_Adi', pa.string()),
    pa.field('Fiyat', pa.float64()),
    pa.field('Gunluk_Getiri', pa.float32()),
    pa.field('Toplam_Deger', pa.float64()),
    pa.field('Pay_Sayisi', pa.float64()),
    pa.field('Kategori', pa.string()),
    pa.field('Guncelleme_Zamani', pa.timestamp('ms'))
])

class TefasDataManager:
    """TEFAS verilerini Azure Blob Storage'dan hızlı okuma/yazma için optimize edilmiş sınıf"""
    
    def __init__(self):
        self.data_file = TEFAS_DATA_FILE
        self.funds_file = TEFAS_FUNDS_FILE
        self.summary_file = TEFAS_SUMMARY_FILE
        self.cache_file = TEFAS_CACHE_FILE
        self.memory_cache = {}
        self.blob_storage = AzureBlobStorage()
        self._load_cache()
    
    def _load_cache(self):
        """Memory cache'i Azure Blob Storage'dan yükle - Sessiz mod"""
        try:
            # Azure Blob Storage'dan cache dosyasını indir (varsa) - sessizce
            cache_content = self.blob_storage.download_file(self.cache_file, silent=True)
            if cache_content:
                self.memory_cache = json.loads(cache_content.decode('utf-8'))
            else:
                self.memory_cache = {}
        except Exception as e:
            # Cache yüklenmediyse sessizce yeni bir cache başlat - log yapmadan
            self.memory_cache = {}    
    def _save_cache(self):
        """Memory cache'i Azure Blob Storage'a kaydet"""
        try:
            cache_json = json.dumps(self.memory_cache, ensure_ascii=False, indent=2)
            success = self.blob_storage.upload_file(
                file_content=cache_json.encode('utf-8'),
                blob_name=self.cache_file
            )
        except Exception as e:
            pass  # Cache kaydedilmedi, sessizce devam et
    
    def ensure_data_structure(self) -> bool:
        """Azure Blob Storage'da veri yapısını kontrol et ve oluştur"""
        try:
            # Ana veri dosyası Azure'da yoksa boş DataFrame oluştur
            data_content = self.blob_storage.download_file(self.data_file)
            if not data_content:
                empty_df = pd.DataFrame(columns=[
                    'Tarih', 'Fon_Kodu', 'Fon_Adi', 'Fiyat', 'Gunluk_Getiri',
                    'Toplam_Deger', 'Pay_Sayisi', 'Kategori', 'Guncelleme_Zamani'
                ])
                # Doğru veri tiplerini ayarla
                empty_df['Tarih'] = pd.to_datetime(empty_df['Tarih'])
                empty_df['Fiyat'] = empty_df['Fiyat'].astype('float64')
                empty_df['Gunluk_Getiri'] = empty_df['Gunluk_Getiri'].astype('float32')
                empty_df['Toplam_Deger'] = empty_df['Toplam_Deger'].astype('float64')
                empty_df['Pay_Sayisi'] = empty_df['Pay_Sayisi'].astype('float64')
                empty_df['Guncelleme_Zamani'] = pd.to_datetime(empty_df['Guncelleme_Zamani'])
                
                # Parquet formatında Azure'a kaydet
                parquet_buffer = io.BytesIO()
                empty_df.to_parquet(parquet_buffer, compression='snappy', index=False)
                parquet_buffer.seek(0)
                
                success = self.blob_storage.upload_file(
                    file_content=parquet_buffer.getvalue(),
                    blob_name=self.data_file
                )
                if success:
                    st.success(f"✅ TEFAS Parquet dosyası Azure'da oluşturuldu: {self.data_file}")
            
            # Fon listesi dosyası
            funds_content = self.blob_storage.download_file(self.funds_file)
            if not funds_content:
                funds_df = pd.DataFrame(columns=[
                    'Fon_Kodu', 'Fon_Adi', 'Kategori', 'Yonetim_Sirketi', 
                    'Son_Guncelleme', 'Aktif_Mi'
                ])
                # Parquet formatında Azure'a kaydet
                parquet_buffer = io.BytesIO()
                funds_df.to_parquet(parquet_buffer, compression='snappy', index=False)
                parquet_buffer.seek(0)
                
                self.blob_storage.upload_file(
                    file_content=parquet_buffer.getvalue(),
                    blob_name=self.funds_file
                )
            
            # Özet istatistikler dosyası
            summary_content = self.blob_storage.download_file(self.summary_file)
            if not summary_content:
                summary_df = pd.DataFrame(columns=[
                    'Tarih', 'Toplam_Fon_Sayisi', 'Pozitif_Getiri', 'Negatif_Getiri',
                    'Ortalama_Getiri', 'En_Yuksek_Getiri', 'En_Dusuk_Getiri', 'Guncelleme_Zamani'
                ])
                # Parquet formatında Azure'a kaydet
                parquet_buffer = io.BytesIO()
                summary_df.to_parquet(parquet_buffer, compression='snappy', index=False)
                parquet_buffer.seek(0)
                
                self.blob_storage.upload_file(
                    file_content=parquet_buffer.getvalue(),
                    blob_name=self.summary_file
                )
            
            return True
            
        except Exception as e:
            st.error(f"❌ TEFAS veri yapısı Azure'da oluşturulurken hata: {str(e)}")
            return False
    
    def upsert_fund_data(self, target_date: datetime, fund_code: str, fund_name: str, 
                        price: float, total_value: float, unit_count: float) -> str:
        """Fon verilerini güncelle veya ekle - Azure Blob Storage ile ULTRA HIZLI"""
        try:
            # Cache key oluştur
            cache_key = f"{target_date.strftime('%Y-%m-%d')}_{fund_code}"
            
            # Fiyatı 6 basamak hassasiyetle yuvarla
            rounded_price = round(float(price), 6)
            
            # Yeni veri satırı - Azure'daki mevcut dosyanın sütun isimleriyle eşleşmeli (boşluklarla)
            new_data = {
                'Tarih': target_date,
                'Fon Kodu': fund_code,  # Boşluklu - Azure'daki dosyayla uyumlu
                'Fon Adı': fund_name,   # Boşluklu - Azure'daki dosyayla uyumlu
                'Fiyat': rounded_price,
                'Tedavüldeki Pay Sayısı': round(float(unit_count), 2),  # Pay sayısı 2 basamak
                'Kişi Sayısı': 0,  # Varsayılan
                'Fon Toplam Değer': round(float(total_value), 2),  # Toplam değer 2 basamak
            }
            
            # Memory cache'de mevcut mu kontrol et
            if cache_key in self.memory_cache:
                # Güncelle
                self.memory_cache[cache_key] = new_data
                return "updated"
            else:
                # Yeni ekle
                self.memory_cache[cache_key] = new_data
                return "inserted"
                
        except Exception as e:
            return f"error: {str(e)}"
    
    def bulk_save_to_parquet(self) -> bool:
        """Memory cache'deki tüm veriyi toplu olarak Azure Blob Storage'a Parquet formatında kaydet - OPTİMİZE EDİLDİ"""
        try:
            if not self.memory_cache:
                st.info("ℹ️ Kaydedilecek yeni veri yok (cache boş)")
                return True
            
            st.info(f"💾 {len(self.memory_cache)} kayıt Azure'a yazılıyor...")
            
            # Memory cache'i DataFrame'e çevir
            new_df = pd.DataFrame(list(self.memory_cache.values()))
            
            # Veri tiplerini optimize et - Azure'daki mevcut dosyanın sütun isimleriyle
            new_df['Tarih'] = pd.to_datetime(new_df['Tarih'])
            new_df['Fiyat'] = new_df['Fiyat'].astype('float64')
            new_df['Fon Toplam Değer'] = new_df['Fon Toplam Değer'].astype('float64')
            new_df['Tedavüldeki Pay Sayısı'] = new_df['Tedavüldeki Pay Sayısı'].astype('float64')
            new_df['Kişi Sayısı'] = new_df['Kişi Sayısı'].astype('int64')
            
            # Mevcut veriyi Azure'dan oku (varsa) - TIMEOUT EKLENDİ
            try:
                existing_content = self.blob_storage.download_file(self.data_file, silent=True)
                if existing_content and len(existing_content) > 100:  # En az 100 byte olmalı
                    parquet_buffer = io.BytesIO(existing_content)
                    existing_df = pd.read_parquet(parquet_buffer)
                    
                    # Eğer mevcut veri varsa duplicate'leri çıkar
                    if not existing_df.empty:
                        # Duplicate'leri çıkar (tarih + fon_kodu kombinasyonu)
                        existing_df = existing_df[~existing_df.set_index(['Tarih', 'Fon Kodu']).index.isin(
                            new_df.set_index(['Tarih', 'Fon Kodu']).index
                        )]
                        
                        # Birleştir
                        final_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        final_df = new_df
                else:
                    # Dosya boş veya yoksa sadece yeni veri
                    final_df = new_df
            except Exception as read_error:
                st.warning(f"⚠️ Mevcut veri okunamadı, sadece yeni veri yazılacak: {str(read_error)}")
                final_df = new_df
            
            # Tarihe göre sırala
            final_df = final_df.sort_values(['Tarih', 'Fon Kodu'])
            
            # Parquet formatında Azure'a kaydet - SNAPPY sıkıştırma ile
            parquet_buffer = io.BytesIO()
            final_df.to_parquet(
                parquet_buffer, 
                compression='snappy', 
                index=False,
                engine='pyarrow'
            )
            parquet_buffer.seek(0)
            
            success = self.blob_storage.upload_file(
                file_content=parquet_buffer.getvalue(),
                blob_name=self.data_file
            )
            
            if success:
                # Cache'i kaydet ve temizle
                self._save_cache()
                st.success(f"✅ {len(new_df)} TEFAS satırı Azure Blob Storage'a kaydedildi (Toplam: {len(final_df)} satır)")
                return True
            else:
                st.error("❌ TEFAS verileri Azure'a kaydedilemedi")
                return False
            
        except Exception as e:
            st.error(f"❌ TEFAS bulk kayıt hatası: {str(e)}")
            import traceback
            st.error(f"Detay: {traceback.format_exc()}")
            return False
    
    def get_fund_price(self, fund_code: str, target_date: datetime) -> Optional[Dict]:
        """Belirli tarih için fon fiyatını al - Azure Blob Storage ile HIZLI"""
        try:
            # Önce cache'de ara
            cache_key = f"{target_date.strftime('%Y-%m-%d')}_{fund_code}"
            if cache_key in self.memory_cache:
                data = self.memory_cache[cache_key]
                return {
                    'price': data.get('Fiyat', 0),
                    'return': 0.0,  # Günlük getiri hesaplanmıyor artık
                    'total_value': data.get('Fon Toplam Değer', 0),
                    'unit_count': data.get('Tedavüldeki Pay Sayısı', 0),
                    'fund_name': data.get('Fon Adı', ''),
                    'date': target_date.strftime('%Y-%m-%d')
                }
            
            # Cache'de yoksa Azure Blob Storage'dan oku
            content = self.blob_storage.download_file(self.data_file)
            if content:
                parquet_buffer = io.BytesIO(content)
                # Tüm veriyi oku
                df = pd.read_parquet(parquet_buffer)
                
                # Exact match dene - hem tarih hem fon kodu tam eşleşmeli
                exact_match = df[
                    (df['Tarih'].dt.date == target_date.date()) & 
                    (df['Fon Kodu'] == fund_code)
                ]
                
                if not exact_match.empty:
                    row = exact_match.iloc[0]
                    return {
                        'price': float(row['Fiyat']),
                        'return': 0.0,  # Günlük getiri bu dosyada yok
                        'total_value': float(row['Fon Toplam Değer']),
                        'unit_count': float(row['Tedavüldeki Pay Sayısı']),
                        'fund_name': row['Fon Adı'],
                        'date': target_date.strftime('%Y-%m-%d')
                    }
                
                # Eğer exact match yoksa SADECE aynı tarihte benzer fon kodlarını ara
                # GEÇMİŞ VERİ ALMASINI ÖNLEMEK İÇİN SADECE TARGET_DATE'te ara
                same_date_funds = df[df['Tarih'].dt.date == target_date.date()]
                
                if not same_date_funds.empty:
                    # Case insensitive partial match (SADECE aynı tarihte)
                    partial_match = same_date_funds[
                        same_date_funds['Fon Kodu'].str.contains(fund_code, case=False, na=False)
                    ]
                    
                    if not partial_match.empty:
                        # En yakın eşleşmeyi al (aynı tarihte)
                        best_match = partial_match.iloc[0]
                        return {
                            'price': float(best_match['Fiyat']),
                            'return': 0.0,  # Günlük getiri bu dosyada yok
                            'total_value': float(best_match['Fon Toplam Değer']),
                            'unit_count': float(best_match['Tedavüldeki Pay Sayısı']),
                            'fund_name': best_match['Fon Adı'],
                            'date': target_date.strftime('%Y-%m-%d')
                        }
            
            # Belirtilen tarih için veri bulunamadı
            return None
            
        except Exception as e:
            return None
    
    def get_latest_fund_price(self, fund_code: str) -> Optional[Dict]:
        """En son fon fiyatını al - Azure Blob Storage ile HIZLI"""
        try:
            content = self.blob_storage.download_file(self.data_file)
            if content:
                parquet_buffer = io.BytesIO(content)
                # Sadece belirli fon için oku ve tarihe göre sırala
                df = pd.read_parquet(parquet_buffer)
                df = df[df['Fon Kodu'] == fund_code]
                
                if not df.empty:
                    # En son tarihi al
                    latest_row = df.sort_values('Tarih', ascending=False).iloc[0]
                    return {
                        'price': float(latest_row['Fiyat']),
                        'return': 0.0,  # Günlük getiri bu dosyada yok
                        'total_value': float(latest_row['Fon Toplam Değer']),
                        'unit_count': float(latest_row['Tedavüldeki Pay Sayısı']),
                        'fund_name': latest_row['Fon Adı'],
                        'date': latest_row['Tarih']
                    }
            
            return None
            
        except Exception as e:
            return None
    
    def get_available_funds(self) -> List[str]:
        """Azure Blob Storage'dan mevcut olan tüm fon kodlarını al - HIZLI"""
        try:
            fund_codes = []
            
            # Memory cache'den al
            for cache_key in self.memory_cache.keys():
                if '_' in cache_key:
                    fund_code = cache_key.split('_', 1)[1]  # tarih_fonkodu formatından fon kodunu al
                    if fund_code not in fund_codes:
                        fund_codes.append(fund_code)
            
            # Azure Blob Storage'dan da al
            try:
                content = self.blob_storage.download_file(self.data_file)
                if content:
                    parquet_buffer = io.BytesIO(content)
                    # Sadece 'Fon Kodu' sütununu oku (boşluklu sütun adı)
                    df = pd.read_parquet(parquet_buffer, columns=['Fon Kodu'])
                    parquet_codes = df['Fon Kodu'].unique().tolist()
                    
                    # Birleştir ve tekrarları kaldır
                    fund_codes.extend([code for code in parquet_codes if code not in fund_codes])
                    
            except Exception as e:
                pass  # Hata durumunda mevcut liste ile devam et
                
            return sorted(fund_codes)  # Alfabetik sıralama
            
        except Exception as e:
            return []
    
    def get_fund_count(self) -> int:
        """Azure Blob Storage'da toplam kaç fon olduğunu döndür"""
        return len(self.get_available_funds())
    
    def clear_memory_cache(self):
        """Memory cache'i temizle ve Azure'dan sil"""
        self.memory_cache = {}
        try:
            # Azure'dan cache dosyasını sil (varsa)
            self.blob_storage.delete_file(self.cache_file)
        except Exception as e:
            pass  # Azure'dan silinemedi, önemli değil
            
        # Lokal cache dosyasını da sil (varsa)
        # Lokal dosya kullanımı tamamen kaldırıldı; sadece blob'dan sil
        try:
            # Eğer local dos varsa, ignore it (we do not touch local FS)
            pass
        except Exception:
            pass

# Global TefasDataManager instance
tefas_dm = TefasDataManager()

# ================ TURKISH GOLD DATA YÖNETİMİ ================

TURKISH_GOLD_DATA_FILE = "turkish_gold_data.parquet"

# Turkish Gold Parquet Schema
TURKISH_GOLD_SCHEMA = pa.schema([
    pa.field('Tarih', pa.date32()),
    pa.field('Instrument_Code', pa.string()),
    pa.field('Instrument_Name', pa.string()),
    pa.field('Price', pa.float64()),
    pa.field('Buy_Price', pa.float64()),
    pa.field('Sell_Price', pa.float64()),
    pa.field('Currency', pa.string()),
    pa.field('Source', pa.string()),
    pa.field('Update_Time', pa.timestamp('ms'))
])

def calculate_turkish_gold_prices(target_date, is_today=False):
    """
    Belirtilen tarih için Türk altın fiyatlarını hesapla
    
    Args:
        target_date: Fiyat hesaplanacak tarih (datetime.date)
        is_today: Bugün için mi hesaplanıyor (True ise Truncgill API kullan)
    
    Returns:
        dict: {instrument_code: price} formatında fiyatlar
    """
    from datetime import datetime, timedelta
    import time as time_module
    
    try:
        # Bugün için direkt Truncgill API'den al
        if is_today:
            try:
                api_url = "https://finans.truncgil.com/today.json"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                }
                
                response = requests.get(api_url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    def parse_price(price_str):
                        if isinstance(price_str, (int, float)):
                            return float(price_str)
                        if isinstance(price_str, str):
                            try:
                                return float(price_str.replace(',', '').replace('.', '').replace(' ', '')) / 100
                            except:
                                return 0.0
                        return 0.0
                    
                    gold_prices = {}
                    
                    # API'den gelen verileri map et
                    api_mapping = {
                        'gram-altin': 'ALTIN_GRAM',
                        'ceyrek-altin': 'ALTIN_CEYREK',
                        'yarim-altin': 'ALTIN_YARIM',
                        'tam-altin': 'ALTIN_TAM',
                        'resat-altini': 'ALTIN_RESAT',
                        'cumhuriyet-altini': 'ALTIN_CUMHURIYET',
                        'ata-altin': 'ALTIN_ATA',
                        'hamit-altin': 'ALTIN_HAMIT',
                        'ikibucuk-altin': 'ALTIN_IKIBUCUK',
                        'besli-altin': 'ALTIN_BESLI',
                        '14-ayar-altin': 'ALTIN_14AYAR',
                        '18-ayar-altin': 'ALTIN_18AYAR',
                        '22-ayar-bilezik': 'ALTIN_22AYAR_BILEZIK'
                    }
                    
                    for api_key, our_key in api_mapping.items():
                        if api_key in data:
                            gold_data = data[api_key]
                            alış = parse_price(gold_data.get('Alış', gold_data.get('alis', '0')))
                            satış = parse_price(gold_data.get('Satış', gold_data.get('satis', '0')))
                            current_price = alış if alış > 0 else satış
                            
                            if current_price > 0:
                                gold_prices[our_key] = current_price
                    
                    # Ons fiyatını hesapla
                    if 'ALTIN_GRAM' in gold_prices:
                        gold_prices['ALTIN_ONS_TRY'] = gold_prices['ALTIN_GRAM'] * 31.1035
                    
                    return gold_prices
                    
            except Exception as e:
                st.warning(f"⚠️ Truncgill API hatası: {str(e)}, Yahoo Finance'a geçiliyor...")
        
        # Geçmiş tarihler için Yahoo Finance'tan hesapla
        # 1. Ounce altın fiyatını al (GC=F)
        try:
            gold_ticker = yf.Ticker("GC=F", session=YF_SESSION)
            
            # Tarihi datetime'a çevir
            start_dt = datetime.combine(target_date, datetime.min.time())
            end_dt = start_dt + timedelta(days=1)
            
            gold_hist = gold_ticker.history(start=start_dt, end=end_dt)
            
            if gold_hist.empty:
                # O gün veri yoksa önceki 5 günü dene
                for i in range(1, 6):
                    prev_date = target_date - timedelta(days=i)
                    prev_start = datetime.combine(prev_date, datetime.min.time())
                    prev_end = prev_start + timedelta(days=1)
                    gold_hist = gold_ticker.history(start=prev_start, end=prev_end)
                    if not gold_hist.empty:
                        break
            
            if gold_hist.empty:
                return {}
            
            # Ounce fiyatı (USD)
            ounce_price_usd = float(gold_hist['Close'].iloc[0])
            
        except Exception as e:
            st.warning(f"⚠️ Yahoo Finance altın fiyatı alınamadı: {str(e)}")
            return {}
        
        time_module.sleep(0.3)  # Rate limiting
        
        # 2. USD/TRY kurunu al
        try:
            usdtry_ticker = yf.Ticker("USDTRY=X", session=YF_SESSION)
            usdtry_hist = usdtry_ticker.history(start=start_dt, end=end_dt)
            
            if usdtry_hist.empty:
                # O gün veri yoksa önceki 5 günü dene
                for i in range(1, 6):
                    prev_date = target_date - timedelta(days=i)
                    prev_start = datetime.combine(prev_date, datetime.min.time())
                    prev_end = prev_start + timedelta(days=1)
                    usdtry_hist = usdtry_ticker.history(start=prev_start, end=prev_end)
                    if not usdtry_hist.empty:
                        break
            
            if usdtry_hist.empty:
                return {}
            
            # USD/TRY kuru
            usdtry_rate = float(usdtry_hist['Close'].iloc[0])
            
        except Exception as e:
            st.warning(f"⚠️ Yahoo Finance USD/TRY kuru alınamadı: {str(e)}")
            return {}
        
        # 3. TL cinsinden fiyatları hesapla
        # Önce gram altın fiyatını hesapla (1 ons = 31.1035 gram)
        gram_price_try = (ounce_price_usd / 31.1035) * usdtry_rate
        
        gold_prices = {}
        
        # Tüm Türk altın enstrümanları için fiyat hesapla
        for instrument_code in TURKISH_GOLD_INSTRUMENTS:
            if instrument_code in TURKISH_GOLD_CONVERSIONS:
                conversion_factor = TURKISH_GOLD_CONVERSIONS[instrument_code]
                gold_prices[instrument_code] = gram_price_try * conversion_factor
        
        return gold_prices
        
    except Exception as e:
        st.error(f"❌ Türk altın fiyatları hesaplama hatası: {str(e)}")
        return {}

class TurkishGoldDataManager:
    """Turkish gold fiyatlarını Azure Blob Storage'da Parquet formatında günlük tarih bazlı yönetmek için sınıf"""
    
    def __init__(self):
        self.data_file = TURKISH_GOLD_DATA_FILE
        self.blob_storage = AzureBlobStorage()
        self.cache = {}
        self.last_update = None
        self._load_cache()
    
    def _load_cache(self):
        """Cache'i Azure Blob Storage'dan yükle - bugünkü verileri al"""
        try:
            content = self.blob_storage.download_file(self.data_file, silent=True)
            if content:
                parquet_buffer = io.BytesIO(content)
                df = pd.read_parquet(parquet_buffer)
                
                # Bugünkü verileri cache'e al
                today = datetime.now().date()
                today_data = df[df['Tarih'] == today]
                
                self.cache = {}
                for _, row in today_data.iterrows():
                    self.cache[row['Instrument_Code']] = {
                        'price': row['Price'],
                        'name': row['Instrument_Name'],
                        'buy_price': row['Buy_Price'],
                        'sell_price': row['Sell_Price'],
                        'last_update': row['Update_Time'].isoformat() if pd.notna(row['Update_Time']) else None
                    }
                
                if len(self.cache) > 0:
                    self.last_update = today_data['Update_Time'].max().isoformat() if not today_data.empty else None
                
                    
        except Exception as e:
            self.cache = {}
            self.last_update = None
    
    def save_daily_prices(self, prices_data):
        """Turkish gold fiyatlarını günlük Parquet formatında Azure Blob Storage'a kaydet"""
        try:
            
            today = datetime.now().date()
            update_time = datetime.now()
            
            if not prices_data:
                return False
            
            # Yeni veri satırlarını oluştur
            new_rows = []
            for instrument_code, data in prices_data.items():
                try:
                    new_rows.append({
                        'Tarih': today,
                        'Instrument_Code': instrument_code,
                        'Instrument_Name': data.get('name', instrument_code),
                        'Price': float(data.get('price', 0)),
                        'Buy_Price': float(data.get('buy', 0)),
                        'Sell_Price': float(data.get('sell', 0)),
                        'Currency': data.get('currency', '₺'),
                        'Source': data.get('source', 'finans.truncgil.com'),
                        'Update_Time': update_time.strftime('%Y-%m-%d %H:%M:%S')  # String format kullan
                    })
                except Exception as e:
                    continue
            
            if not new_rows:
                return False
            
            new_df = pd.DataFrame(new_rows)
            
            # Mevcut Parquet dosyasını oku
            existing_df = pd.DataFrame()
            try:
                content = self.blob_storage.download_file(self.data_file, silent=True)
                if content:
                    parquet_buffer = io.BytesIO(content)
                    existing_df = pd.read_parquet(parquet_buffer)
                    
                    # Bugünkü verileri sil (güncelleme için)
                    existing_df = existing_df[existing_df['Tarih'] != today]
                else:
                    existing_df = pd.DataFrame()  # Boş DataFrame oluştur
                    
            except Exception as e:
                existing_df = pd.DataFrame()  # Hata durumunda boş DataFrame
            
            # Yeni veriyi mevcut veriye ekle
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Tarihe göre sırala
            final_df = final_df.sort_values(['Tarih', 'Instrument_Code'])
            
            # Parquet formatında kaydet
            try:
                parquet_buffer = io.BytesIO()
                # Schema kontrolünü kaldır, sadece basic parquet oluştur
                final_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                
                success = self.blob_storage.upload_file(
                    file_content=parquet_buffer.getvalue(),
                    blob_name=self.data_file
                )
                
                
            except Exception as e:
                return False
            
            if success:
                # Cache'i güncelle
                self.cache = {}
                for instrument_code, data in prices_data.items():
                    self.cache[instrument_code] = {
                        'price': data.get('price', 0),
                        'name': data.get('name', instrument_code),
                        'buy_price': data.get('buy', 0),
                        'sell_price': data.get('sell', 0),
                        'last_update': update_time.isoformat()
                    }
                
                self.last_update = update_time.isoformat()
                
                return True
            return False
            
        except Exception as e:
            return False
    
    def get_prices(self, force_refresh=False):
        """Turkish gold fiyatlarını getir (cache'den veya blob'dan)"""
        try:
            # Force refresh veya cache boşsa blob'dan yükle
            if force_refresh or not self.cache:
                self._load_cache()
            
            return self.cache
            
        except Exception as e:
            return {}
    
    def get_historical_data(self, start_date=None, end_date=None):
        """Tarihsel Turkish gold verilerini getir"""
        try:
            content = self.blob_storage.download_file(self.data_file, silent=True)
            if not content:
                return pd.DataFrame()
            
            parquet_buffer = io.BytesIO(content)
            df = pd.read_parquet(parquet_buffer)
            
            # Tarih filtrelemesi
            if start_date:
                df = df[df['Tarih'] >= start_date]
            if end_date:
                df = df[df['Tarih'] <= end_date]
            
            return df
            
        except Exception as e:
            return pd.DataFrame()
    
    def is_data_fresh(self, max_age_hours=24):
        """Verinin güncel olup olmadığını kontrol et"""
        if not self.last_update:
            return False
        
        try:
            last_update_dt = datetime.fromisoformat(self.last_update.replace('Z', '+00:00'))
            age = datetime.now() - last_update_dt.replace(tzinfo=None)
            return age.total_seconds() < (max_age_hours * 3600)
        except:
            return False
    
    def update_prices_from_api(self):
        """API'den fiyatları çek ve günlük Parquet formatında kaydet"""
        try:
            # Direkt API çağrısı (blob storage kontrolü yapmadan)
            api_prices = self._fetch_api_prices_direct()
            
            if api_prices:
                # API formatını Parquet formatına çevir
                parquet_format = {}
                for instrument, data in api_prices.items():
                    parquet_format[instrument] = {
                        'price': data.get('price', 0),
                        'name': data.get('name', instrument),
                        'buy': data.get('buy', 0),
                        'sell': data.get('sell', 0),
                        'currency': data.get('currency', '₺'),
                        'source': data.get('source', 'finans.truncgil.com')
                    }
                
                # Günlük Parquet formatında kaydet
                success = self.save_daily_prices(parquet_format)
                
                if success:
                    return True
                
            return False
            
        except Exception as e:
            return False
    
    def _fetch_api_prices_direct(self):
        """Direkt API'den fiyatları çek (blob storage kontrolü yapmadan)"""
        turkish_gold_data = {}
        
        try:
            api_url = "https://finans.truncgil.com/today.json"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'tr-TR,tr;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://www.google.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site'
            }
            
            response = requests.get(api_url, headers=headers, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                
                
                # Fiyat formatını düzenle
                def parse_price(price_str):
                    if isinstance(price_str, (int, float)):
                        return float(price_str)
                    if isinstance(price_str, str):
                        try:
                            return float(price_str.replace(',', '').replace('.', '').replace(' ', '')) / 100
                        except:
                            return 0.0
                    return 0.0
                
                # Altın verilerini çıkar
                if 'gram-altin' in data:
                    gram_data = data['gram-altin']
                    alış = parse_price(gram_data.get('Alış', gram_data.get('alis', '0')))
                    satış = parse_price(gram_data.get('Satış', gram_data.get('satis', '0')))
                    current_price = alış if alış > 0 else satış
                    
                    if current_price > 0:
                        turkish_gold_data["ALTIN_GRAM"] = {
                            "name": "Gram Altın (TRY)",
                            "price": current_price,
                            "currency": "₺",
                            "buy": alış,
                            "sell": satış,
                            "source": "finans.truncgil.com"
                        }
                
                # Diğer altın türleri için de aynı işlemi yap
                gold_types = {
                    'ceyrek-altin': 'ALTIN_CEYREK',
                    'yarim-altin': 'ALTIN_YARIM', 
                    'tam-altin': 'ALTIN_TAM',
                    'resat-altini': 'ALTIN_RESAT',
                    'cumhuriyet-altini': 'ALTIN_CUMHURIYET',
                    'ata-altin': 'ALTIN_ATA',
                    'hamit-altin': 'ALTIN_HAMIT',
                    'ikibucuk-altin': 'ALTIN_IKIBUCUK',
                    'besli-altin': 'ALTIN_BESLI',
                    '14-ayar-altin': 'ALTIN_14AYAR',
                    '18-ayar-altin': 'ALTIN_18AYAR',
                    '22-ayar-bilezik': 'ALTIN_22AYAR_BILEZIK'
                }
                
                for api_key, our_key in gold_types.items():
                    if api_key in data:
                        gold_data = data[api_key]
                        alış = parse_price(gold_data.get('Alış', gold_data.get('alis', '0')))
                        satış = parse_price(gold_data.get('Satış', gold_data.get('satis', '0')))
                        current_price = alış if alış > 0 else satış
                        
                        if current_price > 0:
                            turkish_gold_data[our_key] = {
                                "name": f"{api_key.replace('-', ' ').title()} (TRY)",
                                "price": current_price,
                                "currency": "₺",
                                "buy": alış,
                                "sell": satış,
                                "source": "finans.truncgil.com"
                            }
                
                # Ons fiyatı hesapla
                if 'ALTIN_GRAM' in turkish_gold_data:
                    gram_price = turkish_gold_data['ALTIN_GRAM']['price']
                    ons_price = gram_price * 31.1035
                    turkish_gold_data["ALTIN_ONS_TRY"] = {
                        "name": "Ons Altın (TRY)",
                        "price": ons_price,
                        "currency": "₺",
                        "buy": ons_price,
                        "sell": ons_price,
                        "source": "finans.truncgil.com"
                    }
                
                
                return turkish_gold_data
                
        except Exception as e:
            return {}
        
        return {}
    
    def get_data_summary(self):
        """Veri özeti raporu"""
        try:
            content = self.blob_storage.download_file(self.data_file, silent=True)
            if not content:
                return None
            
            parquet_buffer = io.BytesIO(content)
            df = pd.read_parquet(parquet_buffer)
            
            if df.empty:
                return None
            
            summary = {
                'total_records': len(df),
                'date_range': {
                    'start': df['Tarih'].min(),
                    'end': df['Tarih'].max()
                },
                'instruments_count': df['Instrument_Code'].nunique(),
                'latest_update': df['Update_Time'].max(),
                'instruments': df['Instrument_Code'].unique().tolist()
            }
            
            return summary
            
        except Exception as e:
            return None

# Global TurkishGoldDataManager instance
turkish_gold_dm = TurkishGoldDataManager()

# ================ BIST HİSSE YÖNETİMİ (PARQUET) ================

def save_bist_stocks_to_parquet(stocks_dict):
    """BIST hisse senetlerini Azure Blob Storage'a Parquet olarak kaydet"""
    try:
        # Veri yapısını düzelt - stocks_dict içindeki her hisse için detay bilgiler var
        records = []
        for symbol, details in stocks_dict.items():
            if isinstance(details, dict):
                # Eğer detay bilgiler varsa
                records.append({
                    "symbol": symbol,
                    "name": details.get('longName', details.get('shortName', symbol)),
                    "shortName": details.get('shortName', ''),
                    "sector": details.get('sector', ''),
                    "currency": details.get('currency', 'TRY'),
                    "marketCap": details.get('marketCap', 0),
                    "exchange": details.get('exchange', 'IST'),
                    "last_updated": datetime.now().isoformat(),
                    "source": "yahoo_finance"
                })
            else:
                # Eğer sadece isim varsa (eski format)
                records.append({
                    "symbol": symbol,
                    "name": str(details),
                    "shortName": '',
                    "sector": '',
                    "currency": 'TRY',
                    "marketCap": 0,
                    "exchange": 'IST',
                    "last_updated": datetime.now().isoformat(),
                    "source": "yahoo_finance"
                })
        
        df = pd.DataFrame(records)
        
        # Parquet bytes'ını oluştur
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_data = parquet_buffer.getvalue()
        
        # Azure Blob Storage'a kaydet
        if blob_storage.blob_service_client:
            try:
                success = blob_storage.upload_file(file_name=BIST_STOCKS_FILE, data=parquet_data, silent=True)
                if success:
                    print(f"✅ {len(stocks_dict)} BIST hissesi Azure Blob Storage'a kaydedildi")
                    return True
                else:
                    print(f"❌ Azure blob kaydetme başarısız")
                    return False
            except Exception as e:
                print(f"❌ Azure blob parquet kaydetme hatası: {str(e)}")
                # Hata detayını kullanıcıya gösterme, sadece log'la
                return False
        else:
            print("❌ Azure Blob Storage bağlantısı yok")
            return False
            
    except Exception as e:
        st.error(f"❌ BIST hisselerini kaydetme hatası: {str(e)}")
        return False

@st.cache_data(ttl=300)  # 5 dakika cache
def load_bist_stocks_from_parquet():
    """BIST hisse senetlerini Azure Blob Storage'dan Parquet olarak oku"""
    try:        
        # Azure Blob Storage bağlantısını kontrol et
        if not blob_storage.blob_service_client:
            return {}, None        
        
        # Dosyanın varlığını kontrol et
        file_exists = blob_storage.file_exists(BIST_STOCKS_FILE)        
        if not file_exists:
            return {}, None
        
        # Dosyayı indir
        blob_data = blob_storage.download_file(BIST_STOCKS_FILE)
        
        if not blob_data:
            return {}, None        
        
        try:
            # Bytes'ı pandas ile direkt oku
            parquet_buffer = io.BytesIO(blob_data)
            df = pd.read_parquet(parquet_buffer, engine='pyarrow')
            
            if not df.empty:
                # Basit format: sadece symbol -> name mapping
                stocks_dict = {}
                for _, row in df.iterrows():
                    symbol = row['symbol']
                    # Sadece uzun adı al
                    long_name = row.get('name', symbol)
                    stocks_dict[symbol] = long_name
                
                last_updated = df['last_updated'].iloc[0] if 'last_updated' in df.columns else None
                return stocks_dict, last_updated
            else:
                return {}, None
                
        except Exception as e:
            print(f"❌ Parquet okuma hatası: {type(e).__name__}: {str(e)}")
            return {}, None
        
    except Exception as e:
        print(f"❌ Genel hata türü: {type(e).__name__}: {str(e)}")
        return {}, None

def is_bist_data_stale(last_updated, hours=24):
    """BIST verisinin eski olup olmadığını kontrol et"""
    if last_updated is None:
        return True
    
    try:
        if isinstance(last_updated, str):
            last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
        
        time_diff = datetime.now() - last_updated.replace(tzinfo=None)
        return time_diff.total_seconds() > (hours * 3600)
    except:
        return True

def backup_bist_file():
    """BIST dosyasını backup uzantısıyla yedekle"""
    try:
        blob_storage = AzureBlobStorage()
        
        # Azure'dan mevcut dosyayı oku
        if blob_storage and blob_storage.file_exists(BIST_STOCKS_FILE):
            content = blob_storage.download_file(BIST_STOCKS_FILE)
            if content:
                backup_filename = BIST_STOCKS_FILE.replace('.parquet', '_backup.parquet')
                success = blob_storage.upload_file(backup_filename, content)
                if success:
                    return True
        
        # Lokal dosya varsa onu da yedekle (lokal işlemler kaldırıldı)
    except Exception as e:
        print(f"Backup hatası: {str(e)}")
        return False

def run_scheduled_bist_update():
    """Zamanlanmış BIST güncelleme işlemi"""
    try:
        # Önce backup al
        backup_success = backup_bist_file()
        
        # BIST listesini güncelle
        stocks_dict = fetch_all_bist_stocks()
        
        if len(stocks_dict) > 10:
            success = save_bist_stocks_to_parquet(stocks_dict)
            
            # Log dosyasına kaydet
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'action': 'scheduled_update',
                'stocks_count': len(stocks_dict),
                'backup_created': backup_success,
                'success': success
            }
            
            # Persist log to blob (best-effort) and enqueue a human-readable message for UI
            log_file = 'bist_update_log.json'
            try:
                logs = read_logs_from_blob(log_file)
                logs.append(log_entry)
                if len(logs) > 100:
                    logs = logs[-100:]
                write_logs_to_blob(log_file, logs)
            except Exception:
                pass

            # Thread-safe UI log (main thread will flush)
            msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] BIST scheduled update: stocks={len(stocks_dict)}, backup={backup_success}, success={success}"
            enqueue_job_log('bist_update_logs', msg)
            return success
        
        return False
    except Exception as e:
        error_log = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ BIST scheduled update error: {str(e)}"
        # Queue the error for the main thread UI
        try:
            enqueue_job_log('bist_update_logs', error_log)
        except Exception:
            pass

        # Persist to blob (best-effort)
        try:
            blob_logs = read_logs_from_blob('bist_update_log.json')
            blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': error_log})
            if len(blob_logs) > 200:
                blob_logs = blob_logs[-200:]
            write_logs_to_blob('bist_update_log.json', blob_logs)
        except Exception:
            pass

        return False

def init_bist_scheduler():
    """BIST periyodik güncelleme scheduler'ını başlat"""
    if 'bist_scheduler_initialized' not in st.session_state:
        st.session_state['bist_scheduler_initialized'] = True
        
        # Scheduler'ı arka planda çalıştır
        def scheduler_thread():
            while True:
                schedule.run_pending()
                time.sleep(60)  # Her dakika kontrol et
        
        # Arka plan thread'i başlat
        scheduler_thread_obj = threading.Thread(target=scheduler_thread, daemon=True)
        scheduler_thread_obj.start()

def setup_bist_periodic_update(period, update_time):
    """BIST periyodik güncelleme ayarla"""
    try:
        # Mevcut BIST schedule'larını temizle (diğer işlere dokunma)
        schedule.clear('bist')

        p = str(period).strip().lower()
        if hasattr(update_time, 'strftime'):
            time_str = update_time.strftime('%H:%M')
        else:
            time_str = str(update_time)

        # Yeni schedule ekle
        if p.startswith('gün') or p.startswith('gun'):
            schedule.every().day.at(time_str).do(run_scheduled_bist_update).tag('bist')
        elif p.startswith('haft'):
            schedule.every().monday.at(time_str).do(run_scheduled_bist_update).tag('bist')
        elif p.startswith('ay'):
            schedule.every(30).days.at(time_str).do(run_scheduled_bist_update).tag('bist')

        # Store next_run info so UI can show it
        try:
            jobs = schedule.get_jobs('bist')
            if jobs:
                job = jobs[-1]
                st.session_state['bist_next_run'] = getattr(job, 'next_run').isoformat() if getattr(job, 'next_run', None) else None
        except Exception:
            pass

        # Store next_run info so UI can show it
        try:
            jobs = schedule.get_jobs('bist')
            if jobs:
                job = jobs[-1]
                st.session_state['bist_next_run'] = getattr(job, 'next_run').isoformat() if getattr(job, 'next_run', None) else None
        except Exception:
            pass
        
        # Session state'e kaydet
        st.session_state['bist_schedule'] = {
            'period': period,
            'time': time_str,
            'active': True,
            'setup_date': datetime.now().isoformat()
        }
        
        return True
    except Exception as e:
        print(f"Schedule setup error: {str(e)}")
        return False

def backup_nasdaq_file():
    """NASDAQ dosyasını backup uzantısıyla yedekle"""
    try:
        blob_storage = AzureBlobStorage()
        
        # Azure'dan mevcut dosyayı oku
        if blob_storage and blob_storage.file_exists(NASDAQ_STOCKS_FILE):
            content = blob_storage.download_file(NASDAQ_STOCKS_FILE)
            if content:
                backup_filename = NASDAQ_STOCKS_FILE.replace('.parquet', '_backup.parquet')
                success = blob_storage.upload_file(backup_filename, content)
                if success:
                    return True
        # Lokal yedekleme kaldırıldı - yalnızca blob kullanılır
        return False
    except Exception as e:
        print(f"NASDAQ Backup hatası: {str(e)}")
        return False

def run_scheduled_nasdaq_update():
    """Zamanlanmış NASDAQ güncelleme işlemi"""
    try:
        # Önce backup al
        backup_success = backup_nasdaq_file()
        
        # NASDAQ listesini güncelle
        success = fetch_and_save_nasdaq_stocks()
        
        if success and len(success) > 10:
            # Log ekle
            log_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            if backup_success:
                log_message += "✅ Backup alındı, "
            else:
                log_message += "⚠️ Backup alınamadı, "

            log_message += f"NASDAQ güncellendi ({len(success)} hisse)"

            # Queue the log for the main thread to flush
            enqueue_job_log('nasdaq_update_logs', log_message)

            # Persist to blob (best-effort)
            try:
                blob_logs = read_logs_from_blob('nasdaq_update_log.json')
                blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': log_message})
                if len(blob_logs) > 200:
                    blob_logs = blob_logs[-200:]
                write_logs_to_blob('nasdaq_update_log.json', blob_logs)
            except Exception:
                pass

            return True
        else:
            return False
            
    except Exception as e:
        error_log = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ NASDAQ güncelleme hatası: {str(e)}"
        enqueue_job_log('nasdaq_update_logs', error_log)
        return False

def init_nasdaq_scheduler():
    """NASDAQ scheduler'ını başlat"""
    try:
        # Session state'ten ayarları oku
        if st.session_state.get('nasdaq_scheduler_active', False):
            period = st.session_state.get('nasdaq_update_period', 'günlük')
            update_time = st.session_state.get('nasdaq_update_time', datetime_time(9, 0))
            
            # Schedule ayarla
            setup_nasdaq_periodic_update(period, update_time)
            
        # Scheduler thread'i başlat (eğer başlamamışsa)
        def nasdaq_scheduler_thread():
            while True:
                schedule.run_pending()
                time.sleep(60)  # Her dakika kontrol et
        
        # Arka plan thread'i başlat
        nasdaq_scheduler_thread_obj = threading.Thread(target=nasdaq_scheduler_thread, daemon=True)
        nasdaq_scheduler_thread_obj.start()
        
    except Exception as e:
        print(f"NASDAQ Scheduler init hatası: {str(e)}")

def setup_nasdaq_periodic_update(period, update_time):
    """NASDAQ periyodik güncelleme ayarla"""
    try:
        # NASDAQ için ayrı bir schedule namespace kullan
        nasdaq_schedule = schedule
        
        # NASDAQ schedule'ları temizle (sadece nasdaq işleri)
        nasdaq_schedule.clear('nasdaq')
        
        # Normalize inputs and ensure time string
        p = str(period).strip().lower()
        if hasattr(update_time, 'strftime'):
            time_str = update_time.strftime('%H:%M')
        else:
            time_str = str(update_time)

        # Yeni schedule ekle
        if p.startswith('gün') or p.startswith('gun') or p == 'günlük' or p == 'günluk' or p == 'gün':
            nasdaq_schedule.every().day.at(time_str).do(run_scheduled_nasdaq_update).tag('nasdaq')
        elif p.startswith('haft') or p == 'haftalık' or p == 'haftalik':
            nasdaq_schedule.every().monday.at(time_str).do(run_scheduled_nasdaq_update).tag('nasdaq')
        elif p.startswith('ay') or p == 'aylık' or p == 'aylik':
            # schedule.every().month may not be supported; run every 30 days
            nasdaq_schedule.every(30).days.at(time_str).do(run_scheduled_nasdaq_update).tag('nasdaq')
        # Store next_run info so UI can show it
        try:
            jobs = schedule.get_jobs('nasdaq')
            if jobs:
                job = jobs[-1]
                st.session_state['nasdaq_next_run'] = getattr(job, 'next_run').isoformat() if getattr(job, 'next_run', None) else None
        except Exception:
            pass
        
        # Session state'e kaydet
        st.session_state['nasdaq_schedule'] = {
            'period': period,
            'time': time_str,
            'active': True,
            'setup_date': datetime.now().isoformat()
        }
        
        return True
    except Exception as e:
        print(f"NASDAQ Schedule setup error: {str(e)}")
        return False

def backup_tefas_file():
    """TEFAS dosyasını backup uzantısıyla yedekle"""
    try:
        blob_storage = AzureBlobStorage()
        
        # Azure'dan mevcut dosyayı oku
        if blob_storage and blob_storage.file_exists(TEFAS_DATA_FILE):
            content = blob_storage.download_file(TEFAS_DATA_FILE)
            if content:
                backup_filename = TEFAS_DATA_FILE.replace('.xlsx', '_backup.xlsx')
                success = blob_storage.upload_file(backup_filename, content)
                if success:
                    return True
        # Lokal yedekleme kaldırıldı - yalnızca blob kullanılır
        return False
    except Exception as e:
        print(f"TEFAS Backup hatası: {str(e)}")
        return False

def run_scheduled_tefas_update():
    """Zamanlanmış TEFAS güncelleme işlemi"""
    try:
        # Önce backup al
        backup_success = backup_tefas_file()
        
        # Bugünün tarihini kullan
        today = datetime.now().date()
        
        # TEFAS verilerini güncelle (bugün için)
        success = update_tefas_data_to_parquet(today, today, selected_funds=None)
        
        # Log ekle
        log_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        if backup_success:
            log_message += "✅ Backup alındı, "
        else:
            log_message += "⚠️ Backup alınamadı, "
            
        if success:
            log_message += f"TEFAS güncellendi ({today})"
        else:
            log_message += "TEFAS güncellenirken hata oluştu"
            
        # Thread-safe UI log (main thread will flush)
        enqueue_job_log('tefas_update_logs', log_message)

        # Also persist to blob (best-effort)
        try:
            blob_logs = read_logs_from_blob('tefas_update_log.json')
            blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': log_message})
            if len(blob_logs) > 200:
                blob_logs = blob_logs[-200:]
            write_logs_to_blob('tefas_update_log.json', blob_logs)
        except Exception:
            pass
            
        return success
            
    except Exception as e:
        error_log = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ TEFAS güncelleme hatası: {str(e)}"
        try:
            enqueue_job_log('tefas_update_logs', error_log)
        except Exception:
            pass

        # Persist to blob (best-effort)
        try:
            blob_logs = read_logs_from_blob('tefas_update_log.json')
            blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': error_log})
            if len(blob_logs) > 200:
                blob_logs = blob_logs[-200:]
            write_logs_to_blob('tefas_update_log.json', blob_logs)
        except Exception:
            pass

        return False

def init_tefas_scheduler():
    """TEFAS scheduler'ını başlat"""
    try:
        # Session state'ten ayarları oku ve schedule'ı uygula (sadece aktifse)
        if st.session_state.get('tefas_scheduler_active', False):
            period = st.session_state.get('tefas_update_period', 'günlük')
            update_time = st.session_state.get('tefas_update_time', datetime_time(9, 0))
            # Schedule ayarla
            setup_tefas_periodic_update(period, update_time)

        # Scheduler thread'i yalnızca bir kez başlat (tekrar başlatmayı önle)
        if 'tefas_scheduler_initialized' not in st.session_state:
            st.session_state['tefas_scheduler_initialized'] = True

            def tefas_scheduler_thread():
                while True:
                    try:
                        schedule.run_pending()
                    except Exception:
                        # Protect thread from unexpected schedule errors
                        pass
                    time.sleep(60)  # Her dakika kontrol et

            # Arka plan thread'i başlat
            tefas_scheduler_thread_obj = threading.Thread(target=tefas_scheduler_thread, daemon=True)
            tefas_scheduler_thread_obj.start()
        
    except Exception as e:
        print(f"TEFAS Scheduler init hatası: {str(e)}")

def setup_tefas_periodic_update(period, update_time):
    """TEFAS periyodik güncelleme ayarla"""
    try:
        # TEFAS için ayrı bir schedule namespace kullan
        tefas_schedule = schedule
        
        # TEFAS schedule'ları temizle (sadece tefas işleri)
        tefas_schedule.clear('tefas')
        
        p = str(period).strip().lower()
        if hasattr(update_time, 'strftime'):
            time_str = update_time.strftime('%H:%M')
        else:
            time_str = str(update_time)

        # Yeni schedule ekle
        if p.startswith('gün') or p.startswith('gun'):
            tefas_schedule.every().day.at(time_str).do(run_scheduled_tefas_update).tag('tefas')
        elif p.startswith('haft'):
            tefas_schedule.every().monday.at(time_str).do(run_scheduled_tefas_update).tag('tefas')
        elif p.startswith('ay'):
            tefas_schedule.every(30).days.at(time_str).do(run_scheduled_tefas_update).tag('tefas')
        
        # Safety: avoid immediate execution if scheduled time equals current minute
        try:
            grace = timedelta(seconds=30)
            now = datetime.now()
            jobs = schedule.get_jobs('tefas')
            if jobs:
                job = jobs[-1]
                if getattr(job, 'next_run', None) is not None:
                    if job.next_run <= now + grace:
                        if p.startswith('gün') or p.startswith('gun'):
                            job.next_run = job.next_run + timedelta(days=1)
                        elif p.startswith('haft'):
                            job.next_run = job.next_run + timedelta(days=7)
                        elif p.startswith('ay'):
                            job.next_run = job.next_run + timedelta(days=30)
                # store next_run info to session so UI can show it
                try:
                    st.session_state['tefas_next_run'] = getattr(job, 'next_run').isoformat() if getattr(job, 'next_run', None) else None
                except Exception:
                    st.session_state['tefas_next_run'] = str(getattr(job, 'next_run', None))
        except Exception:
            pass
        
        # Session state'e kaydet
        st.session_state['tefas_schedule'] = {
            'period': period,
            'time': time_str,
            'active': True,
            'setup_date': datetime.now().isoformat()
        }
        
        return True
    except Exception as e:
        print(f"TEFAS Schedule setup error: {str(e)}")
        return False

def backup_turkish_gold_file():
    """Turkish Gold dosyasını backup uzantısıyla yedekle"""
    try:
        blob_storage = AzureBlobStorage()
        
        # Azure'dan mevcut dosyayı oku
        if blob_storage and blob_storage.file_exists('turkish_gold_data.parquet'):
            content = blob_storage.download_file('turkish_gold_data.parquet')
            if content:
                backup_filename = 'turkish_gold_data_backup.parquet'
                success = blob_storage.upload_file(backup_filename, content)
                if success:
                    return True
        # Lokal yedekleme kaldırıldı - yalnızca blob kullanılır
        return False
    except Exception as e:
        print(f"Turkish Gold Backup hatası: {str(e)}")
        return False

def run_scheduled_turkish_gold_update():
    """Zamanlanmış Turkish Gold güncelleme işlemi"""
    try:
        # Önce backup al
        backup_success = backup_turkish_gold_file()
        
        # Turkish Gold fiyatlarını güncelle
        success = turkish_gold_dm.update_prices_from_api()
        
        # Log ekle
        log_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        if backup_success:
            log_message += "✅ Backup alındı, "
        else:
            log_message += "⚠️ Backup alınamadı, "
            
        if success:
            log_message += "Turkish Gold fiyatları güncellendi"
        else:
            log_message += "Turkish Gold güncellenirken hata oluştu"
            
        # Thread-safe UI log (main thread will flush)
        enqueue_job_log('turkish_gold_update_logs', log_message)

        # Persist to blob (best-effort)
        try:
            blob_logs = read_logs_from_blob('turkish_gold_update_log.json')
            blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': log_message})
            if len(blob_logs) > 200:
                blob_logs = blob_logs[-200:]
            write_logs_to_blob('turkish_gold_update_log.json', blob_logs)
        except Exception:
            pass
            
        return success
            
    except Exception as e:
        error_log = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Turkish Gold güncelleme hatası: {str(e)}"
        try:
            enqueue_job_log('turkish_gold_update_logs', error_log)
        except Exception:
            pass

        # Persist to blob (best-effort)
        try:
            blob_logs = read_logs_from_blob('turkish_gold_update_log.json')
            blob_logs.append({'timestamp': datetime.now().isoformat(), 'message': error_log})
            if len(blob_logs) > 200:
                blob_logs = blob_logs[-200:]
            write_logs_to_blob('turkish_gold_update_log.json', blob_logs)
        except Exception:
            pass

        return False

def init_turkish_gold_scheduler():
    """Turkish Gold scheduler'ını başlat"""
    try:
        # Session state'ten ayarları oku ve uygulama (sadece aktifse)
        if st.session_state.get('turkish_gold_scheduler_active', False):
            period = st.session_state.get('turkish_gold_update_period', 'günlük')
            update_time = st.session_state.get('turkish_gold_update_time', datetime_time(9, 0))
            setup_turkish_gold_periodic_update(period, update_time)

        # Scheduler thread'i yalnızca bir kez başlat (tekrar başlatma önlemi)
        if 'turkish_gold_scheduler_initialized' not in st.session_state:
            st.session_state['turkish_gold_scheduler_initialized'] = True

            def turkish_gold_scheduler_thread():
                while True:
                    try:
                        schedule.run_pending()
                    except Exception:
                        pass
                    time.sleep(60)  # Her dakika kontrol et

            turkish_gold_scheduler_thread_obj = threading.Thread(target=turkish_gold_scheduler_thread, daemon=True)
            turkish_gold_scheduler_thread_obj.start()
        
    except Exception as e:
        print(f"Turkish Gold Scheduler init hatası: {str(e)}")

def setup_turkish_gold_periodic_update(period, update_time):
    """Turkish Gold periyodik güncelleme ayarla"""
    try:
        # Turkish Gold için ayrı bir schedule namespace kullan
        turkish_gold_schedule = schedule
        
        # Turkish Gold schedule'ları temizle (sadece turkish_gold işleri)
        turkish_gold_schedule.clear('turkish_gold')
        
        p = str(period).strip().lower()
        if hasattr(update_time, 'strftime'):
            time_str = update_time.strftime('%H:%M')
        else:
            time_str = str(update_time)

        # Yeni schedule ekle
        if p.startswith('gün') or p.startswith('gun'):
            turkish_gold_schedule.every().day.at(time_str).do(run_scheduled_turkish_gold_update).tag('turkish_gold')
        elif p.startswith('haft'):
            turkish_gold_schedule.every().monday.at(time_str).do(run_scheduled_turkish_gold_update).tag('turkish_gold')
        elif p.startswith('ay'):
            turkish_gold_schedule.every(30).days.at(time_str).do(run_scheduled_turkish_gold_update).tag('turkish_gold')

        # Safety: avoid immediate execution when scheduled time equals current minute
        try:
            grace = timedelta(seconds=30)
            now = datetime.now()
            jobs = schedule.get_jobs('turkish_gold')
            if jobs:
                job = jobs[-1]
                if getattr(job, 'next_run', None) is not None:
                    if job.next_run <= now + grace:
                        if p.startswith('gün') or p.startswith('gun'):
                            job.next_run = job.next_run + timedelta(days=1)
                        elif p.startswith('haft'):
                            job.next_run = job.next_run + timedelta(days=7)
                        elif p.startswith('ay'):
                            job.next_run = job.next_run + timedelta(days=30)
                try:
                    st.session_state['turkish_gold_next_run'] = getattr(job, 'next_run').isoformat() if getattr(job, 'next_run', None) else None
                except Exception:
                    st.session_state['turkish_gold_next_run'] = str(getattr(job, 'next_run', None))
        except Exception:
            pass

        # Safety: if the scheduled job would run immediately because the user set the time
        # to the current minute, push the first run to the next period to avoid accidental
        # immediate execution when the user clicks "Periyodik Güncellemeyi Ayarla".
        try:
            # small grace window: 30 seconds
            grace = timedelta(seconds=30)
            now = datetime.now()
            jobs = schedule.get_jobs('turkish_gold')
            if jobs:
                # adjust the most recently added job
                job = jobs[-1]
                # job.next_run may be None in some schedule versions; guard against that
                if getattr(job, 'next_run', None) is not None:
                    if job.next_run <= now + grace:
                        # push to next logical occurrence (add one day for daily/weekly/monthly)
                        # for weekly schedules, add 7 days; for 30-day schedules, add 30 days
                        if p.startswith('gün') or p.startswith('gun'):
                            job.next_run = job.next_run + timedelta(days=1)
                        elif p.startswith('haft'):
                            job.next_run = job.next_run + timedelta(days=7)
                        elif p.startswith('ay'):
                            job.next_run = job.next_run + timedelta(days=30)
        except Exception:
            # Non-critical: if adjustment fails, leave scheduling as-is
            pass
        
        # Session state'e kaydet
        st.session_state['turkish_gold_schedule'] = {
            'period': period,
            'time': time_str,
            'active': True,
            'setup_date': datetime.now().isoformat()
        }
        
        return True
    except Exception as e:
        print(f"Turkish Gold Schedule setup error: {str(e)}")
        return False

def fetch_and_save_bist_stocks():
    """BIST hisselerini Yahoo Finance'den çek ve Parquet'e kaydet"""
    try:
        with st.spinner("🔄 BIST hisse listesi Yahoo Finance'den çekiliyor..."):
            # Mevcut fetch_all_bist_stocks fonksiyonunu kullan
            stocks_dict = fetch_all_bist_stocks()
            
            if len(stocks_dict) > 10:  # En az 10 hisse varsa başarılı sayılır
                success = save_bist_stocks_to_parquet(stocks_dict)
                if success:
                    st.success(f"✅ {len(stocks_dict)} BIST hissesi başarıyla güncellendi ve kaydedildi!")
                    return stocks_dict
                else:
                    st.warning("⚠️ Hisseler alındı ancak kaydetme sırasında sorun yaşandı")
                    return stocks_dict
            else:
                st.warning("⚠️ Yahoo Finance'den yeterli BIST hissesi alınamadı")
                
        return stocks_dict
    except Exception as e:
        st.error(f"❌ BIST hisselerini çekme hatası: Lütfen daha sonra tekrar deneyin")
        print(f"BIST fetch detay hatası: {str(e)}")  # Sadece console'a log
        return {}

def get_bist_stocks_smart():
    """Akıllı BIST hisse yönetimi - Parquet dosyasından oku, eskiyse güncelle"""
    try:
        # Önce Parquet dosyasından oku
        stocks_dict, last_updated = load_bist_stocks_from_parquet()
        
        # Eğer veri yoksa veya eskiyse güncelle
        if not stocks_dict or is_bist_data_stale(last_updated, hours=24):
            st.info("📊 BIST hisse listesi güncelleniyor...")
            new_stocks = fetch_and_save_bist_stocks()
            if new_stocks:
                return new_stocks
        
        return stocks_dict
    except Exception as e:
        st.error(f"❌ BIST hisse yönetimi hatası: {str(e)}")
        return {}

# ================ NASDAQ VERİ YÖNETİMİ ================

def save_nasdaq_stocks_to_parquet(stocks_dict):
    """NASDAQ hisse senetlerini Azure Blob Storage'a Parquet olarak kaydet - Detaylı hata yönetimi"""
    try:
        if not stocks_dict:
            st.error("❌ Kaydedilecek NASDAQ verisi yok")
            return False
            
        # Azure Blob Storage servisini başlat
        blob_storage = AzureBlobStorage()
        
        if not blob_storage or not blob_storage.blob_service_client:
            st.error("❌ Azure Blob Storage servisi başlatılamadı")
            st.info("🔧 Azure connection string'i kontrol edin (.env dosyasında AZURE_STORAGE_CONNECTION_STRING)")
            return False
            
        st.info("✅ Azure Blob Storage bağlantısı başarılı")
            
        st.info(f"📊 {len(stocks_dict)} NASDAQ hissesi Parquet formatına dönüştürülüyor...")
        
        # DataFrame oluştur
        df = pd.DataFrame([
            {'symbol': symbol, 'name': name, 'last_updated': datetime.now().isoformat()}
            for symbol, name in stocks_dict.items()
        ])
        
        if df.empty:
            st.error("❌ DataFrame boş, kaydetme iptal edildi")
            return False
        
        st.info(f"📦 DataFrame oluşturuldu: {len(df)} satır, {len(df.columns)} sütun")
        
        # Parquet bytes'ını oluştur
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
        parquet_data = parquet_buffer.getvalue()
        
        if not parquet_data or len(parquet_data) == 0:
            st.error("❌ Parquet verisi oluşturulamadı")
            return False
            
        st.info(f"📊 Parquet verisi hazır: {len(parquet_data)} bytes ({len(parquet_data)//1024} KB)")
            
        st.info(f"☁️ Azure Blob Storage'a yükleniyor: '{NASDAQ_STOCKS_FILE}'")
        
        # Azure Blob Storage'a kaydet - verbose mode ile hataları görelim
        try:
            success = blob_storage.upload_file(file_content=parquet_data, blob_name=NASDAQ_STOCKS_FILE, silent=False)
            if success:
                st.success(f"🎉 {len(stocks_dict)} NASDAQ hissesi Azure'a başarıyla kaydedildi! ({len(parquet_data)//1024} KB)")
                return True
            else:
                st.error(f"❌ Azure blob upload başarısız oldu")
                # Alternatif upload metodu deneyelim
                st.info("🔄 Alternatif upload yöntemi deneniyor...")
                success_alt = blob_storage.upload_file(file_name=NASDAQ_STOCKS_FILE, data=parquet_data, silent=False)
                if success_alt:
                    st.success(f"✅ Alternatif yöntemle {len(stocks_dict)} NASDAQ hissesi kaydedildi!")
                    return True
                else:
                    st.error(f"❌ Tüm upload yöntemleri başarısız")
                    return False
                    
        except Exception as upload_e:
            st.error(f"❌ Upload işlemi sırasında hata: {str(upload_e)}")
            return False
            
    except Exception as e:
        st.error(f"❌ NASDAQ kaydetme kritik hatası: {type(e).__name__}: {str(e)}")
        print(f"NASDAQ save error details: {str(e)}")
        return False

@st.cache_data(ttl=3600, show_spinner=False)  # 1 saat cache, spinner yok
def load_nasdaq_stocks_from_parquet():
    """NASDAQ hisse senetlerini Azure Blob Storage'dan Parquet olarak oku - Ultra optimized caching"""
    try:
        # Azure Blob Storage'dan dene
        if blob_storage and blob_storage.blob_service_client:
            blob_data = blob_storage.download_file(NASDAQ_STOCKS_FILE)
            if blob_data and isinstance(blob_data, bytes) and len(blob_data) > 0:
                try:
                    # Bytes'ı pandas ile direkt oku - daha güvenli yöntem
                    parquet_buffer = io.BytesIO(blob_data)
                    df = pd.read_parquet(parquet_buffer, engine='pyarrow')
                    
                    if not df.empty and 'symbol' in df.columns and 'name' in df.columns:
                        stocks_dict = dict(zip(df['symbol'], df['name']))
                        last_updated = df['last_updated'].iloc[0] if 'last_updated' in df.columns and not df.empty else None
                        return stocks_dict, last_updated
                    else:
                        print("⚠️ Azure blob'da geçersiz DataFrame yapısı")
                        
                except Exception as parquet_error:
                    # Parquet okuma hatalarını filtrele
                    error_msg = str(parquet_error).lower()
                    if not any(x in error_msg for x in ["contentdecodepolicy", "http_generics", "unexpected return type", "deserialize"]):
                        print(f"Parquet okuma hatası: {str(parquet_error)}")
            else:
                print("⚠️ Azure blob'dan veri alınamadı (ilk çalıştırma olabilir)")
        
        return {}, None
        
    except Exception as e:
        print(f"NASDAQ yükleme genel hatası: {str(e)}")
        return {}, None

def is_nasdaq_data_stale(last_updated, hours=24):
    """NASDAQ verisinin eski olup olmadığını kontrol et"""
    if last_updated is None:
        return True
    
    try:
        if isinstance(last_updated, str):
            last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
        
        return (datetime.now() - last_updated).total_seconds() > hours * 3600
    except Exception:
        return True

def get_nasdaq_symbols_from_api():
    """NASDAQ hisse sembollerini çeşitli kaynaklardan dinamik olarak çek"""
    try:
        nasdaq_symbols = set()  # Tekrarları otomatik olarak önler
        
        # 1. NASDAQ resmi API (en güvenilir)
        try:
            nasdaq_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&offset=0&exchange=NASDAQ"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(nasdaq_url, headers=headers, timeout=20)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'rows' in data['data']:
                    for row in data['data']['rows']:
                        symbol = row.get('symbol', '').strip()
                        if symbol and len(symbol) <= 5 and not symbol.endswith('.'):
                            nasdaq_symbols.add(symbol)
                    st.success(f"✅ NASDAQ resmi API'den {len(nasdaq_symbols)} sembol alındı")
        except Exception as e:
            st.warning(f"⚠️ NASDAQ resmi API hatası: {str(e)}")
        
        # 2. FMP (Financial Modeling Prep) API - Backup
        if len(nasdaq_symbols) < 1000:
            try:
                fmp_url = "https://financialmodelingprep.com/api/v3/stock/list?apikey=demo"
                response = requests.get(fmp_url, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    for stock in data:
                        if (stock.get('exchange') == 'NASDAQ' or 
                            stock.get('exchangeShortName') == 'NASDAQ'):
                            symbol = stock.get('symbol', '').strip()
                            if symbol and len(symbol) <= 5 and not symbol.endswith('.'):
                                nasdaq_symbols.add(symbol)
                    st.success(f"✅ FMP API ile toplam {len(nasdaq_symbols)} sembol")
            except Exception as e:
                st.warning(f"⚠️ FMP API hatası: {str(e)}")
        
        # 3. Alpha Vantage demo API - Backup
        if len(nasdaq_symbols) < 1500:
            try:
                av_url = "https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo"
                response = requests.get(av_url, timeout=15)
                if response.status_code == 200:
                    lines = response.text.strip().split('\n')
                    for line in lines[1:]:  # Header'ı atla
                        parts = line.split(',')
                        if len(parts) >= 3:
                            symbol = parts[0].strip().strip('"')
                            exchange = parts[2].strip().strip('"')
                            if 'NASDAQ' in exchange and symbol and len(symbol) <= 5:
                                nasdaq_symbols.add(symbol)
                    st.success(f"✅ Alpha Vantage ile toplam {len(nasdaq_symbols)} sembol")
            except Exception as e:
                st.warning(f"⚠️ Alpha Vantage API hatası: {str(e)}")
                
        # 4. Backup - En büyük NASDAQ şirketleri (eğer API'lar çalışmazsa)
        if len(nasdaq_symbols) < 100:
            try:
                major_nasdaq = [
                    # Tech Giants
                    "AAPL", "GOOGL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ADBE",
                    # Cloud & Enterprise
                    "CRM", "ORCL", "NOW", "WDAY", "ZM", "TEAM", "SNOW", "DDOG", "CRWD", "OKTA",
                    # Semiconductors
                    "INTC", "AMD", "QCOM", "AVGO", "TXN", "MU", "LRCX", "KLAC", "AMAT", "MRVL",
                    # Fintech
                    "PYPL", "SQ", "SOFI", "AFRM", "COIN", "LC", "HOOD", "UPST", "BILL", "AFFIRM",
                    # E-commerce & Retail
                    "EBAY", "ETSY", "SHOP", "MELI", "JD", "PDD", "BABA", "BIDU", "COST", "SBUX",
                    # Media & Entertainment  
                    "DIS", "ROKU", "SPOT", "WBD", "NFLX", "PARA", "EA", "ATVI", "TTWO", "RBLX",
                    # Telecom & Communication
                    "T", "VZ", "TMUS", "CMCSA", "CHTR", "DISH",
                    # Healthcare & Biotech
                    "GILD", "AMGN", "BIIB", "REGN", "VRTX", "MRNA", "BNTX", "JNJ", "PFE", "ILMN",
                    # Auto & EV
                    "NIO", "XPEV", "LI", "RIVN", "LCID", "ENPH", "SEDG", "PLUG", "BLNK", "CHPT",
                    # Transport & Travel
                    "UBER", "LYFT", "DASH", "ABNB", "AIRB", "EXPE", "BKNG", "PCAR",
                    # Financial Services
                    "FISV", "PAYX", "ADP", "INTU", "ADSK", "CTSH",
                    # Social Media
                    "SNAP", "PINS", "ZG", "MTCH", "BMBL",
                    # Enterprise Software
                    "DOCU", "PLTR", "VEEV", "DXCM", "ISRG", "U", "NET", "CRWD",
                    # Commodity & Mining
                    "PAAS", "GOLD", "SBSW", "RGLD", "WPM", "AEM", "KGC", "FAST",
                    # Hardware & Devices
                    "MCHP", "ADI", "XLNX", "SWKS", "QRVO", "MPWR", "POWI", "CRUS", "MTSI"
                ]
                for symbol in major_nasdaq:
                    nasdaq_symbols.add(symbol)
                st.success(f"✅ Backup listesinden {len(major_nasdaq)} büyük NASDAQ şirketi eklendi")
            except Exception as e:
                st.warning(f"⚠️ Backup liste hatası: {str(e)}")
        
        final_symbols = sorted(list(nasdaq_symbols))  # Set'i listeye çevir ve sırala
        st.success(f"🎉 Toplam {len(final_symbols)} NASDAQ sembolü dinamik olarak alındı!")
        return final_symbols
        
    except Exception as e:
        st.error(f"❌ NASDAQ sembolleri alınırken genel hata: {str(e)}")
        # Critical fallback
        return [
            "AAPL", "GOOGL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ADBE",
            "CRM", "ORCL", "INTC", "AMD", "QCOM", "PYPL", "EBAY", "COST", "SBUX", "DIS"
        ]

def fetch_and_save_nasdaq_stocks():
    """NASDAQ hisselerini sadece Alpha Vantage API ile çekip Parquet dosyasına kaydet"""
    try:
        # Önce NASDAQ sembollerini dinamik olarak al
        nasdaq_symbols = get_nasdaq_symbols_from_api()
        
        if not nasdaq_symbols:
            st.error("❌ Hiç NASDAQ sembolü bulunamadı!")
            return {}
        
        # Doğrudan sembol listesini kullan - Yahoo Finance batch processing kaldırıldı
        nasdaq_stocks = {}
        
        st.info(f"� {len(nasdaq_symbols)} NASDAQ sembolü hazırlandı (basit format)")
        
        # Alpha Vantage API'den doğrudan NASDAQ hisselerini al (sembol + şirket ismi)
        st.info("🔄 Alpha Vantage API'den NASDAQ sembollerini ve isimlerini alıyor...")
        
        # Alpha Vantage API'den NASDAQ hisselerini al
        try:
            av_url = "https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo"
            response = requests.get(av_url, timeout=30)
            
            if response.status_code == 200:
                st.info("✅ Alpha Vantage API'ye bağlandı, veri işleniyor...")
                
                # CSV formatındaki veriyi parse et
                lines = response.text.strip().split('\n')
                
                if len(lines) > 1:
                    st.info(f"📊 {len(lines)-1} satır veri bulundu, işleniyor...")
                    
                    # Header'ı atla, veriyi işle
                    for i, line in enumerate(lines[1:]):  # İlk satır header
                        if i % 500 == 0 and i > 0:  # Her 500 satırda progress göster
                            st.info(f"📈 İşlenen: {i}/{len(lines)-1}")
                            
                        parts = line.split(',')
                        if len(parts) >= 6:  # Minimum required fields
                            symbol = parts[0].strip().strip('"')
                            name = parts[1].strip().strip('"')
                            exchange = parts[2].strip().strip('"')
                            
                            # Sadece NASDAQ borsasındaki hisseleri al
                            if ('NASDAQ' in exchange.upper() and 
                                symbol and 
                                len(symbol) <= 6 and 
                                symbol.isalpha() and  # Sadece harf içeren semboller
                                not symbol.endswith('.')):
                                
                                # Şirket ismi varsa kullan, yoksa sembol + Corporation formatı
                                if name and name != symbol and len(name) > 2:
                                    nasdaq_stocks[symbol] = name
                                else:
                                    nasdaq_stocks[symbol] = f"{symbol} Corporation"
                                
                    st.success(f"✅ Alpha Vantage'den {len(nasdaq_stocks)} NASDAQ hissesi alındı!")
                    
                    # Alınan hisselerin bir kısmını göster
                    if nasdaq_stocks:
                        sample_stocks = dict(list(nasdaq_stocks.items())[:10])
                        st.info("📋 Örnek hisseler:")
                        for symbol, name in sample_stocks.items():
                            st.text(f"  {symbol}: {name}")
                        
                        if len(nasdaq_stocks) > 10:
                            st.text(f"  ... ve {len(nasdaq_stocks)-10} hisse daha")
                
                else:
                    st.warning("⚠️ Alpha Vantage'den veri alındı ama içerik boş")
                    
            else:
                st.error(f"❌ Alpha Vantage API hatası: HTTP {response.status_code}")
                
        except Exception as e:
            st.error(f"❌ Alpha Vantage API bağlantı hatası: {str(e)}")
            # Fallback olarak mevcut sembol listesini kullan
            pass
        
        # Fallback: Eğer Alpha Vantage'den veri alınamazsa, mevcut symbol listesini kullan
        if not nasdaq_stocks and nasdaq_symbols:
            st.warning("⚠️ Alpha Vantage'den veri alınamadı, temel symbol listesi kullanılıyor...")
            for i, symbol in enumerate(nasdaq_symbols[:1000]):  # İlk 1000 sembol ile sınırla
                # Basit isim formatı - daha hızlı
                nasdaq_stocks[symbol] = f"{symbol} Corporation"
        
        # Minimum hisse sayısını kontrol et
        if not nasdaq_stocks:
            st.error("❌ Alpha Vantage'den hiç NASDAQ hissesi alınamadı!")
            return {}
        elif len(nasdaq_stocks) < 50:
            st.warning(f"⚠️ Sadece {len(nasdaq_stocks)} NASDAQ hissesi bulundu (çok az)")
        else:
            st.success(f"🎉 Toplam {len(nasdaq_stocks)} NASDAQ hissesi hazır!")
        
        # Parquet dosyasına kaydet
        save_result = save_nasdaq_stocks_to_parquet(nasdaq_stocks)
        if save_result:
            # save_nasdaq_stocks_to_parquet zaten başarı mesajı gösteriyor, tekrar göstermeye gerek yok
            return nasdaq_stocks
        else:
            st.error("❌ Parquet dosyasına kayıt başarısız!")
            return nasdaq_stocks  # Veri var ama kayıt başarısız, yine de veriyi döndür
            
    except Exception as e:
        st.error(f"❌ NASDAQ hisse listesi çekilirken kritik hata: {str(e)}")
        return {}

def get_nasdaq_stocks_smart():
    """Akıllı NASDAQ hisse yönetimi - Sadece Parquet dosyasından oku, manuel güncelleme gerektiğinde bilgilendir"""
    try:
        # Önce Parquet dosyasından oku
        stocks_dict, last_updated = load_nasdaq_stocks_from_parquet()
        
        # Eğer hiç veri yoksa minimal fallback döndür
        if not stocks_dict:
            st.info("NASDAQ hisse verisi bulunamadı. 'Veri Yönetimi' sekmesinden 'NASDAQ Hisselerini Çek ve Kaydet' butonuna basarak veri çekebilirsiniz.")
            return {
                "AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corporation",
                "AMZN": "Amazon.com Inc.", "TSLA": "Tesla Inc."  # Minimal fallback
            }
        
        # Eğer az sayıda hisse varsa (1000'den az) bilgilendir
        if len(stocks_dict) < 1000:
            st.info(f"📊 Şu anda {len(stocks_dict)} NASDAQ hissesi mevcut. Daha fazla hisse için 'Veri Yönetimi' sekmesinden güncelleme yapabilirsiniz.")
        
        # Eğer veri eskiyse (1 gün) bilgilendir
        if last_updated and is_nasdaq_data_stale(last_updated, hours=24):  # 1 gün = 24 saat
            # Uyarı metni kaldırıldı — kullanıcıya tekrar tekrar gösterilmemesi için
            # current_date = datetime.now().strftime("%d.%m.%Y")
            # data_date = datetime.fromisoformat(last_updated.replace('Z', '+00:00')).strftime("%d.%m.%Y") if isinstance(last_updated, str) else last_updated.strftime("%d.%m.%Y")
            # Daha önce burada gösterilen uyarı (st.warning) bilerek kaldırıldı.
            pass
        elif last_updated:
            data_date = datetime.fromisoformat(last_updated.replace('Z', '+00:00')).strftime("%d.%m.%Y %H:%M") if isinstance(last_updated, str) else last_updated.strftime("%d.%m.%Y %H:%M")
            # st.success(f"✅ NASDAQ verileri güncel: {data_date}")  # Bilgilendirme mesajı kaldırıldı
        
        return stocks_dict
        
    except Exception as e:
        st.warning(f"❌ NASDAQ hisse yönetimi hatası: {str(e)}")
        return {"AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corporation"}

# ================ TEFAS VERİ YÖNETİMİ DEVAMI ================

def get_tefas_sheet_name():
    """TEFAS verilerinin bulunduğu dosya formatını döndür"""
    return "parquet"  # Artık Parquet kullanıyoruz

def update_tefas_data_to_parquet(start_date, end_date, selected_funds=None):
    """TEFAS verilerini belirtilen tarih aralığında Parquet'e güncelle - SÜPER HIZLI"""
    try:
        debug_logger.info('TEFAS_UPDATE', f'Starting TEFAS update from {start_date} to {end_date}', {
            'start_date': str(start_date),
            'end_date': str(end_date),
            'selected_funds_count': len(selected_funds) if selected_funds else 'ALL',
            'platform': sys.platform,
            'azure_env': os.getenv('WEBSITE_INSTANCE_ID') is not None  # True if on Azure
        })
        
        if not tefas_dm.ensure_data_structure():
            debug_logger.error('TEFAS_UPDATE', 'Data structure initialization failed')
            return False
        
        debug_logger.info('TEFAS_UPDATE', 'Data structure verified')
        
        # Memory cache'i temizle
        tefas_dm.clear_memory_cache()
        debug_logger.info('TEFAS_UPDATE', 'Memory cache cleared')
        
        # Eğer selected_funds None ise, tüm fonları çek
        if selected_funds is None:
            st.info("🚀 Tüm TEFAS fonları çekilecek (Parquet ile 10x hızlı!)")
            use_all_funds = True
        else:
            use_all_funds = False
            st.info(f"🚀 {len(selected_funds)} seçili fon çekilecek (Parquet ile 10x hızlı!)")
        
        # Progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_days = (end_date - start_date).days + 1
        if use_all_funds:
            pass  # TÜM FONLAR çekilecek
        else:
            pass  # Seçili fonlar çekilecek
        
        # selected_funds'u güvenli hale getir
        if selected_funds is None:
            selected_funds = []
        
        # Hızlı işlem için günlük toplam veri çekme
        success_count = 0
        error_count = 0
        
        # Tarih aralığındaki her gün için
        current_date = start_date
        day_counter = 0
        
        while current_date <= end_date:
            day_counter += 1
            status_text.text(f"⚡ {current_date.strftime('%d.%m.%Y')} tarihi işleniyor... ({day_counter}/{total_days})")
            
            # Hafta sonu kontrolü
            if current_date.weekday() >= 5:  # Cumartesi=5, Pazar=6
                st.caption(f"⏭️ {current_date.strftime('%d.%m.%Y')} hafta sonu - atlanıyor")
                current_date += timedelta(days=1)
                progress_bar.progress(day_counter / total_days)
                continue
            
            try:
                debug_logger.debug('TEFAS_UPDATE', f'Processing date: {current_date.strftime("%Y-%m-%d")}', {
                    'date': str(current_date),
                    'day_of_week': current_date.strftime('%A'),
                    'progress': f'{day_counter}/{total_days}'
                })
                
                # Bu tarih için tüm fonları bir kez çek
                daily_fund_data = None
                api_success = False
                
                # Fiyat verisi için doğru endpoint'leri dene
                priority_configs = [
                    {'fund_type': 0, 'tab_code': 0, 'desc': 'Tüm Fonlar - Fiyat Verileri'},
                    {'fund_type': 1, 'tab_code': 0, 'desc': 'Değişken Fonlar - Fiyat Verileri'},
                    {'fund_type': 2, 'tab_code': 0, 'desc': 'Borçlanma Araçları - Fiyat Verileri'},
                    {'fund_type': 3, 'tab_code': 0, 'desc': 'Para Piyasası - Fiyat Verileri'},
                    {'fund_type': 4, 'tab_code': 0, 'desc': 'Karma Fonlar - Fiyat Verileri'},
                ]
                
                debug_logger.debug('TEFAS_UPDATE', f'Trying {len(priority_configs)} API configurations')
                
                for config in priority_configs:
                    try:
                        api_call_start = datetime.now()
                        debug_logger.debug('TEFAS_API', f'Attempting API call: {config["desc"]}', {
                            'config': config,
                            'date': current_date.strftime('%d.%m.%Y'),
                            'timestamp': api_call_start.isoformat()
                        })
                        
                        try:
                            # API çağrısı yapılıyor
                            debug_logger.info('TEFAS_API', 'Calling fetch_tefas_data from tefasfon package', {
                                'fund_type_code': config['fund_type'],
                                'tab_code': config['tab_code'],
                                'start_date': current_date.strftime('%d.%m.%Y'),
                                'end_date': current_date.strftime('%d.%m.%Y')
                            })
                            
                            daily_fund_data = fetch_tefas_data(
                                fund_type_code=config['fund_type'],
                                tab_code=config['tab_code'],
                                start_date=current_date.strftime('%d.%m.%Y'),
                                end_date=current_date.strftime('%d.%m.%Y')
                            )
                            
                            api_call_duration = (datetime.now() - api_call_start).total_seconds()
                            debug_logger.info('TEFAS_API', f'API call completed successfully: {config["desc"]}', {
                                'duration_seconds': api_call_duration,
                                'data_type': type(daily_fund_data).__name__,
                                'has_data': daily_fund_data is not None,
                                'data_length': len(daily_fund_data) if daily_fund_data is not None else 0
                            })
                            
                        except (ImportError, NameError, AttributeError) as import_error:
                            debug_logger.error('TEFAS_API', 'TEFAS API import/module error - using fallback', {
                                'error': str(import_error),
                                'error_type': type(import_error).__name__,
                                'config': config
                            })
                            st.warning(f"⚠️ TEFAS API import hatası, fallback kullanılıyor: {str(import_error)}")
                            
                            daily_fund_data = fallback_fetch_tefas_data(
                                fund_type_code=config['fund_type'],
                                tab_code=config['tab_code'],
                                start_date=current_date.strftime('%d.%m.%Y'),
                                end_date=current_date.strftime('%d.%m.%Y')
                            )
                        
                        # DataFrame kontrolü
                        if daily_fund_data is not None and not (hasattr(daily_fund_data, 'empty') and daily_fund_data.empty) and len(daily_fund_data) > 0:
                            api_success = True
                            fund_count = len(daily_fund_data)
                            
                            # İlk birkaç fonun bilgisini logla
                            sample_funds = []
                            if hasattr(daily_fund_data, 'to_dict'):
                                fund_records = daily_fund_data.to_dict('records')
                            else:
                                fund_records = daily_fund_data
                            
                            for i, fund in enumerate(fund_records[:3]):  # İlk 3 fon
                                sample_funds.append({
                                    'code': fund.get('Fon Kodu', 'N/A'),
                                    'name': fund.get('Fon Adı', 'N/A'),
                                    'price': fund.get('Fiyat', 'N/A')
                                })
                            
                            debug_logger.info('TEFAS_API', f'Successfully fetched {fund_count} funds', {
                                'fund_count': fund_count,
                                'config': config,
                                'sample_funds': sample_funds
                            })
                            st.caption(f"✅ API başarılı: {fund_count} fon - {config['desc']}")
                            break
                        else:
                            debug_logger.warning('TEFAS_API', 'API returned no data or empty data', {
                                'config': config,
                                'data_is_none': daily_fund_data is None,
                                'data_is_empty': hasattr(daily_fund_data, 'empty') and daily_fund_data.empty if daily_fund_data is not None else 'N/A',
                                'data_length': len(daily_fund_data) if daily_fund_data is not None else 0
                            })
                            
                    except Exception as api_error:
                        import traceback
                        debug_logger.error('TEFAS_API', f'Unexpected error during API call: {str(api_error)}', {
                            'config': config,
                            'error_type': type(api_error).__name__,
                            'error_message': str(api_error),
                            'traceback': traceback.format_exc()
                        })
                        st.caption(f"⚠️ API hatası ({config['desc']}): {str(api_error)}")
                        continue
                    if api_success:
                        break
                
                if api_success and daily_fund_data is not None and len(daily_fund_data) > 0:
                    debug_logger.info('TEFAS_DATA_PROCESSING', f'Starting data processing for {current_date.strftime("%d-%m-%Y")}', {
                        'date': str(current_date),
                        'data_count': len(daily_fund_data),
                        'use_all_funds': use_all_funds,
                        'selected_funds_count': len(selected_funds) if not use_all_funds else 'ALL'
                    })
                    
                    # DataFrame ise dict formatına çevir
                    if hasattr(daily_fund_data, 'to_dict'):
                        fund_records = daily_fund_data.to_dict('records')
                        debug_logger.debug('TEFAS_DATA_PROCESSING', 'Converted DataFrame to dict records')
                    else:
                        fund_records = daily_fund_data
                        debug_logger.debug('TEFAS_DATA_PROCESSING', 'Data already in dict format')
                    
                    daily_success = 0
                    daily_errors = []
                    
                    # Eğer tüm fonlar isteniyorsa, API'den gelen tüm fonları kaydet
                    if use_all_funds:
                        debug_logger.info('TEFAS_DATA_PROCESSING', f'Processing all {len(fund_records)} funds')
                        
                        for fund_info in fund_records:
                            fund_code = fund_info.get('Fon Kodu', '').strip()
                            
                            if fund_code:
                                fund_name = fund_info.get('Fon Adı', '')
                                price = round(float(fund_info.get('Fiyat', 0)), 6) if fund_info.get('Fiyat') else 0
                                total_value = round(float(fund_info.get('Fon Toplam Değer', 0)), 2) if fund_info.get('Fon Toplam Değer') else 0
                                unit_count = round(float(fund_info.get('Tedavüldeki Pay Sayısı', 0)), 2) if fund_info.get('Tedavüldeki Pay Sayısı') else 0
                                
                                # Memory cache'e ekle (ULTRA HIZLI)
                                result = tefas_dm.upsert_fund_data(
                                    current_date, fund_code, fund_name, 
                                    price, total_value, unit_count
                                )
                                
                                if result in ["updated", "inserted"]:
                                    success_count += 1
                                    daily_success += 1
                                else:
                                    error_detail = f"{fund_code}: {result}"
                                    daily_errors.append(error_detail)
                                    st.caption(f"⚠️ {fund_code} güncellenirken hata: {result}")
                        
                        debug_logger.info('TEFAS_DATA_PROCESSING', f'Completed processing for {current_date.strftime("%Y-%m-%d")}', {
                            'successful': daily_success,
                            'errors': len(daily_errors),
                            'error_details': daily_errors[:5] if daily_errors else []  # İlk 5 hata
                        })
                        st.caption(f"⚡ {current_date.strftime('%d.%m.%Y')}: {daily_success} fon işlendi (Memory Cache)")
                    else:
                        debug_logger.info('TEFAS_DATA_PROCESSING', f'Processing selected {len(selected_funds)} funds')
                        
                        # Seçili fonları kaydet
                        for fund_code in selected_funds:
                            found = False
                            for fund_info in fund_records:
                                api_fund_code = fund_info.get('Fon Kodu', '').strip()
                                
                                if api_fund_code == fund_code:
                                    fund_name = fund_info.get('Fon Adı', '')
                                    price = round(float(fund_info.get('Fiyat', 0)), 6) if fund_info.get('Fiyat') else 0
                                    total_value = round(float(fund_info.get('Fon Toplam Değer', 0)), 2) if fund_info.get('Fon Toplam Değer') else 0
                                    unit_count = round(float(fund_info.get('Tedavüldeki Pay Sayısı', 0)), 2) if fund_info.get('Tedavüldeki Pay Sayısı') else 0
                                    
                                    # Memory cache'e ekle (ULTRA HIZLI)
                                    result = tefas_dm.upsert_fund_data(
                                        current_date, fund_code, fund_name, 
                                        price, total_value, unit_count
                                    )
                                    
                                    if result in ["updated", "inserted"]:
                                        success_count += 1
                                        daily_success += 1
                                    else:
                                        st.caption(f"⚠️ {fund_code} güncellenirken hata: {result}")
                                    
                                    found = True
                                    break
                            
                            if not found:
                                error_count += 1
                        
                        st.caption(f"⚡ {current_date.strftime('%d.%m.%Y')}: {daily_success}/{len(selected_funds)} fon işlendi")
                else:
                    # API başarısız - tüm konfigürasyonlar denendi
                    debug_logger.error('TEFAS_API', f'All API configurations failed for {current_date.strftime("%Y-%m-%d")}', {
                        'date': str(current_date),
                        'configs_tried': len(priority_configs),
                        'use_all_funds': use_all_funds,
                        'selected_funds_count': len(selected_funds) if selected_funds else 0
                    })
                    
                    if selected_funds:
                        error_count += len(selected_funds)
                    st.warning(f"⚠️ {current_date.strftime('%d.%m.%Y')} tarihi için API verisi alınamadı")
                
            except Exception as e:
                import traceback
                debug_logger.error('TEFAS_UPDATE', f'Unexpected exception on {current_date.strftime("%Y-%m-%d")}', {
                    'date': str(current_date),
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc()
                })
                
                if selected_funds:
                    error_count += len(selected_funds)
                st.error(f"❌ {current_date.strftime('%d.%m.%Y')} - Genel hata: {str(e)}")
            
            current_date += timedelta(days=1)
            progress_bar.progress(day_counter / total_days)
            time.sleep(0.3)  # API limitleri için kısa bekleme
        
        # Toplu Parquet kaydetme (EN HIZLI)
        debug_logger.info('TEFAS_UPDATE', 'Starting bulk save to Parquet', {
            'total_days_processed': total_days,
            'success_count': success_count,
            'error_count': error_count
        })
        
        status_text.text("💾 Veriler Azure Blob Storage'a kaydediliyor...")
        if tefas_dm.bulk_save_to_parquet():
            debug_logger.info('TEFAS_UPDATE', 'Bulk save to Parquet successful')
            progress_bar.progress(1.0)
            status_text.text(f"🎉 İşlem tamamlandı! Başarılı: {success_count}, Hata: {error_count}")
            
            # Son kontrol - Azure'dan dosya boyutunu kontrol et
            try:
                debug_logger.info('TEFAS_UPDATE', 'Verifying saved Parquet file')
                # Azure'dan dosyayı kontrol et
                if tefas_dm.blob_storage.file_exists(TEFAS_DATA_FILE):
                    content = tefas_dm.blob_storage.download_file(TEFAS_DATA_FILE)
                    if content:
                        parquet_buffer = io.BytesIO(content)
                        df_check = pd.read_parquet(parquet_buffer)
                        
                        debug_logger.info('TEFAS_UPDATE', 'Parquet file verified successfully', {
                            'total_rows': len(df_check),
                            'file_name': TEFAS_DATA_FILE
                        })
                        st.success(f"🚀 Azure'da TEFAS Parquet dosyası: {len(df_check)} toplam satır! (Excel'den 10-50x daha hızlı)")
                    else:
                        debug_logger.warning('TEFAS_UPDATE', 'Parquet file downloaded but content is empty')
                else:
                    debug_logger.warning('TEFAS_UPDATE', 'Parquet file does not exist in Azure')
            except Exception as read_error:
                debug_logger.error('TEFAS_UPDATE', 'Error verifying Parquet file', {
                    'error': str(read_error),
                    'error_type': type(read_error).__name__
                })
                st.warning(f"⚠️ Azure Parquet dosyası kontrol edilirken hata: {str(read_error)}")
            
            debug_logger.info('TEFAS_UPDATE', 'TEFAS update completed successfully', {
                'total_success': success_count,
                'total_errors': error_count,
                'date_range': f'{start_date} to {end_date}'
            })
            return True
        else:
            debug_logger.error('TEFAS_UPDATE', 'Bulk save to Parquet failed')
            st.error("❌ TEFAS Parquet Azure'a kaydetme başarısız!")
            return False
        
    except Exception as e:
        import traceback
        debug_logger.error('TEFAS_UPDATE', 'TEFAS update failed with exception', {
            'error': str(e),
            'error_type': type(e).__name__,
            'traceback': traceback.format_exc()
        })
        st.error(f"❌ TEFAS verileri Azure Parquet'e kaydedilirken hata: {str(e)}")
        return False

def get_tefas_price_from_parquet(fund_code, target_date):
    """Parquet'ten belirli bir fon ve tarihe ait fiyat bilgisini al - HIZLI"""
    return tefas_dm.get_fund_price(fund_code, target_date)

def get_tefas_latest_price_from_parquet(fund_code):
    """Parquet'ten belirli bir fonun en son fiyat bilgisini al - HIZLI"""
    return tefas_dm.get_latest_fund_price(fund_code)

# Geriye uyumluluk için eski isimleri koruyoruz
def get_tefas_price_from_excel(fund_code, target_date):
    """Excel yerine Parquet kullanıyor - geriye uyumluluk için"""
    return get_tefas_price_from_parquet(fund_code, target_date)

def get_tefas_latest_price_from_excel(fund_code):
    """Excel yerine Parquet kullanıyor - geriye uyumluluk için"""
    return get_tefas_latest_price_from_parquet(fund_code)

def update_summary_statistics(start_date, end_date):
    """Özet istatistikleri hesapla ve Parquet'e kaydet - HIZLI"""
    try:
        # Blob üzerinde TEFAS veri dosyasının varlığını kontrol et
        try:
            if not tefas_dm.blob_storage.file_exists(TEFAS_DATA_FILE):
                return False
            content = tefas_dm.blob_storage.download_file(TEFAS_DATA_FILE)
            if not content:
                return False
            parquet_buffer = io.BytesIO(content)
            df = pd.read_parquet(parquet_buffer)
        except Exception as e:
            return False
        
        # Tarih aralığında günlük özet oluştur
        summary_data = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            daily_data = df[df['Tarih'].dt.strftime('%Y-%m-%d') == date_str]
            
            if not daily_data.empty:
                total_funds = len(daily_data)
                positive_returns = len(daily_data[daily_data['Gunluk_Getiri'] > 0])
                negative_returns = len(daily_data[daily_data['Gunluk_Getiri'] < 0])
                avg_return = daily_data['Gunluk_Getiri'].mean()
                max_return = daily_data['Gunluk_Getiri'].max()
                min_return = daily_data['Gunluk_Getiri'].min()
                
                summary_data.append({
                    'Tarih': current_date,
                    'Toplam_Fon_Sayisi': total_funds,
                    'Pozitif_Getiri': positive_returns,
                    'Negatif_Getiri': negative_returns,
                    'Ortalama_Getiri': round(avg_return, 4),
                    'En_Yuksek_Getiri': round(max_return, 4),
                    'En_Dusuk_Getiri': round(min_return, 4),
                    'Guncelleme_Zamani': datetime.now()
                })
            
            current_date += timedelta(days=1)
        
        # Özet DataFrame'i oluştur ve kaydet
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_parquet(TEFAS_SUMMARY_FILE, compression='snappy', index=False)
            st.success(f"✅ {len(summary_data)} günlük özet istatistik Parquet'e kaydedildi")
        
        return True
        
    except Exception as e:
        st.warning(f"⚠️ Özet istatistikler güncellenirken hata: {str(e)}")
        return False

def get_fund_type_code(fund_code):
    """Fon koduna göre TEFAS API'si için uygun tip kodunu döndür"""
    # TEFAS API'si için doğru fon tip kodları
    
    # Değişken fonlar (Hisse Senedi Yoğun)
    if fund_code.endswith('PD') or fund_code in ['HPD', 'GPD', 'ZPD', 'IPD', 'APD']:
        return 1  # Değişken Fonlar
    
    # Hisse senedi fonları
    if fund_code.endswith('HS'):
        return 1  # Değişken Fonlar kategorisinde
    
    # Borçlanma araçları fonları
    if fund_code.endswith('BF'):
        return 2  # Borçlanma Araçları Fonları
    
    # Para piyasası fonları
    if fund_code.endswith('PP'):
        return 3  # Para Piyasası Fonları
    
    # Altın fonları
    if fund_code.endswith('AL') or fund_code in ['AAL', 'IAL', 'HAL', 'ZAL', 'YAL', 'GAL']:
        return 1  # Değişken Fonlar kategorisinde
    
    # Karma fonları
    if fund_code.endswith('KA'):
        return 4  # Karma Fonlar
    
    # Döviz fonları
    if fund_code.endswith('DV'):
        return 5  # Döviz Fonları
    
    # Varsayılan olarak değişken fon
    return 1

def get_fund_category(fund_code):
    """Fon koduna göre kategori belirle"""
    if fund_code.endswith('PD'):
        return 'Değişken Fon'
    elif fund_code.endswith('HS'):
        return 'Hisse Senedi Fonu'
    elif fund_code.endswith('BF'):
        return 'Borçlanma Araçları Fonu'
    elif fund_code.endswith('PP'):
        return 'Para Piyasası Fonu'
    elif fund_code.endswith('AL'):
        return 'Altın Fonu'
    elif fund_code.endswith('KA'):
        return 'Karma Fon'
    elif fund_code.endswith('DV'):
        return 'Döviz Fonu'
    else:
        return 'Diğer'

def get_fund_management_company(fund_code):
    """Fon koduna göre yönetim şirketini belirle"""
    if fund_code.startswith('H'):
        return 'Halk Portföy'
    elif fund_code.startswith('G'):
        return 'Gedik Portföy'
    elif fund_code.startswith('Z'):
        return 'Ziraat Portföy'
    elif fund_code.startswith('I'):
        return 'İş Portföy'
    elif fund_code.startswith('A'):
        return 'Ak Portföy'
    elif fund_code.startswith('Y'):
        return 'Yapı Kredi Portföy'
    elif fund_code.startswith('T'):
        return 'TSKB Portföy'
    elif fund_code.startswith('O'):
        return 'ODEABANK Portföy'
    else:
        return 'Bilinmiyor'

# ================ BANNER CAROUSEL VE ABONELİK BİLGİLERİ ================

def show_feature_carousel():
    """Login sayfasında özellik tanıtım carousel'i göster"""
    st.markdown("""
    <style>
    .carousel-container {
        position: relative;
        width: 100%;
        max-width: 900px;
        margin: 0 auto 30px auto;
        overflow: hidden;
        border-radius: 20px;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
    }
    
    .carousel-slides {
        display: flex;
        animation: slide 15s infinite;
        width: 300%;
    }
    
    .carousel-slide {
        width: 100%;
        min-height: 280px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        padding: 40px;
        text-align: center;
    }
    
    .slide-1 {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d1b2a 100%);
    }
    
    .slide-2 {
        background: linear-gradient(135deg, #2d5016 0%, #1a2f0e 100%);
    }
    
    .slide-3 {
        background: linear-gradient(135deg, #4a1942 0%, #2d0f29 100%);
    }
    
    .carousel-slide h2 {
        color: #ffffff;
        font-size: 28px;
        font-weight: 700;
        margin-bottom: 15px;
    }
    
    .carousel-slide p {
        color: #b8c5d6;
        font-size: 16px;
        line-height: 1.6;
        max-width: 600px;
    }
    
    .carousel-slide .feature-icon {
        font-size: 60px;
        margin-bottom: 20px;
    }
    
    .carousel-dots {
        display: flex;
        justify-content: center;
        gap: 10px;
        padding: 15px;
        background: rgba(0, 0, 0, 0.3);
    }
    
    .carousel-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: rgba(255, 255, 255, 0.3);
    }
    
    .carousel-dot.active {
        background: #3b82f6;
    }
    
    @keyframes slide {
        0%, 30% { transform: translateX(0); }
        33%, 63% { transform: translateX(-33.333%); }
        66%, 96% { transform: translateX(-66.666%); }
        100% { transform: translateX(0); }
    }
    </style>
    
    <div class="carousel-container">
        <div class="carousel-slides">
            <div class="carousel-slide slide-1">
                <div class="feature-icon">📊</div>
                <h2>Portföy Yönetimi</h2>
                <p>BIST, NASDAQ, Kıymetli Madenler, Döviz ve TEFAS fonlarınızı tek bir platformda takip edin. Anlık fiyat güncellemeleri ve detaylı performans analizleri ile yatırımlarınızı kontrol altında tutun.</p>
            </div>
            <div class="carousel-slide slide-2">
                <div class="feature-icon">📈</div>
                <h2>Piyasa Analizi</h2>
                <p>Gelişmiş teknik analiz araçları, interaktif grafikler ve güncel piyasa verileri ile bilinçli yatırım kararları alın. RSI, MACD, Bollinger Bands ve daha fazlası.</p>
            </div>
            <div class="carousel-slide slide-3">
                <div class="feature-icon">🎁</div>
                <h2>1 Ay Ücretsiz Deneyin!</h2>
                <p>Yeni üyelere özel 30 gün ücretsiz deneme süresi! Kredi kartı gerekmez. Tüm özelliklere tam erişim ile platformumuzu risk almadan keşfedin.</p>
            </div>
        </div>
        <div class="carousel-dots">
            <div class="carousel-dot active"></div>
            <div class="carousel-dot"></div>
            <div class="carousel-dot"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def show_subscription_info():
    """Abonelik planları ve ödeme bilgilerini göster"""
    
    # Ücretsiz deneme banner'ı
    st.markdown("""
    <div style="background: linear-gradient(135deg, rgba(251, 191, 36, 0.25) 0%, rgba(245, 158, 11, 0.15) 100%); 
                padding: 20px; border-radius: 16px; border: 2px solid rgba(251, 191, 36, 0.5);
                text-align: center; margin-bottom: 20px;">
        <h3 style="color: #fbbf24; margin-bottom: 8px;">🎁 Yeni Üyelere Özel!</h3>
        <p style="font-size: 24px; font-weight: 700; color: #ffffff; margin: 5px 0;">1 AY ÜCRETSİZ DENEME</p>
        <p style="color: #fcd34d; font-size: 14px;">Kayıt olduğunuzda otomatik olarak 30 günlük ücretsiz deneme süresi başlar!</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<h3 style='text-align: center;'>Abonelik Planları</h3>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, rgba(37, 99, 235, 0.2) 0%, rgba(15, 23, 42, 0.9) 100%); 
                    padding: 12px; border-radius: 12px; border: 1px solid rgba(59, 130, 246, 0.3);
                    text-align: center;">
            <p style="color: #60a5fa; margin-bottom: 4px; font-size: 13px;">3 Aylık</p>
            <p style="font-size: 22px; font-weight: 700; color: #ffffff; margin: 4px 0;">90 ₺</p>
            <p style="color: #94a3b8; font-size: 12px; margin: 0;">Aylık 30 ₺</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, rgba(16, 185, 129, 0.2) 0%, rgba(15, 23, 42, 0.9) 100%); 
                    padding: 12px; border-radius: 12px; border: 1px solid rgba(16, 185, 129, 0.3);
                    text-align: center;">
            <p style="color: #34d399; margin-bottom: 4px; font-size: 13px;">12 Aylık</p>
            <p style="font-size: 22px; font-weight: 700; color: #ffffff; margin: 4px 0;">360 ₺</p>
            <p style="color: #94a3b8; font-size: 12px; margin: 0;">Aylık 30 ₺</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<h3 style='text-align: center;'>Ödeme Bilgileri</h3>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background: rgba(15, 23, 42, 0.8); padding: 12px; border-radius: 12px; border: 1px solid rgba(148, 163, 184, 0.2); text-align: center;">
        <p style="color: #94a3b8; margin-bottom: 4px; font-size: 12px;"><strong style="color: #e2e8f0;">Banka:</strong> {PAYMENT_INFO['bank_name']}</p>
        <p style="color: #94a3b8; margin-bottom: 4px; font-size: 12px;"><strong style="color: #e2e8f0;">Alıcı:</strong> {PAYMENT_INFO['account_holder']}</p>
        <p style="color: #94a3b8; margin-bottom: 4px; font-size: 12px;"><strong style="color: #e2e8f0;">IBAN:</strong> <code style="background: rgba(59, 130, 246, 0.2); padding: 2px 6px; border-radius: 4px; color: #60a5fa; font-size: 11px;">{PAYMENT_INFO['iban']}</code></p>
        <p style="color: #ffffff; font-size: 11px; margin-top: 8px; margin-bottom: 0;">⚠️ {PAYMENT_INFO['description']}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # İletişim bilgisi
    st.markdown("""
    <div style="text-align: center; margin-top: 20px; padding: 10px;">
        <p style="color: #94a3b8; font-size: 12px; margin: 0;">📧 İletişim: <a href="mailto:infofinansapp@gmail.com" style="color: #60a5fa; text-decoration: none;">infofinansapp@gmail.com</a></p>
    </div>
    """, unsafe_allow_html=True)

def show_subscription_expired_page():
    """Abonelik süresi dolmuş kullanıcılar için sayfa göster"""
    inject_dark_theme()
    
    st.markdown("""
    <div style="text-align: center; padding: 40px;">
        <h1 style="color: #ef4444;">⏰ Abonelik Süreniz Doldu</h1>
        <p style="color: #94a3b8; font-size: 18px; margin: 20px 0;">
            Platformu kullanmaya devam etmek için lütfen aboneliğinizi yenileyin.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    show_subscription_info()
    
    user_email = st.session_state.get('user_email', '')
    subscription = get_user_subscription(user_email)
    
    if subscription:
        st.markdown("---")
        st.markdown("### 📋 Mevcut Abonelik Bilgileriniz")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Plan", subscription.get("plan_name", "Bilinmiyor"))
        with col2:
            st.metric("Başlangıç", subscription.get("start_date", "-"))
        with col3:
            st.metric("Bitiş", subscription.get("end_date", "-"))
    
    st.markdown("---")
    col_logout1, col_logout2 = st.columns(2)
    with col_logout1:
        if st.button("🚪 Çıkış Yap", type="primary", use_container_width=True):
            # 🔐 GÜVENLİ ÇIKIŞ: Token'ı iptal et
            user_email = st.session_state.get('user_email', '')
            if user_email:
                user_id = get_user_id_from_email(user_email)
                revoke_remember_me_token(user_email, user_id, series_id=None)
            
            # Cookie'leri temizle
            if COOKIES_AVAILABLE and cookie_manager is not None:
                cookie_manager.delete("finapp_remember_token", key="del_token_1")
                cookie_manager.delete("finapp_remembered_email", key="del_email_1")
                cookie_manager.delete("finapp_persistent_logins", key="del_logins_1")
            
            clear_remembered_credentials()
            # Session state'i temizle ve logout flag'i ayarla
            st.session_state['just_logged_out'] = True
            st.session_state['remembered_email'] = ""
            for key in ['logged_in', 'user_email', 'user_name']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    with col_logout2:
        if st.button("🔐 Beni Hatırlamayı Sil", use_container_width=True):
            # 🔐 GÜVENLİ: Token'ları iptal et
            user_email = st.session_state.get('user_email', '')
            if user_email:
                user_id = get_user_id_from_email(user_email)
                revoke_remember_me_token(user_email, user_id, series_id=None)  # Tüm token'ları sil
            
            clear_remembered_credentials()
            # Cookie manager ile sil
            if COOKIES_AVAILABLE and cookie_manager is not None:
                try:
                    cookie_manager.delete("finapp_remember_token", key="del_token_2")
                    cookie_manager.delete("finapp_remembered_email", key="del_email_2")
                    cookie_manager.delete("finapp_persistent_logins", key="del_logins_2")
                except Exception as e:
                    st.warning(f"Cookie temizleme hatası: {e}")
            st.success("✅ Kaydedilen bilgiler ve tüm oturum token'ları silindi!")
            st.info("Bir sonraki girişte login bilgilerini tekrar girmeniz gerekecek.")

# ================ ADMİN PANELİ ================

def show_admin_panel():
    """Admin paneli - Kullanıcı abonelik yönetimi"""
    st.markdown("## ⚙️ Admin Paneli - Abonelik Yönetimi")
    
    # Tüm kullanıcıları listele
    users = load_users()
    subscriptions = load_subscriptions()
    
    st.markdown("### 👥 Kayıtlı Kullanıcılar")
    
    # Kullanıcı tablosu oluştur
    user_data = []
    for email, user_info in users.items():
        sub = subscriptions.get(email.lower(), {})
        user_data.append({
            "Email": email,
            "Ad": user_info.get("name", "-"),
            "Kayıt Tarihi": user_info.get("registered_at", "-")[:10] if user_info.get("registered_at") else "-",
            "Plan": sub.get("plan_name", "Abonelik Yok"),
            "Başlangıç": sub.get("start_date", "-"),
            "Bitiş": sub.get("end_date", "-"),
            "Durum": "✅ Aktif" if is_subscription_active(email) else "❌ Pasif"
        })
    
    if user_data:
        df = pd.DataFrame(user_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("Henüz kayıtlı kullanıcı yok.")
    
    st.markdown("---")
    st.markdown("### ➕ Abonelik Tanımla / Düzenle")
    
    col1, col2 = st.columns(2)
    
    with col1:
        user_emails = list(users.keys())
        selected_email = st.selectbox("Kullanıcı Seçin", user_emails, key="admin_select_user")
        
        plan_options = {
            "trial": "🎁 Deneme (1 Ay - Ücretsiz)",
            "3_months": "3 Aylık (90 TL)",
            "12_months": "12 Aylık (360 TL)",
            "custom": "Özel Tarih"
        }
        selected_plan = st.selectbox("Plan Seçin", list(plan_options.keys()), 
                                      format_func=lambda x: plan_options[x], key="admin_select_plan")
    
    with col2:
        if selected_plan == "custom":
            start_date = st.date_input("Başlangıç Tarihi", value=datetime.now(), key="admin_start_date")
            end_date = st.date_input("Bitiş Tarihi", value=datetime.now() + timedelta(days=30), key="admin_end_date")
        else:
            start_date = st.date_input("Başlangıç Tarihi", value=datetime.now(), key="admin_start_date_auto")
            plan_info = SUBSCRIPTION_PLANS.get(selected_plan, {"months": 1})
            months = plan_info["months"]
            end_date = start_date + timedelta(days=months * 30)
            st.info(f"Bitiş Tarihi: {end_date.strftime('%Y-%m-%d')}")
    
    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        if st.button("✅ Abonelik Tanımla", type="primary", use_container_width=True):
            if selected_email:
                plan_key = selected_plan if selected_plan != "custom" else "custom"
                plan_info = SUBSCRIPTION_PLANS.get(plan_key, {"name": "Özel Plan", "months": 1})
                
                subscriptions = load_subscriptions()
                subscriptions[selected_email.lower()] = {
                    "plan": plan_key,
                    "plan_name": plan_info.get("name", "Özel Plan"),
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d") if selected_plan == "custom" else (start_date + timedelta(days=plan_info["months"] * 30)).strftime("%Y-%m-%d"),
                    "status": "active",
                    "is_active": True,
                    "is_trial": selected_plan == "trial",
                    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "created_by": st.session_state.get('user_email', 'admin')
                }
                
                if save_subscriptions(subscriptions):
                    st.success(f"✅ {selected_email} için abonelik tanımlandı!")
                    st.rerun()
                else:
                    st.error("❌ Abonelik kaydedilemedi!")
    
    with col_btn2:
        if st.button("🚫 Aboneliği İptal Et", type="secondary", use_container_width=True):
            if selected_email:
                if cancel_subscription(selected_email):
                    st.success(f"✅ {selected_email} aboneliği iptal edildi!")
                    st.rerun()
                else:
                    st.error("❌ İptal işlemi başarısız!")
    
    # İstatistikler
    st.markdown("---")
    st.markdown("### 📊 İstatistikler")
    
    total_users = len(users)
    active_subs = sum(1 for email in users.keys() if is_subscription_active(email))
    expired_subs = total_users - active_subs
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Toplam Kullanıcı", total_users)
    with col2:
        st.metric("Aktif Abonelik", active_subs)
    with col3:
        st.metric("Süresi Dolmuş", expired_subs)

# Login ekranı
def show_login_page():
    """Kullanıcı giriş ekranını göster"""
    
    # Dark mode CSS for login page
    st.markdown("""
    <style>
    /* Login page dark theme */
    .stApp {
        background: radial-gradient(circle at 20% 20%, #0b1327 0%, #050b16 45%, #01030b 100%) !important;
    }
    
    /* Fix white header/toolbar area at the top */
    [data-testid="stHeader"] {
        background: #0b1327 !important;
        background-color: #0b1327 !important;
    }
    
    [data-testid="stToolbar"] {
        background: #0b1327 !important;
        background-color: #0b1327 !important;
    }
    
    header[data-testid="stHeader"] {
        background: #0b1327 !important;
        background-color: #0b1327 !important;
    }
    
    /* Top toolbar container */
    .stApp > header {
        background: #0b1327 !important;
        background-color: #0b1327 !important;
    }
    
    /* Main container background */
    .main .block-container {
        background: transparent !important;
    }
    
    /* AppView container */
    [data-testid="stAppViewContainer"] {
        background: radial-gradient(circle at 20% 20%, #0b1327 0%, #050b16 45%, #01030b 100%) !important;
    }
    
    /* Title styling */
    h1 {
        color: #e2e8f0 !important;
        text-align: center;
        font-weight: 700;
        margin-bottom: 2rem;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(15, 23, 42, 0.8);
        border-radius: 12px;
        padding: 8px;
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #94a3b8;
        border: none;
        border-radius: 8px;
        padding: 12px 24px;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.9), rgba(29, 78, 216, 0.9));
        color: #ffffff !important;
    }
    
    /* Subheader styling */
    h3 {
        color: #e2e8f0 !important;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    
    /* Input fields */
    .stTextInput input {
        background: rgba(15, 23, 42, 0.8) !important;
        color: #e2e8f0 !important;
        border: 1px solid rgba(100, 116, 139, 0.3) !important;
        border-radius: 8px !important;
        padding: 12px !important;
    }
    
    .stTextInput input:focus {
        border-color: rgba(59, 130, 246, 0.6) !important;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2) !important;
    }
    
    /* Labels */
    .stTextInput label {
        color: #e2e8f0 !important;
        font-weight: 500 !important;
    }
    
    /* Buttons */
    .stButton button {
        background: linear-gradient(135deg, #2563eb, #1d4ed8) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 12px 24px !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px rgba(37, 99, 235, 0.3) !important;
    }
    
    /* Form submit button */
    .stFormSubmitButton button {
        background: linear-gradient(135deg, #2563eb, #1d4ed8) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 12px 24px !important;
        font-weight: 600 !important;
        width: 100%;
    }
    
    /* Info, success, error messages */
    .stAlert {
        background: rgba(15, 23, 42, 0.8) !important;
        border-radius: 8px !important;
        color: #e2e8f0 !important;
        border-left: 4px solid !important;
    }
    .stAlert p,
    .stAlert span,
    .stAlert div {
        color: #e2e8f0 !important;
    }
    
    div[data-baseweb="notification"] {
        background: rgba(15, 23, 42, 0.95) !important;
        color: #e2e8f0 !important;
    }
    
    /* Horizontal rule */
    hr {
        border-color: rgba(100, 116, 139, 0.2) !important;
        margin: 2rem 0 !important;
    }
    
    /* Form container */
    [data-testid="stForm"] {
        background: rgba(15, 23, 42, 0.5) !important;
        border: 1px solid rgba(59, 130, 246, 0.2) !important;
        border-radius: 12px !important;
        padding: 2rem !important;
    }
    
    /* Success message specific styling */
    .stSuccess {
        background: rgba(16, 185, 129, 0.15) !important;
        border-left-color: #10b981 !important;
    }
    .stSuccess p, .stSuccess span, .stSuccess div, .stSuccess strong, .stSuccess em, .stSuccess code {
        color: #ffffff !important;
    }
    
    /* Error message specific styling */
    .stError {
        background: rgba(239, 68, 68, 0.25) !important;
        border-left-color: #ef4444 !important;
        border: 2px solid rgba(239, 68, 68, 0.5) !important;
    }
    .stError p, .stError span, .stError div, .stError strong, .stError em, .stError code {
        color: #ffffff !important;
        font-weight: 600 !important;
    }
    
    /* Info message specific styling */
    .stInfo {
        background: rgba(59, 130, 246, 0.25) !important;
        border-left-color: #3b82f6 !important;
        border: 2px solid rgba(59, 130, 246, 0.5) !important;
    }
    .stInfo p, .stInfo span, .stInfo div, .stInfo strong, .stInfo em, .stInfo code {
        color: #ffffff !important;
        font-weight: 500 !important;
    }
    
    /* Warning message specific styling */
    .stWarning {
        background: rgba(245, 158, 11, 0.25) !important;
        border-left-color: #f59e0b !important;
        border: 2px solid rgba(245, 158, 11, 0.5) !important;
    }
    .stWarning p, .stWarning span, .stWarning div, .stWarning strong, .stWarning em, .stWarning code {
        color: #ffffff !important;
        font-weight: 500 !important;
    }
    
    /* Radio button text styling - Tab metinlerini beyaz yap */
    .stRadio > label {
        color: white !important;
    }
    
    .stRadio > div[role="radiogroup"] > label {
        color: white !important;
    }
    
    .stRadio > div[role="radiogroup"] > label > div {
        color: white !important;
    }
    
    .stRadio > div[role="radiogroup"] > label > div > p {
        color: white !important;
        font-weight: 500 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Özellik tanıtım carousel'i göster
    show_feature_carousel()
    
    st.title("🔐 Portföy Yönetim Sistemi")
    
    # Tab seçimi için radio buton - widget state'ini kullan
    tab_names = ["🔑 Giriş Yap", "📝 Kayıt Ol", "🔄 Şifre Sıfırla"]
    
    # Default index belirleme - sadece ilk render'da
    if 'tab_selector' not in st.session_state:
        st.session_state['tab_selector'] = 0
    
    # Tab değiştirme isteği varsa, widget oluşturulmadan önce değeri ayarla
    if st.session_state.get('switch_to_reset', False):
        st.session_state['tab_selector'] = 2
        st.session_state['switch_to_reset'] = False
    
    # Kayıt başarılı olduysa giriş sekmesine yönlendir
    if st.session_state.get('redirect_to_login', False):
        st.session_state['tab_selector'] = 0
        st.session_state['redirect_to_login'] = False
    
    # Radio butonu - widget kendi state'ini yönetsin
    selected_tab_index = st.radio(
        "Sekme seçimi", 
        options=range(len(tab_names)),
        format_func=lambda x: tab_names[x],
        horizontal=True, 
        label_visibility="collapsed",
        key="tab_selector"
    )
    
    selected_tab = tab_names[selected_tab_index]
    
    st.markdown("---")
    
    # Seçilen tab'a göre içerik göster
    if selected_tab == "🔑 Giriş Yap":
        # 🔐 Cookie Manager ile Remember Me (extra-streamlit-components)
        # Logout sonrası auto-login'i atla
        if st.session_state.get('just_logged_out', False):
            st.session_state['just_logged_out'] = False
            # Cookie'leri tekrar temizle (async silme tamamlanmamış olabilir)
            if COOKIES_AVAILABLE and cookie_manager is not None:
                try:
                    cookie_manager.delete("finapp_remember_token", key="del_token_final")
                    cookie_manager.delete("finapp_remembered_email", key="del_email_final")
                    cookie_manager.delete("finapp_persistent_logins", key="del_logins_final")
                except:
                    pass
        elif COOKIES_AVAILABLE and cookie_manager is not None:
            # Cookie'den token kontrol et
            print("[AUTO_LOGIN_DEBUG] 🔍 Checking for remember-me token...")
            remember_token = cookie_manager.get("finapp_remember_token")
            print(f"[AUTO_LOGIN_DEBUG] Token exists: {bool(remember_token)}, Token length: {len(remember_token) if remember_token else 0}")
            
            if remember_token and not st.session_state.get('logged_in', False):
                print("[AUTO_LOGIN_DEBUG] 🔐 Attempting auto-login with token...")
                # Token'ı doğrula
                ip_address, user_agent = get_client_info()
                success, email, new_token, warning = validate_and_rotate_token(remember_token, ip_address, user_agent)
                print(f"[AUTO_LOGIN_DEBUG] Validation result - success={success}, email={email}, has_new_token={bool(new_token)}, warning={warning}")
                
                if success and email:
                    print(f"[AUTO_LOGIN_DEBUG] ✅ Auto-login successful for {email}")
                    # Otomatik giriş yap
                    st.session_state['logged_in'] = True
                    st.session_state['user_email'] = email
                    st.session_state['remembered_email'] = email
                    
                    # Yeni token'ı kaydet (rotation)
                    if new_token:
                        print("[AUTO_LOGIN_DEBUG] 🔄 Rotating token...")
                        set_remember_cookie(
                            "finapp_remember_token",
                            new_token,
                            datetime.now() + timedelta(days=30),
                            "set_token_rotate",
                        )
                        set_remember_cookie(
                            "finapp_remembered_email",
                            email,
                            datetime.now() + timedelta(days=30),
                            "set_email_rotate",
                        )
                    
                    if warning:
                        st.warning(f"⚠️ {warning}")
                    
                    # User name ayarla
                    if email == "erdalural@gmail.com":
                        st.session_state['user_name'] = "Erdal Ural (Test Kullanıcısı)"
                    else:
                        users = load_users()
                        if email in users:
                            st.session_state['user_name'] = users[email].get('name', email)
                    
                    st.success("✅ Otomatik giriş başarılı!")
                    st.rerun()
                else:
                    print(f"[AUTO_LOGIN_DEBUG] ❌ Auto-login failed - success={success}, email={email}")
                    # Token geçersiz - session state'den temizle
                    st.session_state['remembered_email'] = ""
        
        # Kaydedilen email'i yükle
        if 'remembered_email' not in st.session_state:
            if COOKIES_AVAILABLE and cookie_manager is not None:
                st.session_state['remembered_email'] = cookie_manager.get("finapp_remembered_email") or ""
            else:
                st.session_state['remembered_email'] = ""
        
        # Ana layout: Sol taraf giriş formu, sağ taraf abonelik bilgileri
        main_left, main_right = st.columns([1, 1])
        
        with main_left:
            st.subheader("👤 Mevcut Hesaba Giriş")
            
            # CSS - sadece butonları 1/3 boyutuna indir ve checkbox yazısını beyaz yap
            st.markdown("""
            <style>
            [data-testid="stForm"] button {
                max-width: 33.33% !important;
            }
            .stCheckbox label, .stCheckbox label p, .stCheckbox label span {
                color: #ffffff !important;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Form - nested columns kaldırıldı (Azure uyumluluğu için)
            with st.form("login_form"):
                email = st.text_input("📧 Email:", value=st.session_state.get('remembered_email', ''), key="login_email")
                # 🔐 ŞİFRE ASLA SAKLANMAZ - Güvenlik için her seferinde girilmeli
                password = st.text_input("🔒 Şifre:", type="password", key="login_password")
                remember_me = st.checkbox("Beni Hatırla", value=st.session_state.get('login_remember_me', False), key="login_remember_me")

                # Butonlar alt alta (nested columns Azure'da desteklenmiyor)
                login_submitted = st.form_submit_button("🚀 Giriş Yap", type="primary", use_container_width=True)
                forgot_password = st.form_submit_button("🔑 Şifremi Unuttum", use_container_width=True)

            if login_submitted:
                if email and password:
                    if authenticate_user(email, password):
                        print(f"[LOGIN_DEBUG] ✅ Authentication successful for {email}")
                        st.session_state['logged_in'] = True
                        st.session_state['user_email'] = email
                        
                        # 🔐 GÜVENLİ REMEMBER ME - Cookie Manager ile
                        if st.session_state.get('login_remember_me', False):
                            print("[LOGIN_DEBUG] 🔐 Remember Me checkbox is CHECKED, creating token...")
                            # Güvenli token oluştur (şifre ASLA saklanmaz!)
                            ip_address, user_agent = get_client_info()
                            print(f"[LOGIN_DEBUG] Client info - IP: {ip_address}, UA: {user_agent[:50]}...")
                            cookie_value = create_remember_me_token(email, ip_address, user_agent)
                            print(f"[LOGIN_DEBUG] Token created: {bool(cookie_value)}, length: {len(cookie_value) if cookie_value else 0}")
                            
                            if cookie_value and COOKIES_AVAILABLE and cookie_manager is not None:
                                print("[LOGIN_DEBUG] 💾 Saving remember-me cookies...")
                                # Pending login data'yı cookie'ye kaydet
                                pending = st.session_state.get('pending_login_data')
                                if pending:
                                    import base64
                                    user_id = pending['user_id']
                                    login_entry = pending['login_entry']
                                    
                                    # Mevcut logins'i yükle veya boş dict
                                    logins_json = cookie_manager.get("finapp_persistent_logins")
                                    if logins_json:
                                        try:
                                            logins = json.loads(base64.b64decode(logins_json.encode()).decode('utf-8'))
                                        except:
                                            logins = {}
                                    else:
                                        logins = {}
                                    
                                    # User için liste yoksa oluştur ve yeni kaydı ekle
                                    if user_id not in logins:
                                        logins[user_id] = []
                                    logins[user_id] = [login_entry]  # Tek kayıt tut (eski kayıtları sil)
                                    
                                    # Cookie'lere kaydet (extra-streamlit-components API)
                                    encoded_logins = base64.b64encode(json.dumps(logins).encode('utf-8')).decode()
                                    expires = datetime.now() + timedelta(days=30)
                                    set_remember_cookie(
                                        "finapp_persistent_logins",
                                        encoded_logins,
                                        expires,
                                        "set_logins_login",
                                    )
                                    set_remember_cookie(
                                        "finapp_remember_token",
                                        cookie_value,
                                        expires,
                                        "set_token_login",
                                    )
                                    set_remember_cookie(
                                        "finapp_remembered_email",
                                        email,
                                        expires,
                                        "set_email_login",
                                    )
                                    
                                    del st.session_state['pending_login_data']
                                
                                print("[LOGIN_DEBUG] ✅ All remember-me cookies saved successfully")
                                st.success("✅ Beni Hatırla aktif!")
                                save_remembered_credentials(email, "")
                        else:
                            print("[LOGIN_DEBUG] ⚠️ Remember Me checkbox is UNCHECKED, deleting cookies...")
                            # Seçili değilse, cookie'leri sil
                            if COOKIES_AVAILABLE and cookie_manager is not None:
                                cookie_manager.delete("finapp_remember_token", key="del_token_3")
                                cookie_manager.delete("finapp_remembered_email", key="del_email_3")
                                cookie_manager.delete("finapp_persistent_logins", key="del_logins_3")
                            clear_remembered_credentials()
                        
                        # Kullanıcı değiştiğinde önceki portföy önbelleğini ve ilgili state'leri temizle
                        for _k in [
                            'portfolio_initialized',
                            'portfolio_data',
                            'portfolio_data_hash',
                            'portfolio_values_cache',
                            'active_portfolio_tab',
                        ]:
                            if _k in st.session_state:
                                del st.session_state[_k]
                        
                        # TEST KULLANICISI için özel isim
                        if email == "erdalural@gmail.com":
                            st.session_state['user_name'] = "Erdal Ural (Test Kullanıcısı)"
                        else:
                            users = load_users()
                            st.session_state['user_name'] = users[email]['name']
                        
                        st.success("✅ Başarıyla giriş yaptınız!")
                        st.rerun()
                    else:
                        st.error("❌ Email veya şifre hatalı!")
                else:
                    st.error("❌ Lütfen tüm alanları doldurun!")
            
            if forgot_password:
                # Tab değiştirme isteğini flag olarak kaydet
                st.session_state['switch_to_reset'] = True
                st.rerun()
        
        # Sağ kolon - Abonelik bilgileri
        with main_right:
            show_subscription_info()
    
    elif selected_tab == "📝 Kayıt Ol":
        st.subheader("🆕 Yeni Hesap Oluştur")

        # CSS - input label'ları beyaz yap ve email alanını 1/5 boyutuna indir
        st.markdown("""
        <style>
        .stTextInput label, .stTextInput label p, .stTextInput label span {
            color: #ffffff !important;
        }
        
        /* Email alanını 1/8 oranında küçült ama içindeki metni büyült */
        div[data-testid="stTextInput"] input[type="text"] {
            font-size: 14px !important;
            padding: 8px 12px !important;
            min-height: 32px !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # Kayıt formu - email alanı (1/8 oranında küçük sütun)
        email_col, _ = st.columns([1, 7])
        with email_col:
            new_email = st.text_input("📧 Email:", key="register_email")

        # ============ DÖKÜMANLAR ONAY SEKSİYONU (E-POSTA DOĞRULAMASINDAN ÖNCE) ============
        st.markdown("---")
        st.subheader("📋 Dökümanları Onayla")
        st.info("Hizmetlerimizi kullanabilmek için aşağıdaki dökümanları okuyup onaylamanız gerekmektedir.")
        
        # Session state'de onay durumlarını kontrol et
        if 'doc_accepted_user_terms' not in st.session_state:
            st.session_state['doc_accepted_user_terms'] = False
        if 'doc_accepted_privacy' not in st.session_state:
            st.session_state['doc_accepted_privacy'] = False
        if 'doc_accepted_cookie' not in st.session_state:
            st.session_state['doc_accepted_cookie'] = False
        if 'show_user_terms_modal' not in st.session_state:
            st.session_state['show_user_terms_modal'] = False
        if 'show_privacy_modal' not in st.session_state:
            st.session_state['show_privacy_modal'] = False
        if 'show_cookie_modal' not in st.session_state:
            st.session_state['show_cookie_modal'] = False
        
        # CSS - checkbox yazısı ve döküman metinlerini beyaz yap
        st.markdown("""
        <style>
        .stCheckbox {
            color: #ffffff !important;
        }
        .stCheckbox label, .stCheckbox label p, .stCheckbox label span {
            color: #ffffff !important;
        }
        .stCheckbox label div {
            color: #ffffff !important;
        }
        .stExpander p, .stExpander li, .stExpander h1, .stExpander h2, .stExpander h3 {
            color: #ffffff !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Dökümanlar - tek seviye kolonlar (Azure uyumluluğu için)
        doc_col1, doc_col2, doc_col3 = st.columns([1, 1, 1])
        
        with doc_col1:
            st.session_state['doc_accepted_user_terms'] = st.checkbox(
                "Okudum, kabul ediyorum", 
                value=st.session_state['doc_accepted_user_terms'],
                key="check_user_terms"
            )
            if st.button("📄 Kullanıcı Aydınlatma Metni", key="btn_user_terms"):
                st.session_state['show_user_terms_modal'] = not st.session_state['show_user_terms_modal']
            
            if st.session_state['doc_accepted_user_terms']:
                st.success("✅ Kabul Edildi")
        
        with doc_col2:
            st.session_state['doc_accepted_privacy'] = st.checkbox(
                "Okudum, kabul ediyorum",
                value=st.session_state['doc_accepted_privacy'],
                key="check_privacy"
            )
            if st.button("🔒 Gizlilik Politikası", key="btn_privacy"):
                st.session_state['show_privacy_modal'] = not st.session_state['show_privacy_modal']
            
            if st.session_state['doc_accepted_privacy']:
                st.success("✅ Kabul Edildi")
        
        with doc_col3:
            st.session_state['doc_accepted_cookie'] = st.checkbox(
                "Okudum, kabul ediyorum",
                value=st.session_state['doc_accepted_cookie'],
                key="check_cookie"
            )
            if st.button("🍪 Elektronik İleti Politikası", key="btn_cookie"):
                st.session_state['show_cookie_modal'] = not st.session_state['show_cookie_modal']
            
            if st.session_state['doc_accepted_cookie']:
                st.success("✅ Kabul Edildi")
        
        # Modal ekranları göster (kolonların dışında)
        if st.session_state['show_user_terms_modal']:
            with st.expander("📄 Kullanıcı Aydınlatma Metni", expanded=True):
                doc_content = get_document("user_terms")
                st.markdown(f'<div style="color: white;">{doc_content}</div>', unsafe_allow_html=True)
        
        if st.session_state['show_privacy_modal']:
            with st.expander("🔒 Gizlilik Politikası", expanded=True):
                doc_content = get_document("privacy")
                st.markdown(f'<div style="color: white;">{doc_content}</div>', unsafe_allow_html=True)
        
        if st.session_state['show_cookie_modal']:
            with st.expander("🍪 Elektronik İleti Politikası", expanded=True):
                doc_content = get_document("cookie")
                st.markdown(f'<div style="color: white;">{doc_content}</div>', unsafe_allow_html=True)
        
        # Tüm dökümanların onaylanıp onaylanmadığını kontrol et
        all_docs_accepted = (st.session_state.get('doc_accepted_user_terms', False) and
                            st.session_state.get('doc_accepted_privacy', False) and
                            st.session_state.get('doc_accepted_cookie', False))
        
        # E-posta doğrulama durumunu kontrol et
        email_verified = False
        if new_email:
            email_verified = is_email_verified(new_email) or st.session_state.get(f"email_verified_{new_email}", False)
        
        st.markdown("---")
        
        # E-posta doğrulama bölümü - sadece tüm dökümanlar onaylandığında göster
        if not all_docs_accepted:
            st.warning("⚠️ Devam etmek için lütfen yukarıdaki tüm dökümanları onaylayın.")
        else:
            # Email girildiğinde doğrulama sürecini başlat (veya sekmeye girince butonu göster)
            if not email_verified:
                # Show instruction and the Kod Gönder button immediately (even if email empty)
                st.info("📧 E-posta adresinizi doğrulamanız gerekiyor. Kod Gönder'e basın ve e-posta adresinizi girin.")

                # Cooldown kontrolü (30 saniye)
                cooldown_key = f"email_cooldown_{new_email}" if new_email else "email_cooldown_empty"
                last_sent_time = st.session_state.get(cooldown_key, None)
                cooldown_remaining = 0
                
                if last_sent_time:
                    elapsed = (datetime.now() - last_sent_time).total_seconds()
                    if elapsed < 30:
                        cooldown_remaining = int(30 - elapsed)
                
                # Buton devre dışı mı kontrol et
                button_disabled = cooldown_remaining > 0
                button_label = f"⏳ Bekleyin ({cooldown_remaining}s)" if button_disabled else "📨 Kod Gönder"
                
                if st.button(button_label, type="primary", key="send_code", disabled=button_disabled):
                    if new_email:
                        # E-posta format kontrolü
                        if "@" in new_email and "." in new_email.split("@")[1]:
                            # Email zaten kayıtlı mı kontrol et
                            users = load_users()
                            if new_email.lower() in [e.lower() for e in users.keys()]:
                                st.error("❌ Bu e-posta adresi zaten kayıtlı! Lütfen giriş yapın veya farklı bir e-posta kullanın.")
                            else:
                                verification_code = generate_verification_code()
                                # Kodu session state'e kaydet
                                store_verification_code(new_email, verification_code)
                                # Email göndermeyi dene
                                success, message = send_verification_email(new_email, verification_code)
                                # Kod gönderildi olarak işaretle
                                st.session_state[f"code_sent_{new_email}"] = True
                                # Cooldown zamanını kaydet
                                st.session_state[f"email_cooldown_{new_email}"] = datetime.now()
                                st.rerun()
                        else:
                            st.error("❌ Geçerli bir e-posta adresi girin!")
                    else:
                        st.error("❗ Lütfen önce e-posta adresinizi girin, sonra 'Kod Gönder' butonuna basın.")

                # If a code was previously sent to this email, show verification input
                if new_email and st.session_state.get(f"code_sent_{new_email}", False):
                    st.success("📧 Doğrulama kodu e-posta adresinize gönderildi!")
                    # Show a compact input for the 6-digit verification code (1/10 width)
                    col_code, col_spacer = st.columns([1, 9])
                    with col_code:
                        verification_input = st.text_input(
                            "🔑 E-postanıza gelen 6 haneli kodu girin:",
                            max_chars=6,
                            key="verification_code",
                            placeholder="123456",
                            help="Lütfen e-postanıza gelen 6 haneli doğrulama kodunu girin"
                        )

                    # Doğrulama butonları - nested columns kaldırıldı (Azure uyumluluğu için)
                    if st.button("✅ Doğrula", type="primary", key="verify_code"):
                        # Ensure a 6-digit numeric code is entered
                        if not verification_input:
                            st.error("❌ Lütfen doğrulama kodunu girin!")
                        elif len(verification_input) != 6 or not verification_input.isdigit():
                            st.error("❌ Doğrulama kodu 6 haneli sayısal olmalıdır!")
                        else:
                            success, message = verify_code(new_email, verification_input)
                            if success:
                                # Kod gönderildi state'ini temizle and mark verified
                                st.session_state.pop(f"code_sent_{new_email}", None)
                                st.session_state[f"email_verified_{new_email}"] = True
                                st.success(f"✅ {message}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")

            else:
                st.success("✅ E-posta adresiniz doğrulandı!")

        # Şifre alanları (sadece e-posta doğrulandığında göster)
        if email_verified:
            
            # Hesap başarıyla oluşturulduysa, button gösterme
            if st.session_state.get('account_created_success', False):
                # Başarılı kayıt mesajını göster ama button gösterme
                st.success("✅ Hesabınız başarıyla oluşturuldu!")
                st.info("🔑 Lütfen giriş yapma sekmesinde hesabınız ile giriş yapın.")
            else:
                # CSS - Şifre alanlarını ve butonunu küçült
                st.markdown("""
                <style>
                /* Şifre alanlarını 5'de 1 oranında küçült */
                div[data-testid="stTextInput"] input[type="password"] {
                    font-size: 11.2px !important;  /* 14px * 0.8 */
                    padding: 2.8px 6px !important;
                    min-height: 28px !important;
                }
                
                div[data-testid="stTextInput"] label {
                    font-size: 11.2px !important;  /* 14px * 0.8 */
                    margin-bottom: 4px !important;
                }
                
                /* Hesap Oluştur butonunu küçült */
                div[data-testid="stButton"] button[key="create_account_button"] {
                    font-size: 10px !important;
                    padding: 4px 8px !important;
                    min-height: 24px !important;
                }
                </style>
                """, unsafe_allow_html=True)
                
                # Şifre alanları
                st.info("🔐 **Güçlü Şifre Oluşturun:** En az 8 karakter, 1 rakam ve 1 özel karakter (!@#$%&*) içermelidir.")
                
                # Şifre alanlarını 5'de 1 oranında küçültmek için kolona yerleştir
                pwd_col, _ = st.columns([1, 4])
                with pwd_col:
                    new_password = st.text_input("🔒 Şifre:", type="password", key="register_password")
                
                confirm_col, _ = st.columns([1, 4])
                with confirm_col:
                    confirm_password = st.text_input("🔒 Şifre Tekrar:", type="password", key="confirm_password")
                
                # Hata mesajları için placeholder oluştur
                error_placeholder = st.empty()

                # Hesap Oluştur butonunu 1/8 oranında küçültmek için kolona yerleştir
                btn_col, _ = st.columns([1, 7])
                with btn_col:
                    button_clicked = st.button("📝 Hesap Oluştur", type="primary", use_container_width=True, key="create_account_button")
                
                if button_clicked:
                    print(f"[DEBUG] Button clicked - email={new_email}, pwd_len={len(new_password) if new_password else 0}, confirm_len={len(confirm_password) if confirm_password else 0}")
                    
                    # Dökümanları kontrol et
                    all_docs_accepted = (st.session_state.get('doc_accepted_user_terms', False) and
                                        st.session_state.get('doc_accepted_privacy', False) and
                                        st.session_state.get('doc_accepted_cookie', False))
                    
                    if not all_docs_accepted:
                        with error_placeholder.container():
                            st.error("❌ Lütfen tüm dökümanları okuyup onaylayın!")
                    elif not new_email or not new_password or not confirm_password:
                        with error_placeholder.container():
                            st.error("❌ Lütfen tüm alanları doldurun!")
                    elif new_email and new_password and confirm_password:
                        if new_password == confirm_password:
                            # Password policy: min 8 chars, at least one digit, at least one special char
                            has_min_len = len(new_password) >= 8
                            has_digit = any(ch.isdigit() for ch in new_password)
                            has_special = any(not ch.isalnum() for ch in new_password)
                            
                            print(f"[DEBUG] Password checks - len={len(new_password)}, has_min_len={has_min_len}, has_digit={has_digit}, has_special={has_special}")

                            if not has_min_len:
                                with error_placeholder.container():
                                    st.error("❌ **Şifre Çok Kısa!**")
                                    st.info("💡 Şifreniz en az **8 karakter** uzunluğunda olmalıdır. Örnek: `Guvenli123!`")
                            elif not has_digit:
                                with error_placeholder.container():
                                    st.error("❌ **Şifrede Rakam Yok!**")
                                    st.info("💡 Şifreniz en az **bir rakam (0-9)** içermelidir. Örnek: `Guvenli123!`")
                            elif not has_special:
                                with error_placeholder.container():
                                    st.error("❌ **Şifrede Özel Karakter Yok!**")
                                    st.info("💡 Şifreniz en az **bir özel karakter** içermelidir (örn. `!@#$%&*`). Örnek: `Guvenli123!`")
                            else:
                                # Onaylanan dökümanları kaydet
                                accepted_docs = {
                                    'user_terms': st.session_state.get('doc_accepted_user_terms', False),
                                    'privacy_policy': st.session_state.get('doc_accepted_privacy', False),
                                    'cookie_policy': st.session_state.get('doc_accepted_cookie', False),
                                    'accepted_at': datetime.now().isoformat()
                                }
                                
                                success, message = register_user(new_email, new_password, "", accepted_docs)
                                print(f"[REGISTER RESULT] email={new_email}, success={success}, message={message}")
                                if success:
                                    # Yeni kullanıcıya 1 aylık ücretsiz deneme aboneliği tanımla
                                    try:
                                        subscriptions = load_subscriptions()
                                        start_date = datetime.now()
                                        end_date = start_date + timedelta(days=TRIAL_PERIOD_DAYS)
                                        subscriptions[new_email.lower()] = {
                                            "plan": "trial",
                                            "plan_name": "Deneme (Ücretsiz)",
                                            "start_date": start_date.strftime("%Y-%m-%d"),
                                            "end_date": end_date.strftime("%Y-%m-%d"),
                                            "status": "active",
                                            "is_trial": True,
                                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "created_by": "system_auto_trial"
                                        }
                                        save_subscriptions(subscriptions)
                                        print(f"[TRIAL SUBSCRIPTION] 30 gün deneme tanımlandı: {new_email}")
                                    except Exception as e:
                                        print(f"[TRIAL ERROR] Deneme aboneliği tanımlanamadı: {e}")
                                    
                                    with error_placeholder.container():
                                        st.success("✅ Hesabınız başarıyla oluşturuldu!")
                                        st.info("🔑 Lütfen giriş yapma sekmesinde hesabınız ile giriş yapın.")
                                    
                                    # Button'u gizle
                                    st.session_state['account_created_success'] = True
                                    
                                    # Temizlik
                                    st.session_state.pop(f"email_verified_{new_email}", None)
                                    if f"code_sent_{new_email}" in st.session_state:
                                        st.session_state.pop(f"code_sent_{new_email}")
                                    # Döküman onay state'lerini temizle
                                    st.session_state.pop('doc_accepted_user_terms', None)
                                    st.session_state.pop('doc_accepted_privacy', None)
                                    st.session_state.pop('doc_accepted_cookie', None)
                                    
                                    # Giriş yap sekmesine yönlendirme flag'i
                                    st.session_state['redirect_to_login'] = True
                                    
                                    # Sayfayı yenile - button kaldırılsın
                                    st.rerun()
                                else:
                                    with error_placeholder.container():
                                        st.error(f"❌ {message}")
                        else:
                            with error_placeholder.container():
                                st.error("❌ Şifreler eşleşmiyor!")
                    else:
                        with error_placeholder.container():
                            st.error("❌ Lütfen tüm alanları doldurun!")
    
    elif selected_tab == "🔄 Şifre Sıfırla":
        show_password_reset_form()

# Şifre sıfırlama fonksiyonu
def show_password_reset_form():
    """Şifre sıfırlama formunu göster"""
    st.subheader("🔄 Şifre Sıfırlama")
    
    # Şifre sıfırlama aşaması kontrolü
    if 'reset_step' not in st.session_state:
        st.session_state['reset_step'] = 1
    
    if st.session_state['reset_step'] == 1:
        # 1. Aşama: E-posta girişi ve doğrulama kodu gönderme
        st.markdown("""<p style="color: white; font-weight: bold;">Adım 1: E-posta adresinizi girin</p>""", unsafe_allow_html=True)
        
        with st.form("reset_email_form"):
            left_col, right_col = st.columns([2, 3])
            with left_col:
                reset_email = st.text_input("📧 Kayıtlı E-posta Adresiniz:", key="reset_email_form_input")

                # Place submit button under the input and left-aligned
                if st.form_submit_button("📨 Doğrulama Kodu Gönder", type="primary"):
                    submit_button = True
                else:
                    submit_button = False
            
            if submit_button:
                if reset_email:
                    try:
                        # Kullanıcının kayıtlı olup olmadığını kontrol et
                        users = load_users()
                        if reset_email in users:
                            verification_code = generate_verification_code()
                            success, message = send_verification_email(reset_email, verification_code)
                            if success:
                                store_verification_code(reset_email, verification_code)
                                st.session_state['reset_email'] = reset_email
                                st.session_state['reset_step'] = 2
                                st.success("✅ Şifre sıfırlama kodu e-posta adresinize gönderildi!")
                                st.rerun()
                            else:
                                st.error(f"❌ E-posta gönderilirken hata: {message}")
                        else:
                            st.error("❌ Bu e-posta adresi sisteme kayıtlı değil!")
                    except Exception as e:
                        st.error(f"❌ Hata oluştu: {str(e)}")
                        st.error(f"❌ Hata türü: {type(e).__name__}")
                else:
                    st.error("❌ Lütfen e-posta adresinizi girin!")
    
    elif st.session_state['reset_step'] == 2:
        # 2. Aşama: Doğrulama kodu girişi
        st.markdown('<p style="color: white; font-weight: bold;">Adım 2: E-postanıza gelen doğrulama kodunu girin</p>', unsafe_allow_html=True)
        st.info(f"📧 Kod gönderildi: {st.session_state['reset_email']}")
        
        with st.form("verification_code_form"):
            verification_code = st.text_input("🔢 Doğrulama Kodu:", key="reset_verification_code_form", max_chars=6)
            
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                verify_button = st.form_submit_button("✅ Kodu Doğrula", type="primary")
            
            if verify_button:
                if verification_code:
                    try:
                        is_valid, message = verify_code(st.session_state['reset_email'], verification_code)
                        if is_valid:
                            st.session_state['reset_step'] = 3
                            st.success("✅ Doğrulama başarılı! Yeni şifrenizi belirleyin.")
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                    except Exception as e:
                        st.error(f"❌ Doğrulama hatası: {str(e)}")
                else:
                    st.error("❌ Lütfen doğrulama kodunu girin!")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col3:
            if st.button("🔙 Geri", key="back_to_step1"):
                st.session_state['reset_step'] = 1
                st.rerun()
    
    elif st.session_state['reset_step'] == 3:
        # 3. Aşama: Yeni şifre belirleme
        st.markdown('<p style="color: white; font-weight: bold;">Adım 3: Yeni şifrenizi belirleyin</p>', unsafe_allow_html=True)
        st.info(f"👤 Kullanıcı: {st.session_state['reset_email']}")
        
        with st.form("new_password_form"):
            new_password = st.text_input("🔒 Yeni Şifre:", type="password", key="new_password_form")
            confirm_password = st.text_input("🔒 Yeni Şifre (Tekrar):", type="password", key="confirm_new_password_form")
            
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                save_button = st.form_submit_button("💾 Şifreyi Kaydet", type="primary")
            
            if save_button:
                if new_password and confirm_password:
                    if new_password == confirm_password:
                        # Password policy for reset: min 8 chars, at least one digit, at least one special char
                        has_min_len = len(new_password) >= 8
                        has_digit = any(ch.isdigit() for ch in new_password)
                        has_special = any(not ch.isalnum() for ch in new_password)

                        if not has_min_len:
                            st.error("❌ Şifre en az 8 karakter olmalıdır!")
                        elif not has_digit:
                            st.error("❌ Şifre en az bir rakam içermelidir!")
                        elif not has_special:
                            st.error("❌ Şifre en az bir özel karakter (örn. !@#%&) içermelidir!")
                        else:
                            try:
                                # Şifreyi güncelle
                                users = load_users()
                                if st.session_state['reset_email'] in users:
                                    users[st.session_state['reset_email']]['password'] = hash_password(new_password)
                                    users[st.session_state['reset_email']]['password_updated_at'] = datetime.now().isoformat()
                                    save_users(users)
                                    
                                    # Session state temizle
                                    reset_email_temp = st.session_state.get('reset_email', '')
                                    st.session_state['reset_step'] = 1
                                    if 'reset_email' in st.session_state:
                                        del st.session_state['reset_email']
                                    if 'verification_codes' in st.session_state and reset_email_temp in st.session_state['verification_codes']:
                                        del st.session_state['verification_codes'][reset_email_temp]
                                    
                                    st.success("✅ Şifreniz başarıyla güncellendi! Artık yeni şifrenizle giriş yapabilirsiniz.")
                                    st.balloons()
                                    time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error("❌ Kullanıcı bulunamadı!")
                            except Exception as e:
                                st.error(f"❌ Şifre güncelleme hatası: {str(e)}")
                    else:
                        st.error("❌ Şifreler eşleşmiyor!")
                else:
                    st.error("❌ Lütfen tüm alanları doldurun!")
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col3:
            if st.button("🔙 Geri", key="back_to_step2"):
                st.session_state['reset_step'] = 2
                st.rerun()

# Portföy işlemleri
def add_transaction(user_email, transaction_type, instrument_code, instrument_name, category, quantity, price, currency, date):
    """Portföye işlem ekle"""
    portfolios = load_portfolios()
    
    if user_email not in portfolios:
        portfolios[user_email] = {'transactions': []}

    transaction = {
        'id': len(portfolios[user_email]['transactions']) + 1,
        'type': transaction_type,  # 'BUY' veya 'SELL'
        'instrument_code': instrument_code,
        'instrument_name': instrument_name,
        'category': category,
        'quantity': float(quantity),
        'price': float(price),
        'currency': currency,
        'date': date.strftime('%d/%m/%Y'),
        'total_value': float(quantity) * float(price),
        'created_at': datetime.now().isoformat()
    }
    
    portfolios[user_email]['transactions'].append(transaction)
    
    save_portfolios(portfolios)
    return True

def delete_all_transactions(user_email):
    """Kullanıcının tüm işlem geçmişini sil"""
    try:
        portfolios = load_portfolios()
        
        if user_email in portfolios:
            portfolios[user_email]['transactions'] = []
            save_portfolios(portfolios)
            return True
        return False
    except Exception as e:
        st.error(f"Hata: {str(e)}")
        return False

def delete_transactions_by_ids(user_email, transaction_ids):
    """Belirtilen ID'lerdeki işlemleri sil"""
    try:
        portfolios = load_portfolios()
        
        if user_email in portfolios:
            # ID'lere göre işlemleri filtrele (silinecekleri hariç tut)
            remaining_transactions = []
            for trans in portfolios[user_email]['transactions']:
                if trans.get('id') not in transaction_ids:
                    remaining_transactions.append(trans)
            
            portfolios[user_email]['transactions'] = remaining_transactions
            
            # ID'leri yeniden düzenle
            for i, trans in enumerate(portfolios[user_email]['transactions']):
                trans['id'] = i + 1
            
            save_portfolios(portfolios)
            return True
        return False
    except Exception as e:
        st.error(f"Hata: {str(e)}")
        return False

@st.cache_data(ttl=120)  # 2 dakika cache
@st.cache_data(ttl=60, show_spinner=False)  # Cache, ama target_currency değişince yenile
def get_portfolio_summary(user_email, target_currency="₺"):
    """Kullanıcının portföy özetini al"""
    portfolios = load_portfolios()
    
    if user_email not in portfolios:
        return [], 0, {}
    
    transactions = portfolios[user_email]['transactions']
    
    # Enstrüman bazında pozisyonları hesapla
    positions = {}
    
    for trans in transactions:
        code = trans['instrument_code']
        
        if code not in positions:
            positions[code] = {
                'instrument_name': trans['instrument_name'],
                'category': trans['category'],
                'currency': trans['currency'],
                'total_quantity': 0,
                'total_cost': 0,
                'total_cost_target_currency': 0,  # Hedef para biriminde toplam maliyet
                'transactions_count': 0,
                'transaction_details': []  # İşlem detayları
            }
        
        # İşlem tarihindeki kur ile hedef para birimine çevir
        # Her iki tarih formatını da destekle (DD/MM/YYYY ve YYYY-MM-DD)
        try:
            transaction_date = datetime.strptime(trans['date'], '%d/%m/%Y').date()
        except:
            try:
                transaction_date = datetime.strptime(trans['date'], '%Y-%m-%d').date()
            except:
                transaction_date = datetime.now().date()
        
        rate_on_transaction_date = get_currency_rate(trans['currency'], target_currency, transaction_date)
        total_value_in_target_currency = trans['total_value'] * rate_on_transaction_date
        
        # DEBUG: Transaction kur çevrimi
        logging.info(f"Transaction conversion: {code} on {trans['date']} | {trans['currency']} → {target_currency} | Rate: {rate_on_transaction_date} | {trans['total_value']} → {total_value_in_target_currency}")
        
        if trans['type'] == 'BUY':
            positions[code]['total_quantity'] += trans['quantity']
            positions[code]['total_cost'] += trans['total_value']
            positions[code]['total_cost_target_currency'] += total_value_in_target_currency
        else:  # SELL
            positions[code]['total_quantity'] -= trans['quantity']
            positions[code]['total_cost'] -= trans['total_value']
            positions[code]['total_cost_target_currency'] -= total_value_in_target_currency
        
        positions[code]['transactions_count'] += 1
        
        # İşlem detayını kaydet
        positions[code]['transaction_details'].append({
            'date': trans['date'],
            'type': trans['type'],
            'quantity': trans['quantity'],
            'price': trans['price'],
            'currency': trans['currency'],
            'total_value': trans['total_value'],
            'rate_used': rate_on_transaction_date,
            'total_value_target_currency': total_value_in_target_currency
        })
    
    # Güncel fiyatlarla değerlendirme
    portfolio_summary = []
    total_portfolio_value = 0
    
    for code, pos in positions.items():
        if pos['total_quantity'] > 0:  # Sadece pozitif pozisyonları göster
            # Güncel fiyat al
            current_price = get_current_price(code, pos['category'])
            current_value = pos['total_quantity'] * current_price
            
            # Güncel değeri hedef para birimine çevir (güncel kur ile)
            # Pozisyonda saklanan para birimini kullan (kullanıcının işlemdeki para birimi)
            current_currency = pos['currency']
            current_rate = get_currency_rate(current_currency, target_currency)
            current_value_converted = current_value * current_rate
            
            # DEBUG: Kur çevrimi kontrolü
            logging.info(f"Portfolio conversion: {code} | {current_currency} → {target_currency} | Rate: {current_rate} | Value: {current_value} → {current_value_converted}")
            
            # Ortalama maliyet hesapla (hedef para biriminde)
            avg_cost_target_currency = pos['total_cost_target_currency'] / pos['total_quantity'] if pos['total_quantity'] > 0 else 0
            
            # Kar/Zarar hesaplama (hedef para biriminde)
            profit_loss = current_value_converted - pos['total_cost_target_currency']
            profit_loss_percent = (profit_loss / pos['total_cost_target_currency']) * 100 if pos['total_cost_target_currency'] > 0 else 0
            
            # Ortalama maliyet fiyatını orijinal para biriminde de göster
            avg_cost_original = pos['total_cost'] / pos['total_quantity'] if pos['total_quantity'] > 0 else 0
            
            portfolio_summary.append({
                'Kod': code,
                'Adı': pos['instrument_name'],
                'Kategori': pos['category'],
                'Miktar': pos['total_quantity'],
                'Ort. Maliyet': avg_cost_original,
                'Ort. Maliyet (Hedef)': avg_cost_target_currency,
                'Güncel Fiyat': current_price,
                'Güncel Değer': current_value_converted,
                'Toplam Maliyet': pos['total_cost_target_currency'],  # Hedef para biriminde
                'Kar/Zarar': profit_loss,
                'Kar/Zarar %': profit_loss_percent,
                'Para Birimi': target_currency,
                'İşlem Detayları': pos['transaction_details']  # Detaylı analiz için
            })
            
            total_portfolio_value += current_value_converted
    
    return portfolio_summary, total_portfolio_value, positions

@st.cache_data(ttl=60)  # 1 dakika cache - hisse fiyatları için
def get_current_price(instrument_code, category):
    """Enstrümanın güncel fiyatını al"""
    try:
        current_categories = get_instrument_categories()
        category_info = current_categories.get(category, {})
        suffix = category_info.get("suffix", "")
        
        # Nakit para birimleri için özel işleme (1.0 fiyat, ama currency conversion gerekli)
        # Not: Kur çevrimi get_portfolio_summary içinde yapılacak
        if category == "CASH":
            return 1.0  # Nakit her zaman 1 birim = 1 değer (kur çevrimi ayrıca yapılır)
        
        # TEFAS fonları için özel işleme
        if category == "TEFAS":
            try:
                # Önce Parquet'ten en son fiyatı almaya çalış
                parquet_data = get_tefas_latest_price_from_parquet(instrument_code)
                
                if parquet_data and parquet_data.get('price', 0) > 0:
                    # Parquet'te veri varsa onu kullan
                    return parquet_data['price']
                
                # Parquet'te veri yoksa Excel'den dene (geriye uyumluluk)
                excel_data = get_tefas_latest_price_from_excel(instrument_code)
                
                if excel_data and excel_data['price'] > 0:
                    # Excel'de veri varsa onu kullan
                    return excel_data['price']
                
                # Parquet ve Excel'de veri yoksa - API çağrısı YAPMA, sıfır döndür
                logging.warning(f"TEFAS fon fiyatı bulunamadı: {instrument_code}")
                return 0
                        
            except Exception as e:
                logging.error(f"TEFAS fon fiyatı alınırken hata ({instrument_code}): {str(e)}")
                return 0
        
        # Kripto paralar için özel işleme
        if category == "CRYPTO":
            # TRY çiftleri için Türk kripto borsalarını kullan
            if "-TRY" in instrument_code:
                try:
                    # Önce Binance TR API'sini dene
                    crypto_symbol = instrument_code.replace("-", "").upper()  # BTC-TRY -> BTCTRY
                    
                    # Binance TR API
                    binance_url = f"https://api.binance.com/api/v3/ticker/price?symbol={crypto_symbol}"
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    
                    response = requests.get(binance_url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        price = float(data['price'])
                        return price
                    
                    # Binance başarısızsa Paribu API'sini dene
                    crypto_pair = instrument_code.replace("-", "_").lower()  # BTC-TRY -> btc_try
                    paribu_url = f"https://v3.paribu.com/app/markets/{crypto_pair}"
                    
                    response = requests.get(paribu_url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'last' in data:
                            price = float(data['last'])
                            return price
                    
                    # İkisi de başarısızsa USD üzerinden hesaplama (session ile)
                    usd_symbol = instrument_code.replace("-TRY", "-USD")
                    usd_ticker = yf.Ticker(usd_symbol, session=YF_SESSION)
                    usd_hist = usd_ticker.history(period="1d")
                    
                    if not usd_hist.empty:
                        # USD/TRY kurunu al (session ile)
                        usdtry_ticker = yf.Ticker("USDTRY=X", session=YF_SESSION)
                        usdtry_hist = usdtry_ticker.history(period="1d")
                        
                        if not usdtry_hist.empty:
                            usd_price = usd_hist['Close'].iloc[-1]
                            usd_try_rate = usdtry_hist['Close'].iloc[-1]
                            try_price = usd_price * usd_try_rate
                            return try_price
                    
                    # Hiçbiri işe yaramazsa 0 döndür
                    return 0
                    
                except Exception as e:
                    return 0
            
            # USD ve diğer çiftler için normal Yahoo Finance (session ile)
            else:
                try:
                    ticker = yf.Ticker(instrument_code, session=YF_SESSION)
                    hist = ticker.history(period="1d")
                    if not hist.empty:
                        return hist['Close'].iloc[-1]
                    return 0
                except Exception as e:
                    return 0
        
        # Türk altını için özel hesaplama
        if instrument_code in TURKISH_GOLD_INSTRUMENTS:
            turkish_gold_data = get_turkish_gold_prices()
            if instrument_code in turkish_gold_data:
                return turkish_gold_data[instrument_code]['price']
            return 0
        
        # Hisse senetleri için Yahoo Finance'den çek
        # BIST, NASDAQ ve diğer borsalar için
        ticker_symbol = f"{instrument_code}{suffix}"
        
        try:
            # Session ile yfinance Ticker oluştur (Azure uyumlu)
            ticker = yf.Ticker(ticker_symbol, session=YF_SESSION)
            
            # Önce 1 günlük veriyi dene
            hist = ticker.history(period="1d")
            if not hist.empty and 'Close' in hist.columns:
                price = hist['Close'].iloc[-1]
                if price > 0:
                    return float(price)
            
            # 1 günlük veri yoksa 5 günlük geçmişe bak
            hist = ticker.history(period="5d")
            if not hist.empty and 'Close' in hist.columns:
                price = hist['Close'].iloc[-1]
                if price > 0:
                    return float(price)
            
            # Son çare olarak ticker.info'dan fiyat al
            info = ticker.info
            if info:
                if 'regularMarketPrice' in info and info['regularMarketPrice']:
                    return float(info['regularMarketPrice'])
                elif 'currentPrice' in info and info['currentPrice']:
                    return float(info['currentPrice'])
                elif 'previousClose' in info and info['previousClose']:
                    return float(info['previousClose'])
            
            return 0
            
        except Exception as e:
            # Hata durumunda detaylı log
            print(f"Yahoo Finance hatası ({ticker_symbol}): {str(e)}")
            return 0
        
    except Exception as e:
        print(f"get_current_price genel hatası ({instrument_code}, {category}): {str(e)}")
        return 0


def validate_price_for_date(instrument_code, category, target_date, user_price):
    """Belirli tarih için fiyat validasyonu yap"""
    try:
        from datetime import datetime, timedelta
        
        # Bugün ise geçerli
        today = datetime.now().date()
        if target_date == today:
            return {
                "is_valid": True,
                "suggested_price": None,
                "error_message": ""
            }
        
        # Nakit para birimleri için validasyon (her zaman geçerli)
        if category == "CASH":
            return {
                "is_valid": True,
                "suggested_price": 1.0,
                "error_message": ""
            }
        
        # TEFAS fonları için özel validasyon
        if category == "TEFAS":
            # Sadece Parquet'ten fiyatı al - API çağrısı yapma
            try:
                # target_date date objesi ise datetime'a çevir
                if hasattr(target_date, 'date'):
                    # Zaten datetime objesi
                    target_datetime = target_date
                else:
                    # date objesi ise datetime'a çevir
                    from datetime import datetime
                    target_datetime = datetime.combine(target_date, datetime.min.time())
                
                price_data = get_tefas_price_from_parquet(instrument_code, target_datetime)
                
                if price_data and price_data.get('price', 0) > 0:
                    suggested_price = price_data['price']
                    return {
                        "is_valid": True,
                        "suggested_price": suggested_price,
                        "error_message": ""
                    }
                else:
                    # Fiyat bulunamadı ama manuel girişe izin ver
                    return {
                        "is_valid": True,
                        "suggested_price": None,
                        "error_message": f"⚠️ {instrument_code} fonu için {target_date} tarihinde fiyat bilgisi bulunamadı. Manuel fiyat girişi yapılıyor."
                    }
            except Exception as e:
                return {
                    "is_valid": False,
                    "suggested_price": None,
                    "error_message": f"🚫 {instrument_code} fonu için fiyat kontrolü başarısız: {str(e)}"
                }
        
        # Diğer enstrümanlar için geçmiş fiyat kontrolü
        historical_price = get_historical_price(instrument_code, category, target_date)
        
        if historical_price > 0:
            return {
                "is_valid": True,
                "suggested_price": historical_price,
                "error_message": ""
            }
        else:
            # Hafta sonu kontrolü
            weekday = target_date.weekday()
            if weekday >= 5:  # Cumartesi (5) veya Pazar (6)
                return {
                    "is_valid": False,
                    "suggested_price": None,
                    "error_message": f"🚫 {target_date} hafta sonu (piyasa kapalı). Lütfen hafta içi bir tarih seçin."
                }
            else:
                return {
                    "is_valid": False,
                    "suggested_price": None,
                    "error_message": f"🚫 {instrument_code} için {target_date} tarihinde fiyat bilgisi bulunamadı!"
                }
        
    except Exception as e:
        return {
            "is_valid": False,
            "suggested_price": None,
            "error_message": f"🚫 Fiyat validasyonu sırasında hata: {str(e)}"
        }

def get_historical_price(instrument_code, category, date):
    """Belirli tarihteki enstrüman fiyatını al"""
    try:
        current_categories = get_instrument_categories()
        category_info = current_categories.get(category, {})
        suffix = category_info.get("suffix", "")
        
        # TEFAS fonları için - geçmiş veri
        if category == "TEFAS":
            try:
                # date objesi ise datetime'a çevir
                if hasattr(date, 'date'):
                    # Zaten datetime objesi
                    target_datetime = date
                else:
                    # date objesi ise datetime'a çevir
                    from datetime import datetime
                    target_datetime = datetime.combine(date, datetime.min.time())
                
                # Önce Parquet'ten belirtilen tarih için fiyatı almaya çalış
                parquet_data = get_tefas_price_from_parquet(instrument_code, target_datetime)
                
                if parquet_data and parquet_data.get('price', 0) > 0:
                    # Parquet'te veri varsa onu kullan - SESSİZ MOD
                    return parquet_data['price']
                
                # Parquet'te veri yoksa Excel'den dene (geriye uyumluluk)
                excel_data = get_tefas_price_from_excel(instrument_code, date)
                
                if excel_data and excel_data['price'] > 0:
                    # Excel'de veri varsa onu kullan - SESSİZ MOD
                    return excel_data['price']
                
                # Parquet ve Excel'de veri yoksa - SESSİZ MOD, 0 döndür
                return 0
                        
            except Exception as e:
                # Hata durumunda sessizce 0 döndür
                return 0
        
        # Türk altını için özel hesaplama
        if instrument_code in TURKISH_GOLD_INSTRUMENTS:
            try:
                # Önce tarihsel Turkish Gold Parquet verisinden almaya çalış
                target_date = date if hasattr(date, 'date') else date
                if hasattr(target_date, 'date'):
                    target_date = target_date.date()
                
                # TurkishGoldDataManager'dan tarihsel veri al
                turkish_gold_dm = TurkishGoldDataManager()
                historical_data = turkish_gold_dm.get_historical_data(start_date=target_date, end_date=target_date)
                
                if not historical_data.empty:
                    # Belirtilen tarih için veri var mı kontrol et
                    row = historical_data[historical_data['Instrument_Code'] == instrument_code]
                    if not row.empty:
                        return float(row.iloc[0]['Price'])
                
                # Parquet'te yoksa Yahoo Finance'den USD/TRY ve altın fiyatını al
                # date parametresi date objesi olmayabilir, kontrol et
                if not hasattr(date, 'year'):
                    date = datetime.strptime(date, '%d/%m/%Y').date() if isinstance(date, str) else date
                
                usdtry = yf.Ticker("USDTRY=X", session=YF_SESSION)
                start_date = date - timedelta(days=7)  # 7 gün öncesinden başla
                end_date = date + timedelta(days=1)
                usdtry_hist = usdtry.history(start=start_date, end=end_date)
                
                # O tarihteki altın fiyatını al
                gold_usd = yf.Ticker("GC=F", session=YF_SESSION)

                gold_hist = gold_usd.history(start=start_date, end=end_date)
                
                if not usdtry_hist.empty and not gold_hist.empty:
                    # En yakın tarihteki kurları al
                    usd_try_rate = usdtry_hist['Close'].iloc[-1]
                    gold_usd_price = gold_hist['Close'].iloc[-1]
                    
                    # Hesaplamaları yap
                    gold_try_ons = gold_usd_price * usd_try_rate
                    gold_try_gram = gold_try_ons / 31.1035
                    
                    if instrument_code == "ALTIN_GRAM":
                        return gold_try_gram
                    elif instrument_code == "ALTIN_CEYREK":
                        return gold_try_gram * 1.75
                    elif instrument_code == "ALTIN_YARIM":
                        return gold_try_gram * 3.61
                    elif instrument_code == "ALTIN_TAM":
                        return gold_try_gram * 7.216
                    elif instrument_code == "ALTIN_ONS_TRY":
                        return gold_try_ons
                    elif instrument_code == "ALTIN_RESAT":
                        return gold_try_gram * 7.216  # Tam altın ağırlığı (22 ayar)
                    elif instrument_code == "ALTIN_CUMHURIYET":
                        return gold_try_gram * 7.216  # Tam altın ağırlığı (22 ayar)
            except:
                pass
            return 0
        
        # Normal enstrümanlar için Yahoo Finance
        ticker_symbol = f"{instrument_code}{suffix}"
        
        # Belirli tarih için veri al - tarihleri önce tanımla
        start_date = date - timedelta(days=30)  # 30 gün öncesinden başla (hafta sonları için)
        end_date = date + timedelta(days=1)
        
        # Kripto paralar için özel işleme
        if category == "CRYPTO":
            # TRY çiftleri için özel geçmiş fiyat alma
            if "-TRY" in instrument_code:
                try:
                    # Geçmiş tarihlerde Binance TR API'si mevcut değildi, USD üzerinden hesapla
                    usd_symbol = instrument_code.replace("-TRY", "-USD")
                    
                    # USD fiyatını al (session ile)
                    usd_ticker = yf.Ticker(usd_symbol, session=YF_SESSION)
                    usd_hist = usd_ticker.history(start=start_date, end=end_date)
                    
                    # USD/TRY kurunu al (session ile)
                    usdtry_ticker = yf.Ticker("USDTRY=X", session=YF_SESSION)
                    usdtry_hist = usdtry_ticker.history(start=start_date, end=end_date)
                    
                    if not usd_hist.empty and not usdtry_hist.empty:
                        # En yakın tarihteki fiyatları al
                        target_date_str = date.strftime('%Y-%m-%d')
                        
                        # USD fiyatı
                        usd_hist.index = usd_hist.index.strftime('%Y-%m-%d')
                        if target_date_str in usd_hist.index:
                            usd_price = usd_hist.loc[target_date_str, 'Close']
                        else:
                            usd_price = usd_hist['Close'].iloc[-1]
                        
                        # USD/TRY kuru
                        usdtry_hist.index = usdtry_hist.index.strftime('%Y-%m-%d')
                        if target_date_str in usdtry_hist.index:
                            usd_try_rate = usdtry_hist.loc[target_date_str, 'Close']
                        else:
                            usd_try_rate = usdtry_hist['Close'].iloc[-1]
                        
                        try_price = usd_price * usd_try_rate
                        return try_price
                    
                    return 0
                except Exception as e:
                    return 0
            
            # USD ve diğer çiftler için normal Yahoo Finance (session ile)
            else:
                ticker = yf.Ticker(instrument_code, session=YF_SESSION)
        else:
            # Kripto olmayan enstrümanlar (session ile)
            ticker = yf.Ticker(ticker_symbol, session=YF_SESSION)
        
        hist = ticker.history(start=start_date, end=end_date)
        
        if not hist.empty:
            # O tarihe en yakın fiyatı al
            target_date_str = date.strftime('%Y-%m-%d')
            
            # Tam tarihi bul
            hist.index = hist.index.strftime('%Y-%m-%d')
            if target_date_str in hist.index:
                return hist.loc[target_date_str, 'Close']
            else:
                # En yakın tarihi al
                return hist['Close'].iloc[-1]
        
        return 0
        
    except Exception as e:
        return 0

def get_category_currency(category):
    """Kategori bazında para birimini al"""
    category_currency = {
        'BIST': '₺',
        'NASDAQ': '$',
        'METALS': '$',
        'FOREX': '$'  # FOREX için genelde USD bazlı gösterim
    }
    return category_currency.get(category, '$')

def get_specific_instrument_currency(instrument_code, category):
    """Belirli enstrüman için para birimini al"""
    # Nakit para birimleri için özel para birimi belirleme
    if category == 'CASH':
        if 'TRY' in instrument_code:
            return '₺'
        elif 'USD' in instrument_code:
            return '$'
        elif 'EUR' in instrument_code:
            return '€'
        elif 'GBP' in instrument_code:
            return '£'
        elif 'JPY' in instrument_code:
            return '¥'
        elif 'CHF' in instrument_code:
            return 'CHF'
        elif 'CAD' in instrument_code:
            return 'CAD'
        elif 'AUD' in instrument_code:
            return 'AUD'
        elif 'SEK' in instrument_code:
            return 'SEK'
        elif 'NOK' in instrument_code:
            return 'NOK'
        elif 'DKK' in instrument_code:
            return 'DKK'
        elif 'PLN' in instrument_code:
            return 'PLN'
        elif 'CZK' in instrument_code:
            return 'CZK'
        elif 'HUF' in instrument_code:
            return 'HUF'
        elif 'RUB' in instrument_code:
            return 'RUB'
        elif 'CNY' in instrument_code:
            return 'CNY'
        elif 'KRW' in instrument_code:
            return 'KRW'
        elif 'SGD' in instrument_code:
            return 'SGD'
        elif 'HKD' in instrument_code:
            return 'HKD'
        elif 'INR' in instrument_code:
            return 'INR'
        elif 'BRL' in instrument_code:
            return 'BRL'
        elif 'MXN' in instrument_code:
            return 'MXN'
        elif 'ZAR' in instrument_code:
            return 'ZAR'
        elif 'SAR' in instrument_code:
            return 'SAR'
        elif 'AED' in instrument_code:
            return 'AED'
        else:
            return '$'  # Varsayılan olarak USD
    
    # TEFAS fonları için TRY
    if category == 'TEFAS':
        return '₺'
    
    # Kripto paralar için para birimi belirleme
    if category == 'CRYPTO':
        if '-TRY' in instrument_code:
            return '₺'  # BTC-TRY gibi Türk Lirası bazlı kripto çiftleri
        elif '-EUR' in instrument_code:
            return '€'  # EUR bazlı kripto çiftleri
        else:
            return '$'  # Genellikle USD bazlı
    
    # FOREX çiftleri için özel para birimi belirleme
    if category == 'FOREX':
        if 'TRY' in instrument_code:
            if instrument_code.startswith('TRY'):
                return '$'  # TRYUSD=X gibi çiftler için USD
            else:
                return '₺'  # USDTRY=X gibi çiftler için TRY
        elif 'EUR' in instrument_code:
            if instrument_code.startswith('EUR'):
                return '$'  # EURUSD=X için USD
            else:
                return '€'  # Diğer EUR çiftleri için EUR
        elif 'GBP' in instrument_code:
            return '$'  # GBP çiftleri genelde USD bazlı
        else:
            return '$'  # Diğer FOREX çiftleri için USD
    
    # Türk altını için TRY
    if instrument_code in TURKISH_GOLD_INSTRUMENTS:
        return '₺'
    
    # Diğer kategoriler için varsayılan
    current_categories = get_instrument_categories()
    return current_categories[category]["currency"]

# Ana uygulama başlığı güncelleme
def show_main_app():
    """Ana uygulamayı göster"""
    
    # Abonelik kontrolü - Admin kullanıcılar her zaman erişebilir
    user_email = st.session_state.get('user_email', '')
    if not is_admin(user_email):
        if not is_subscription_active(user_email):
            # Abonelik süresi dolmuş - engelleme sayfası göster
            show_subscription_expired_page()
            return
    
    # Flush logs queued by background jobs into st.session_state (must run on main thread)
    try:
        flush_job_logs()
    except Exception:
        # If flushing fails, continue rendering the UI — non-fatal
        pass

    # Apply the dark finance dashboard styling on every render
    inject_dark_theme()
    
    # Kullanıcı bilgilerini üst kısımda göster
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        st.title("📊 Finansal Analiz Platformu")
        st.markdown("*BIST • NASDAQ • Kıymetli Madenler • Döviz • TEFAS Fonları • Portföy*")
    
    with col2:
        # Kullanıcı adını göster
        user_name = st.session_state.get('user_name', 'Kullanıcı')
        st.markdown(f'<div style="padding: 8px; text-align: center;"><strong>👤 {user_name}</strong></div>', unsafe_allow_html=True)
    
    with col3:
        if st.button("🚪 Çıkış Yap", type="secondary"):
            # 🔐 GÜVENLİ ÇIKIŞ: Token'ı iptal et
            user_email = st.session_state.get('user_email', '')
            if user_email:
                user_id = get_user_id_from_email(user_email)
                revoke_remember_me_token(user_email, user_id, series_id=None)  # Tüm token'ları sil
            
            # Çıkış sırasında "Beni Hatırla" verilerini temizle
            clear_remembered_credentials()
            # Cookie manager ile sil
            if COOKIES_AVAILABLE and cookie_manager is not None:
                try:
                    cookie_manager.delete("finapp_remember_token", key="del_token_4")
                    cookie_manager.delete("finapp_remembered_email", key="del_email_4")
                    cookie_manager.delete("finapp_persistent_logins", key="del_logins_4")
                except Exception as e:
                    pass  # Çıkış sırasında hata gösterme
            # Logout flag'i ayarla (auto-login'i önlemek için)
            st.session_state['just_logged_out'] = True
            st.session_state['remembered_email'] = ""
            # Oturum ve kullanıcı bilgilerini temizle
            for key in ['logged_in', 'user_email', 'user_name']:
                if key in st.session_state:
                    del st.session_state[key]
            # Kullanıcıya özel cache/state alanlarını da temizle
            for _k in [
                'portfolio_initialized',
                'portfolio_data',
                'portfolio_data_hash',
                'portfolio_values_cache',
                'active_portfolio_tab',
            ]:
                if _k in st.session_state:
                    del st.session_state[_k]
            st.rerun()
    
    st.markdown("---")

    # Scheduler admin panel (developer expander) removed from UI.
    # The diagnostics and runtime controls were developer-only and have been hidden.
    # If you need them back, restore the `show_scheduler_admin_panel` implementation here.
    
    # Ana sekme yapısı artık sidebar'da - Modern tasarım
    with st.sidebar:
        # Modern sidebar CSS stileri
        st.markdown("""
        <style>
        /* Dashboard başlığı - modern karanlık kutu */
        .menu-header {
            background: linear-gradient(135deg, rgba(37, 99, 235, 0.72) 0%, rgba(12, 18, 32, 0.95) 100%);
            color: var(--text-primary);
            padding: 18px 16px;
            text-align: left;
            font-size: 16px;
            letter-spacing: 0.4px;
            font-weight: 600;
            margin: -1rem -1rem 22px -1rem;
            border-bottom: 1px solid rgba(59, 130, 246, 0.28);
            box-shadow: 0 12px 26px rgba(8, 13, 24, 0.6);
            border-radius: 0 0 20px 20px;
        }

        .menu-header span {
            display: block;
            font-size: 13px;
            font-weight: 400;
            color: var(--text-secondary);
            margin-top: 4px;
        }

        /* Streamlit butonlarını modern hale getir */
        .stButton > button {
            background: linear-gradient(135deg, var(--accent-start) 0%, var(--accent-end) 100%);
            color: #f8fafc;
            border: 1px solid rgba(37, 99, 235, 0.55);
            border-radius: 12px;
            padding: 14px 16px;
            font-weight: 600;
            font-size: 14px;
            transition: all 0.3s ease;
            box-shadow: 0 12px 24px rgba(15, 23, 42, 0.45);
            width: 100%;
            margin: 6px 0;
            text-align: left;
        }

        .stButton > button:hover {
            transform: translateX(4px) translateY(-2px) scale(1.01);
            box-shadow: 0 18px 32px rgba(37, 99, 235, 0.35);
            border-color: rgba(96, 165, 250, 0.6);
        }

        .stButton > button:focus-visible {
            outline: none;
            box-shadow: 0 0 0 2px rgba(148, 163, 184, 0.2), 0 0 0 5px rgba(37, 99, 235, 0.45);
        }

        /* Aktif buton stili */
        .stButton > button:active {
            transform: translateX(1px) translateY(0px);
            box-shadow: 0 8px 18px rgba(37, 99, 235, 0.4);
        }

        /* Divider stilleri */
        .sidebar-divider {
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(37, 99, 235, 0.6), transparent);
            margin: 24px 0;
            border: none;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Modern menü başlığı
        st.markdown('<div class="menu-header">🚀 Finans Dashboard<span>Gerçek zamanlı finansal içgörüler</span></div>', unsafe_allow_html=True)
        
        # Session state'de seçili menüyü tut
        if 'selected_menu' not in st.session_state:
            st.session_state.selected_menu = "portfolio"
        
        # Doğrudan menü butonları
        if st.button("💼 Portföy Yönetimi", key="menu_portfolio"):
            st.session_state.selected_menu = "portfolio"
        
        if st.button("📈 Piyasa Analizi", key="menu_market"):
            st.session_state.selected_menu = "market"
            
        # Veri Yönetimi sadece admin kullanıcısı için
        if st.session_state.get('user_email') == 'erdalural@gmail.com':
            if st.button("🛠️ Veri Yönetimi", key="menu_data"):
                st.session_state.selected_menu = "data"
        
        # Admin Paneli - Abonelik yönetimi (sadece admin kullanıcı)
        if is_admin(st.session_state.get('user_email', '')):
            if st.button("⚙️ Admin Paneli", key="menu_admin"):
                st.session_state.selected_menu = "admin"
        
        # Seçili menü bilgisi
        menu_info = {
            "portfolio": {
                "icon": "💼",
                "title": "Portföy Yönetimi", 
                "desc": "Yatırım portföyünüzü yönetin"
            },
            "market": {
                "icon": "📈", 
                "title": "Piyasa Analizi",
                "desc": "Piyasa verilerini analiz edin"
            }
        }
        
        # Admin kullanıcısı için veri yönetimi menüsü ekle
        if st.session_state.get('user_email') == 'erdalural@gmail.com':
            menu_info["data"] = {
                "icon": "🛠️",
                "title": "Veri Yönetimi", 
                "desc": "Veri kaynaklarınızı yönetin"
            }
        
        # Admin kullanıcısı için admin paneli menüsü ekle
        if is_admin(st.session_state.get('user_email', '')):
            menu_info["admin"] = {
                "icon": "⚙️",
                "title": "Admin Paneli", 
                "desc": "Kullanıcı ve abonelik yönetimi"
            }
        
        st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    if st.session_state.selected_menu == "portfolio":
        show_portfolio_management()
    elif st.session_state.selected_menu == "market":
        show_market_analysis()
    elif st.session_state.selected_menu == "data":
        # Veri Yönetimi sadece admin kullanıcısı için
        if st.session_state.get('user_email') == 'erdalural@gmail.com':
            show_data_management()
        else:
            st.error("🚫 Bu bölüme erişim yetkiniz bulunmamaktadır.")
            st.session_state.selected_menu = "portfolio"  # Ana sayfa'ya yönlendir
    elif st.session_state.selected_menu == "admin":
        # Admin Paneli - Abonelik yönetimi
        if is_admin(st.session_state.get('user_email', '')):
            show_admin_panel()
        else:
            st.error("🚫 Bu bölüme erişim yetkiniz bulunmamaktadır.")
            st.session_state.selected_menu = "portfolio"

def show_data_management():
    """Veri yönetimi sekmesini göster - BIST, NASDAQ ve TEFAS veri işlemleri"""
    # Modern CSS stilleri
    st.markdown("""
    <style>
    .modern-header {
        background: linear-gradient(140deg, rgba(16, 24, 40, 0.95) 0%, rgba(8, 13, 23, 0.88) 60%, rgba(3, 7, 18, 0.9) 100%);
        color: var(--text-primary);
        padding: 24px;
        border-radius: 18px;
        text-align: left;
        font-size: 24px;
        font-weight: 600;
        margin: 16px 0 24px 0;
        border: 1px solid var(--card-border);
        box-shadow: 0 18px 45px rgba(8, 13, 24, 0.55);
    }
    .modern-description {
        background: rgba(14, 22, 36, 0.82);
        padding: 18px 22px;
        border-radius: 14px;
        color: var(--text-secondary);
        font-size: 16px;
        margin: 10px 0 28px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.65);
        box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.1);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="modern-header">🛠️ Veri Yönetimi</div>', unsafe_allow_html=True)
    st.markdown('<div class="modern-description">Bu sekmede BIST hisse listesi, NASDAQ hisse listesi, TEFAS fon verilerinizi ve Turkish Gold fiyatlarını yönetebilirsiniz.</div>', unsafe_allow_html=True)
    
    # Alt sekmeler
    dtab1, dtab2, dtab3, dtab4 = st.tabs([
        "📊 BIST Hisse Yönetimi", 
        "🏛️ NASDAQ Hisse Yönetimi", 
        "📈 TEFAS Veri Yönetimi", 
        "🥇 Turkish Gold Yönetimi"
    ])
    
    with dtab1:
        show_bist_data_management()
    
    with dtab2:
        show_nasdaq_data_management()
    
    with dtab3:
        show_tefas_data_management()
    
    with dtab4:
        show_turkish_gold_data_management()

def show_portfolio_management():
    """Portföy yönetimi sekmesini göster"""
    # Modern sidebar CSS stilleri - Portföy yönetimi için (Piyasa Analizi ile hizalandı)
    st.sidebar.markdown("""
    <style>
    /* Modern section başlıkları - karanlık temaya uyum */
    .section-header {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.85) 0%, rgba(15, 23, 42, 0.92) 100%);
        color: var(--text-primary);
        padding: 12px 16px;
        border-radius: 12px;
        font-size: 13px;
        font-weight: 600;
        margin: 20px 0 14px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.7);
        box-shadow: 0 14px 24px rgba(8, 13, 24, 0.45);
    }

    /* Modern multiselect ve selectbox stilleri */
    div[data-testid="stMultiSelect"],
    div[data-testid="stSelectbox"] {
        background: linear-gradient(135deg, rgba(16, 24, 40, 0.96) 0%, rgba(12, 19, 33, 0.88) 100%);
        border-radius: 18px;
        padding: 8px 10px 10px 10px;
        border: 1px solid rgba(59, 130, 246, 0.28);
        box-shadow: 0 22px 44px rgba(6, 11, 22, 0.55);
        margin-bottom: 8px;
    }

    div[data-testid="stMultiSelect"] > label,
    div[data-testid="stSelectbox"] > label {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.58) 0%, rgba(29, 78, 216, 0.48) 100%);
        border-radius: 10px;
        padding: 6px 10px;
        font-weight: 600;
        font-size: 12px;
        color: #f8fafc;
        margin-bottom: 8px;
        box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.18);
        letter-spacing: 0.01em;
    }

    div[data-testid="stMultiSelect"] > label p,
    div[data-testid="stSelectbox"] > label p {
        color: #f8fafc !important;
        margin: 0 !important;
    }

    /* Kategori seçimi değer metnini görünür tut */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child {
        display: flex !important;
        align-items: center !important;
        min-height: 48px !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child p {
        color: #f8fafc !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        letter-spacing: 0.01em !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input {
        color: #f8fafc !important;
    }
    
    /* Multiselect input alanı - placeholder ve metin okunabilirliği */
    div[data-testid="stMultiSelect"] input {
        color: #ffffff !important;
        font-size: 14px !important;
        font-weight: 500 !important;
    }
    
    div[data-testid="stMultiSelect"] input::placeholder {
        color: #d1d5db !important;
        opacity: 0.9 !important;
    }
    
    /* Seçili öğeler (tags) */
    div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background: rgba(37, 99, 235, 0.8) !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        padding: 4px 8px !important;
        border-radius: 6px !important;
    }
    
    div[data-testid="stMultiSelect"] ul {
        max-height: 280px;
        background-color: rgba(13, 20, 34, 0.96);
        border-radius: 10px;
        color: var(--text-primary);
    }
    /* Force closed multiselect/select control to be dark and show muted placeholder */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:nth-child(1),
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:nth-child(1) {
        background: rgba(15, 23, 42, 0.92) !important;
        color: var(--text-primary) !important;
        border: 1px solid rgba(59, 130, 246, 0.18) !important;
        border-radius: 10px !important;
        padding: 10px 12px !important;
        box-shadow: none !important;
    }

    /* Placeholder text inside the closed select control */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input::placeholder,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child input::placeholder {
        color: #cbd5e1 !important; /* muted light */
        opacity: 0.95 !important;
    }

    /* Ensure selected text uses readable font like Enstrüman selection */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] > div,
    div[data-testid="stSelectbox"] div {
        font-size: 14px !important;
        font-weight: 500 !important;
        line-height: 1.4 !important;
        color: var(--text-primary) !important;
    }

    /* Stronger, sidebar-specific selector to ensure Kategori Select matches Enstrüman Seçimi */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:nth-child(1) {
        background: linear-gradient(135deg, rgba(30,41,59,0.85) 0%, rgba(15,23,42,0.92) 100%) !important;
        border: 1px solid rgba(59, 130, 246, 0.18) !important;
        box-shadow: inset 0 0 0 1px rgba(15, 23, 42, 0.45) !important;
        border-radius: 12px !important;
        padding: 12px 14px !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
    }

    /* Sidebar placeholder/selected text clarity */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input::placeholder {
        color: #e2e8f0 !important;
        opacity: 0.95 !important;
    }

    /* Sidebar chevron color */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] svg {
        fill: #e2e8f0 !important;
        color: #e2e8f0 !important;
        opacity: 0.95 !important;
    }

    /* Ensure the dropdown chevron is visible and muted */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] svg,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] svg {
        fill: #cbd5e1 !important;
        color: #cbd5e1 !important;
        opacity: 0.95 !important;
    }
    div[data-testid="stSelectbox"] div {
        color: var(--text-primary);
        font-weight: 500;
        font-size: 14px;
    }

    /* Modern input stilleri */
    div[data-testid="stTextInput"] > div > div > input,
    div[data-testid="stNumberInput"] input {
        background: rgba(13, 20, 34, 0.92);
        border: 1px solid rgba(59, 130, 246, 0.3);
        border-radius: 12px;
        padding: 12px;
        font-size: 13px;
        transition: all 0.3s ease;
        color: var(--text-primary);
    }
    div[data-testid="stTextInput"] > div > div > input:focus,
    div[data-testid="stNumberInput"] input:focus {
        border-color: rgba(37, 99, 235, 0.6);
        box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.45);
    }

    /* Modern date input stilleri */
    div[data-testid="stDateInput"] > div > div > input {
        background: rgba(13, 20, 34, 0.92);
        border: 1px solid rgba(37, 99, 235, 0.35);
        border-radius: 12px;
        padding: 10px 12px;
        color: var(--text-primary);
        font-size: 13px;
    }

    /* Modern buton stilleri */
    div[data-testid="stButton"] > button {
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.95) 0%, rgba(29, 78, 216, 0.95) 100%);
        color: #f8fafc;
        border: 1px solid rgba(59, 130, 246, 0.55);
        border-radius: 18px;
        padding: 12px 22px;
        font-weight: 600;
        font-size: 14px;
        transition: all 0.3s ease;
        box-shadow: 0 16px 30px rgba(15, 23, 42, 0.5);
    }
    div[data-testid="stButton"] > button:hover {
        transform: translateY(-2px) scale(1.01);
        box-shadow: 0 22px 36px rgba(37, 99, 235, 0.32);
        border-color: rgba(148, 163, 184, 0.3);
    }

    /* Primary buton özel stili */
    div[data-testid="stButton"] > button[kind="primary"] {
        background: linear-gradient(135deg, #22d3ee 0%, #0ea5e9 100%);
        color: #041120;
        box-shadow: 0 20px 30px rgba(14, 165, 233, 0.4);
        font-weight: 700;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #06b6d4 0%, #0ea5e9 100%);
        box-shadow: 0 26px 36px rgba(14, 165, 233, 0.45);
        transform: translateY(-3px) scale(1.02);
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Modern CSS stilleri (ana içerik alanı için)
    st.markdown("""
    <style>
    .modern-header {
        background: transparent;
        color: var(--text-primary);
        padding: 24px 0;
        border-radius: 18px;
        text-align: left;
        font-size: 24px;
        font-weight: 600;
        margin: 16px 0 24px 0;
        border: none;
        box-shadow: none;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Başlık ve abonelik bilgisini iki kolona ayır
    col_header, col_subscription = st.columns([1, 1])
    
    with col_header:
        st.markdown('<div class="modern-header">💼 Portföy Yönetimi</div>', unsafe_allow_html=True)
    
    # Aktif sekme durumunu session state'de tut
    if 'active_portfolio_tab' not in st.session_state:
        st.session_state.active_portfolio_tab = 0

    tab_labels = ["📊 Portföy Özeti", "➕ İşlem Ekle", "📋 İşlem Geçmişi"]

    # Özel sekme stili (radio butonlarını modern tab görünümlü yap)
    st.markdown(
        """
        <style>
        div[data-testid="stRadio"][aria-label="Portföy sekmesini seçin"] > div[role="radiogroup"] {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
        }
        div[data-testid="stRadio"][aria-label="Portföy sekmesini seçin"] label {
            background: rgba(17, 24, 39, 0.75);
            padding: 11px 22px;
            border-radius: 999px;
            border: 1px solid rgba(59, 130, 246, 0.28);
            box-shadow: 0 10px 22px rgba(8, 13, 24, 0.45);
            transition: all 0.25s ease;
            cursor: pointer;
            font-weight: 600;
            color: rgba(248, 250, 252, 0.92);
            letter-spacing: 0.15px;
        }
        div[data-testid="stRadio"][aria-label="Portföy sekmesini seçin"] label:hover {
            border-color: rgba(96, 165, 250, 0.55);
            box-shadow: 0 16px 28px rgba(37, 99, 235, 0.25);
            transform: translateY(-2px);
            color: var(--text-primary);
        }
        div[data-testid="stRadio"][aria-label="Portföy sekmesini seçin"] label[strata-selected="true"] {
            background: linear-gradient(135deg, rgba(37, 99, 235, 0.95) 0%, rgba(29, 78, 216, 0.95) 100%);
            color: #f8fafc !important;
            box-shadow: 0 18px 32px rgba(37, 99, 235, 0.35);
            border-color: transparent;
            transform: translateY(-2px);
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Ensure the radio widget's session key is seeded once to avoid
    # a mismatch between the widget value and our `active_portfolio_tab`.
    if 'portfolio_tab_selector' not in st.session_state:
        st.session_state['portfolio_tab_selector'] = st.session_state.get('active_portfolio_tab', 0)

    selected_tab_index = st.radio(
        "Portföy sekmesini seçin",
        options=list(range(len(tab_labels))),
        format_func=lambda idx: tab_labels[idx],
        horizontal=True,
        key="portfolio_tab_selector",
        label_visibility="collapsed"
    )

    # Keep a convenience duplicate for other code paths
    st.session_state.active_portfolio_tab = selected_tab_index

    if selected_tab_index == 0:
        show_portfolio_summary()
    elif selected_tab_index == 1:
        show_add_transaction()
    else:
        show_transaction_history()

def show_sidebar_bottom_buttons_portfolio():
    """Portföy sayfaları için sidebar alt butonları"""
    # CSS stileri - Şikayet & Öneri ve Hesap Ayarları butonlarını küçült ve aşağıya taşı
    st.sidebar.markdown("""
    <style>
    /* Sidebar buton stillerini özel hale getir - daha küçük font ve daha aşağıya */
    [data-testid="stSidebar"] button[key*="feedback_portfolio"],
    [data-testid="stSidebar"] button[key*="settings_portfolio"] {
        font-size: 10.5px !important;  /* 14px -> 10.5px (%75 küçültü) */
        padding: 8px 10px !important;  /* Daha kompakt padding */
        margin: 20px 0 !important;  /* Daha aşağıya */
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Sidebar'da Şikayet & Öneri ve Ayarlar butonları
    st.sidebar.markdown('<hr style="margin: 300px 0 40px 0; border-color: rgba(59, 130, 246, 0.2);">', unsafe_allow_html=True)
    
    # Session state for sidebar sections
    if 'show_feedback_portfolio' not in st.session_state:
        st.session_state['show_feedback_portfolio'] = False
    if 'show_settings_portfolio' not in st.session_state:
        st.session_state['show_settings_portfolio'] = False
    
    # Şikayet & Öneri Butonu
    if st.sidebar.button("📝 Şikayet & Öneri", key="btn_toggle_feedback_portfolio", use_container_width=True):
        st.session_state['show_feedback_portfolio'] = not st.session_state['show_feedback_portfolio']
        st.session_state['show_settings_portfolio'] = False
    
    if st.session_state['show_feedback_portfolio']:
        with st.sidebar:
            st.markdown('<div style="background: rgba(30, 41, 59, 0.6); padding: 12px; border-radius: 8px; margin-top: 8px;">', unsafe_allow_html=True)
            
            feedback_type = st.radio(
                "Bildirim Türü:",
                options=["sikayet", "oneri", "bilgi_talebi"],
                format_func=lambda x: {"sikayet": "🔴 Şikayet", "oneri": "💡 Öneri", "bilgi_talebi": "❓ Bilgi Talebi"}[x],
                key="feedback_type_portfolio",
                horizontal=True
            )
            
            feedback_subject = st.text_input("Konu:", max_chars=100, key="feedback_subject_portfolio")
            feedback_message = st.text_area("Açıklama:", height=80, key="feedback_message_portfolio")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Gönder", key="btn_send_feedback_portfolio", type="primary", use_container_width=True):
                    if not feedback_subject or not feedback_message:
                        st.error("⚠️ Tüm alanları doldurun!")
                    else:
                        success, message = send_feedback_email(
                            feedback_type=feedback_type,
                            subject=feedback_subject,
                            message=feedback_message,
                            user_email=st.session_state.get('user_email', ''),
                            user_name=st.session_state.get('user_name', '')
                        )
                        if success:
                            st.success("✅ Gönderildi!")
                        else:
                            st.error(message)
            with col2:
                if st.button("❌ Kapat", key="btn_close_feedback_portfolio", use_container_width=True):
                    st.session_state['show_feedback_portfolio'] = False
                    st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Ayarlar Butonu
    if st.sidebar.button("⚙️ Hesap Ayarları", key="btn_toggle_settings_portfolio", use_container_width=True):
        st.session_state['show_settings_portfolio'] = not st.session_state['show_settings_portfolio']
        st.session_state['show_feedback_portfolio'] = False
    
    if st.session_state['show_settings_portfolio']:
        with st.sidebar:
            user_email = st.session_state.get('user_email', '')
            subscription = get_user_subscription(user_email)
            
            if subscription and is_subscription_active(user_email):
                start_date = subscription.get('start_date', 'N/A')
                end_date = subscription.get('end_date', 'N/A')
                plan = subscription.get('plan', 'N/A')
                days_remaining = get_subscription_days_remaining(user_email)
                
                st.markdown(f"""
                <div style="background: rgba(37, 99, 235, 0.08); padding: 12px; border-radius: 8px; border: 1px solid rgba(37, 99, 235, 0.3); margin-top: 8px;">
                    <div style="font-weight: 600; font-size: 12px; color: #60a5fa; margin-bottom: 8px;">✅ Aktif Abonelik</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Başlangıç:</strong> {start_date}</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Bitiş:</strong> {end_date}</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Plan:</strong> {plan}</div>
                    <div style="font-size: 11px; color: #60a5fa; margin: 6px 0; font-weight: 600;">⏱️ Kalan: {days_remaining} gün</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: rgba(239, 68, 68, 0.08); padding: 12px; border-radius: 8px; border: 1px solid rgba(239, 68, 68, 0.3); margin-top: 8px;">
                    <div style="font-weight: 600; font-size: 12px; color: #fca5a5; margin-bottom: 6px;">⚠️ Abonelik Süresi Dolmuş</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95);">Lütfen aboneliğinizi yenileyin.</div>
                </div>
                """, unsafe_allow_html=True)
            
            if st.button("❌ Kapat", key="btn_close_settings_portfolio", use_container_width=True):
                st.session_state['show_settings_portfolio'] = False
                st.rerun()

def show_portfolio_summary():
    """Portföy özetini göster"""
    # Modern CSS stilleri
    st.markdown("""
    <style>
    .modern-subheader {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.78) 0%, rgba(15, 23, 42, 0.82) 100%);
        color: var(--text-primary);
        padding: 18px 22px;
        border-radius: 16px;
        font-size: 18px;
        font-weight: 600;
        margin: 18px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.76);
        box-shadow: 0 18px 32px rgba(8, 13, 24, 0.38);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="modern-subheader">📊 Portföy Özeti</div>', unsafe_allow_html=True)
    
    # Para birimi seçimi - replace dropdown with horizontal radio for better dark-mode rendering
    target_currency = st.radio(
        "💱 Görüntüleme Para Birimi:",
        options=["₺", "$", "€", "£"],
        help="Portföy otomatik olarak seçilen para birimine çevrilir",
        key="portfolio_currency_selector",
        horizontal=True,
    )
    
    # Session state'te portföy verilerini sakla
    if 'portfolio_initialized' not in st.session_state:
        st.session_state.portfolio_initialized = False
        st.session_state.portfolio_data = None
        st.session_state.total_value = 0
        st.session_state.current_currency = target_currency
        st.session_state.portfolio_data_hash = None
    
    # Yenile butonu
    refresh_clicked = st.button("🔄 Portföyü Yenile", type="primary", key="refresh_portfolio_btn")
    
    # Para birimi değişip değişmediğini kontrol et - ÖNCE kontrol et
    previous_currency = st.session_state.get('current_currency', target_currency)
    currency_changed = previous_currency != target_currency
    
    # Veri güncelleme: Sadece buton tıklanınca veya para birimi değişince
    should_refresh = refresh_clicked or currency_changed or not st.session_state.portfolio_initialized
    
    if should_refresh:
        with st.spinner("Portföy verileri güncelleniyor..."):
            # Cache'i sadece refresh butonu tıklandığında temizle
            if refresh_clicked:
                st.cache_data.clear()
            
            # Verileri al
            portfolio_data, total_value, positions = get_portfolio_summary(
                st.session_state['user_email'], 
                target_currency
            )
            
            # Session state'e kaydet
            st.session_state.portfolio_data = portfolio_data
            st.session_state.total_value = total_value
            st.session_state.current_currency = target_currency
            st.session_state.portfolio_initialized = True
    
    # Mevcut verileri kullan
    portfolio_data = st.session_state.portfolio_data
    total_value = st.session_state.total_value
    
    # Para birimi değişikliğinde ek bilgi
    if currency_changed and not refresh_clicked and portfolio_data:
        st.info(f"ℹ️ Portföy {previous_currency} → {target_currency} para birimine dönüştürüldü")
    
    if portfolio_data:
        # Portföy detaylarını göster
        show_portfolio_details_table(portfolio_data, target_currency)
        
    else:
        st.info("📝 Henüz portföyünüzde hiç işlem bulunmuyor. 'İşlem Ekle' sekmesinden yatırımlarınızı kaydetmeye başlayın!")
    
    # Sidebar alt butonları
    show_sidebar_bottom_buttons_portfolio()

@st.cache_data(ttl=300)  # 5 dakika cache
def calculate_portfolio_value_over_time(user_email, target_currency="₺"):
    """Kullanıcının portföy değerini zaman içinde hesapla - Aylık"""
    try:
        portfolios = load_portfolios()
        
        if user_email not in portfolios or not portfolios[user_email].get('transactions'):
            return pd.DataFrame()
        
        transactions = portfolios[user_email]['transactions']
        
        # İşlemleri tarihe göre sırala
        transactions_df = pd.DataFrame(transactions)
        transactions_df['date'] = pd.to_datetime(transactions_df['date'])
        transactions_df = transactions_df.sort_values('date')
        
        # İlk ve son işlem tarihlerini al
        start_date = transactions_df['date'].min().date()
        end_date = datetime.now().date()
        
        # Aylık tarih aralığı oluştur
        monthly_dates = []
        current_date = start_date.replace(day=1)  # Ayın ilk günü
        
        while current_date <= end_date:
            monthly_dates.append(current_date)
            # Bir sonraki ayın ilk günü
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        portfolio_values = []
        
        for calc_date in monthly_dates:
            # Bu tarihe kadar olan işlemleri filtrele
            relevant_transactions = transactions_df[transactions_df['date'].dt.date <= calc_date]
            
            if relevant_transactions.empty:
                portfolio_values.append({
                    'Tarih': calc_date,
                    'Toplam Değer': 0,
                    'Toplam Maliyet': 0,
                    'Kar/Zarar': 0
                })
                continue
            
            # Pozisyonları hesapla
            positions = {}
            
            for _, trans in relevant_transactions.iterrows():
                code = trans['instrument_code']
                
                if code not in positions:
                    positions[code] = {
                        'total_quantity': 0,
                        'total_cost': 0,
                        'category': trans['category'],
                        'instrument_name': trans['instrument_name']
                    }
                
                # İşlem tarihindeki kur ile hedef para birimine çevir
                rate_on_transaction_date = get_currency_rate(trans['currency'], target_currency, trans['date'].date())
                cost_in_target_currency = trans['total_value'] * rate_on_transaction_date
                
                if trans['type'] == 'BUY':
                    positions[code]['total_quantity'] += trans['quantity']
                    positions[code]['total_cost'] += cost_in_target_currency
                else:
                    positions[code]['total_quantity'] -= trans['quantity']
                    # Satış için oranlı maliyet çıkarma
                    if positions[code]['total_quantity'] > 0:
                        avg_cost = positions[code]['total_cost'] / (positions[code]['total_quantity'] + trans['quantity'])
                        positions[code]['total_cost'] -= avg_cost * trans['quantity']
            
            # Bu tarih için toplam değeri hesapla
            total_value = 0
            total_cost = 0
            missing_data_instruments = []
            
            for code, pos in positions.items():
                if pos['total_quantity'] > 0:
                    # Bu tarih için mümkünse geçmiş fiyatı al
                    try:
                        if calc_date == datetime.now().date():
                            # Bugün ise güncel fiyatı kullan
                            current_price = get_current_price(code, pos['category'])
                        else:
                            # Geçmiş tarih ise geçmiş fiyatı al
                            current_price = get_historical_price(code, pos['category'], calc_date)
                        
                        if current_price > 0:
                            # Para birimi dönüşümü
                            price_currency = get_specific_instrument_currency(code, pos['category'])
                            rate_on_calc_date = get_currency_rate(price_currency, target_currency, calc_date)
                            current_value = pos['total_quantity'] * current_price * rate_on_calc_date
                            total_value += current_value
                        else:
                            # Fiyat bulunamazsa bu pozisyonu atla ve bilgi ver
                            if pos['category'] == 'TEFAS':
                                missing_data_instruments.append(f"{code} ({calc_date.strftime('%Y-%m-%d')})")
                            # TEFAS fonu için fiyat yoksa değeri 0 olarak hesapla
                            # Maliyet değeri kullanma - yanıltıcı olur
                        
                        total_cost += pos['total_cost']
                        
                    except Exception as e:
                        # Hata durumunda bu pozisyonu atla
                        if pos['category'] == 'TEFAS':
                            missing_data_instruments.append(f"{code} (hata: {str(e)[:50]}...)")
                        total_cost += pos['total_cost']
            
                st.warning(f"⚠️ {calc_date} tarihinde fiyat verisi bulunamayan TEFAS fonları: {', '.join(missing_data_instruments[:3])}{'...' if len(missing_data_instruments) > 3 else ''}")
            
            portfolio_values.append({
                'Tarih': calc_date,
                'Toplam Değer': total_value,
                'Toplam Maliyet': total_cost,
                'Kar/Zarar': total_value - total_cost
            })
        
        return pd.DataFrame(portfolio_values)
    
    except Exception as e:
        st.error(f"Portföy zaman serisi hesaplanırken hata: {str(e)}")
        return pd.DataFrame()

def show_portfolio_time_series(user_email, target_currency):
    """Portföy değeri zaman serisi grafiğini göster"""
    try:
        # Cache'lenmiş veri al
        time_series_data = calculate_portfolio_value_over_time(user_email, target_currency)
        
        if time_series_data.empty:
            st.info("📊 Henüz zaman serisi verileri mevcut değil. Birkaç işlem ekleyince grafik görünecek!")
            return
        
        # Plotly ile interaktif grafik oluştur
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=(f'Portföy Toplam Değeri ({target_currency})', f'Kar/Zarar Trendi ({target_currency})'),
            vertical_spacing=0.08,
            row_heights=[0.7, 0.3]
        )
        
        # Ana portföy değeri çizgisi
        fig.add_trace(
            go.Scatter(
                x=time_series_data['Tarih'],
                y=time_series_data['Toplam Değer'],
                mode='lines+markers',
                name=f'Toplam Değer ({target_currency})',
                line=dict(color='#60a5fa', width=3),
                marker=dict(size=6),
                hovertemplate='<b>%{x}</b><br>' +
                             f'Toplam Değer: %{{y:,.2f}} {target_currency}<br>' +
                             '<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Maliyet çizgisi (referans için)
        fig.add_trace(
            go.Scatter(
                x=time_series_data['Tarih'],
                y=time_series_data['Toplam Maliyet'],
                mode='lines',
                name=f'Toplam Maliyet ({target_currency})',
                line=dict(color='#6c757d', width=2, dash='dash'),
                hovertemplate='<b>%{x}</b><br>' +
                             f'Toplam Maliyet: %{{y:,.2f}} {target_currency}<br>' +
                             '<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Kar/Zarar trendi
        colors = ['red' if x < 0 else 'green' for x in time_series_data['Kar/Zarar']]
        fig.add_trace(
            go.Bar(
                x=time_series_data['Tarih'],
                y=time_series_data['Kar/Zarar'],
                name=f'Kar/Zarar ({target_currency})',
                marker_color=colors,
                opacity=0.7,
                hovertemplate='<b>%{x}</b><br>' +
                             f'Kar/Zarar: %{{y:,.2f}} {target_currency}<br>' +
                             '<extra></extra>'
            ),
            row=2, col=1
        )
        
        # Layout güncelleme
        fig.update_layout(
            title=f'📈 Portföy Performansı - Aylık Zaman Serisi ({target_currency})',
            height=700,
            showlegend=True,
            hovermode='x unified',
            paper_bgcolor='rgba(9, 13, 24, 0.0)',
            plot_bgcolor='rgba(10, 18, 32, 0.92)',
            font=dict(color='#e2e8f0'),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor='rgba(10, 18, 32, 0.85)',
                bordercolor='rgba(59, 130, 246, 0.25)',
                borderwidth=1,
                font=dict(color='#e2e8f0')
            ),
            margin=dict(t=100, r=20, l=20, b=60)
        )
        
        # X ekseni formatı
        fig.update_xaxes(title_text="Tarih", row=2, col=1)
        fig.update_yaxes(title_text=f"Değer ({target_currency})", row=1, col=1)
        fig.update_yaxes(title_text=f"Kar/Zarar ({target_currency})", row=2, col=1)
        
        # Grid ve eksen stilleri
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(148, 163, 184, 0.18)',
            zerolinecolor='rgba(148, 163, 184, 0.25)',
            color='#e2e8f0'
        )
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(148, 163, 184, 0.18)',
            zerolinecolor='rgba(148, 163, 184, 0.25)',
            color='#e2e8f0'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Özet istatistikler
        if len(time_series_data) > 1:
            col1, col2, col3, col4 = st.columns(4)
            
            first_value = time_series_data.iloc[0]['Toplam Değer']
            last_value = time_series_data.iloc[-1]['Toplam Değer']
            total_return = last_value - first_value
            total_return_pct = (total_return / first_value * 100) if first_value > 0 else 0
            
            max_value = time_series_data['Toplam Değer'].max()
            min_value = time_series_data['Toplam Değer'].min()
            
            with col1:
                st.metric(
                    f"📅 İlk Değer",
                    f"{first_value:,.2f} {target_currency}",
                    help=f"İlk işlem tarihi: {time_series_data.iloc[0]['Tarih'].strftime('%Y-%m-%d')}"
                )
            
            with col2:
                st.metric(
                    f"📈 Toplam Getiri",
                    f"{total_return:,.2f} {target_currency}",
                    delta=f"{total_return_pct:.2f}%",
                    delta_color="normal" if total_return >= 0 else "inverse"
                )
            
            with col3:
                st.metric(
                    f"🔝 En Yüksek",
                    f"{max_value:,.2f} {target_currency}",
                    help="Portföyün ulaştığı en yüksek değer"
                )
            
            with col4:
                st.metric(
                    f"🔻 En Düşük",
                    f"{min_value:,.2f} {target_currency}",
                    help="Portföyün ulaştığı en düşük değer"
                )
    
    except Exception as e:
        st.error(f"Zaman serisi grafiği oluşturulurken hata: {str(e)}")

def show_portfolio_details_table(portfolio_data, target_currency):
    """Portföy detayları tablosunu göster - Stabil widget state ile"""
    
    if not portfolio_data:
        st.info("📝 Portföy verisi bulunamadı.")
        return
    
    df = pd.DataFrame(portfolio_data)
    display_columns = ['Kod', 'Adı', 'Kategori', 'Miktar', 'Ort. Maliyet', 
                     'Güncel Fiyat', 'Güncel Değer', 'Toplam Maliyet', 
                     'Kar/Zarar', 'Kar/Zarar %', 'Para Birimi']
    df_display = df[display_columns].copy()

    # Ana metrikler - Her zaman görünür
    st.markdown("---")
    st.subheader("📊 Portföy Toplamları")
    
    # Ana metrikler
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_current_value = df_display['Güncel Değer'].sum()
        st.metric(
            f"💎 Toplam Güncel Değer",
            f"{total_current_value:,.2f} {target_currency}",
            help="Portföyün toplam güncel değeri"
        )
    
    with col2:
        total_cost = df_display['Toplam Maliyet'].sum()
        st.metric(
            f"💰 Toplam Maliyet",
            f"{total_cost:,.2f} {target_currency}",
            help="Portföye yapılan toplam yatırım"
        )
    
    with col3:
        total_profit_loss = df_display['Kar/Zarar'].sum()
        profit_color = "normal" if total_profit_loss >= 0 else "inverse"
        st.metric(
            f"⚖️ Toplam Kar/Zarar",
            f"{total_profit_loss:,.2f} {target_currency}",
            delta=f"{total_profit_loss:,.2f}",
            delta_color=profit_color,
            help="Toplam kar veya zarar miktarı"
        )
    
    with col4:
        total_profit_loss_pct = (total_profit_loss / total_cost * 100) if total_cost > 0 else 0
        pct_color = "normal" if total_profit_loss_pct >= 0 else "inverse"
        st.metric(
            f"📈 Toplam Kar/Zarar %",
            f"{total_profit_loss_pct:.2f}%",
            delta=f"{total_profit_loss_pct:.2f}%",
            delta_color=pct_color,
            help="Yatırımın toplam getiri oranı"
        )

    # Açılabilir detaylı analiz bölümleri
    
    # Grafik Analizleri
    with st.expander("📈 Detaylı Grafik Analizleri", expanded=False):
        st.subheader("📊 Kategori Bazında Dağılım")
        category_values = df_display.groupby('Kategori')['Güncel Değer'].sum()
        
        fig = go.Figure(data=[go.Pie(
            labels=category_values.index,
            values=category_values.values,
            hole=0.3
        )])
        fig.update_layout(
            title=f"Portföy Dağılımı ({target_currency})",
            height=400,
            paper_bgcolor='rgba(9, 13, 24, 0.0)',
            plot_bgcolor='rgba(10, 18, 32, 0.92)',
            font=dict(color='#e2e8f0'),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5,
                        font=dict(color='#e2e8f0'))
        )
        st.plotly_chart(fig, use_container_width=True)

    # Detaylı Portföy Tablosu
    with st.expander("📋 Detaylı Portföy Tablosu", expanded=False):
        st.subheader("📋 Portföy Detayları")
        
        # Basit formatlanmış tablo gösterimi
        try:
            formatted_df = df_display.copy()
            
            # Sayısal sütunları formatla
            formatted_df['Miktar'] = formatted_df['Miktar'].apply(lambda x: format_quantity_display(x, decimals=4))
            formatted_df['Ort. Maliyet'] = formatted_df['Ort. Maliyet'].apply(lambda x: f"{x:.2f}")
            formatted_df['Güncel Fiyat'] = formatted_df['Güncel Fiyat'].apply(lambda x: f"{x:.6f}")
            formatted_df['Güncel Değer'] = formatted_df['Güncel Değer'].apply(lambda x: f"{x:.2f}")
            formatted_df['Toplam Maliyet'] = formatted_df['Toplam Maliyet'].apply(lambda x: f"{x:.2f}")
            formatted_df['Kar/Zarar'] = formatted_df['Kar/Zarar'].apply(lambda x: f"{x:.2f}")
            formatted_df['Kar/Zarar %'] = formatted_df['Kar/Zarar %'].apply(lambda x: f"{x:.2f}%")
            
            # Render a dark-themed Plotly table so the table card matches the dashboard's dark background
            try:
                table_fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=[f"<b>{c}</b>" for c in formatted_df.columns],
                        fill_color='rgba(22, 30, 46, 0.96)',
                        font=dict(color='#e2e8f0', size=12),
                        align='left'
                    ),
                    cells=dict(
                        values=[formatted_df[c].tolist() for c in formatted_df.columns],
                        fill_color=[['rgba(11, 18, 30, 0.78)' if i % 2 == 0 else 'rgba(7, 12, 22, 0.86)' for i in range(len(formatted_df))]],
                        font=dict(color='#e2e8f0', size=11),
                        align='left'
                    )
                )])
                table_fig.update_layout(
                    margin=dict(t=10, r=10, l=10, b=10),
                    paper_bgcolor='rgba(9, 13, 24, 0.0)',
                    plot_bgcolor='rgba(10, 18, 32, 0.92)',
                    height=min(700, 36 * (len(formatted_df) + 2))
                )
                st.plotly_chart(table_fig, use_container_width=True)
            except Exception:
                # Fallback to Streamlit dataframe if Plotly table creation fails for any reason
                st.dataframe(formatted_df, use_container_width=True, hide_index=True)
            
        except Exception as e:
            # Fallback dataframe rendering: try Plotly dark table first
            try:
                fallback_fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=[f"<b>{c}</b>" for c in df_display.columns],
                        fill_color='rgba(22, 30, 46, 0.96)',
                        font=dict(color='#e2e8f0', size=12),
                        align='left'
                    ),
                    cells=dict(
                        values=[df_display[c].tolist() for c in df_display.columns],
                        fill_color=[['rgba(11, 18, 30, 0.78)' if i % 2 == 0 else 'rgba(7, 12, 22, 0.86)' for i in range(len(df_display))]],
                        font=dict(color='#e2e8f0', size=11),
                        align='left'
                    )
                )])
                fallback_fig.update_layout(
                    margin=dict(t=10, r=10, l=10, b=10),
                    paper_bgcolor='rgba(9, 13, 24, 0.0)',
                    plot_bgcolor='rgba(10, 18, 32, 0.92)',
                    height=min(700, 36 * (len(df_display) + 2))
                )
                st.plotly_chart(fallback_fig, use_container_width=True)
            except Exception:
                st.dataframe(df_display, use_container_width=True)

    # Kategori ve İstatistik Analizleri
    with st.expander("🏷️ Kategori Analizi ve İstatistikler", expanded=False):
        # Kategori bazında toplamlar
        st.markdown("### 🏷️ Kategori Bazında Toplamlar")
        category_summary = df_display.groupby('Kategori').agg({
            'Güncel Değer': 'sum',
            'Toplam Maliyet': 'sum',
            'Kar/Zarar': 'sum'
        }).round(2)
        
        # Kategori toplamlarına yüzde hesapla
        category_summary['Portföy Payı %'] = (category_summary['Güncel Değer'] / total_current_value * 100).round(2)
        category_summary['Kategori Getiri %'] = ((category_summary['Kar/Zarar'] / category_summary['Toplam Maliyet']) * 100).round(2)
        
        # Sütun isimlerini güncelle
        category_summary.columns = [
            f'Güncel Değer ({target_currency})',
            f'Toplam Maliyet ({target_currency})', 
            f'Kar/Zarar ({target_currency})',
            'Portföy Payı (%)',
            'Kategori Getiri (%)'
        ]
        
        # Render category summary as a dark Plotly table to match dashboard theme
        try:
            cat_fig = go.Figure(data=[go.Table(
                header=dict(
                    values=[f"<b>{c}</b>" for c in category_summary.reset_index().columns],
                    fill_color='rgba(22, 30, 46, 0.96)',
                    font=dict(color='#e2e8f0', size=12),
                    align='left'
                ),
                cells=dict(
                    values=[category_summary.reset_index()[c].tolist() for c in category_summary.reset_index().columns],
                    fill_color=[['rgba(11, 18, 30, 0.78)' if i % 2 == 0 else 'rgba(7, 12, 22, 0.86)' for i in range(len(category_summary))]],
                    font=dict(color='#e2e8f0', size=11),
                    align='left'
                )
            )])
            cat_fig.update_layout(
                margin=dict(t=10, r=10, l=10, b=10),
                paper_bgcolor='rgba(9, 13, 24, 0.0)',
                plot_bgcolor='rgba(10, 18, 32, 0.92)',
                height=min(500, 36 * (len(category_summary) + 2))
            )
            st.plotly_chart(cat_fig, use_container_width=True)
        except Exception:
            st.dataframe(category_summary, use_container_width=True)

        # Özet istatistikler
        st.markdown("### 📋 Özet İstatistikler")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **📊 Portföy Bilgileri:**
            - **Toplam Enstrüman:** {len(df_display)} adet
            - **Karlı Pozisyon:** {len(df_display[df_display['Kar/Zarar'] > 0])} adet
            - **Zararlı Pozisyon:** {len(df_display[df_display['Kar/Zarar'] < 0])} adet
            - **Başa Baş:** {len(df_display[df_display['Kar/Zarar'] == 0])} adet
            """)
        
        with col2:
            # En iyi ve en kötü performans gösteren enstrümanlar
            if len(df_display) > 0:
                best_performer = df_display.loc[df_display['Kar/Zarar %'].idxmax()]
                worst_performer = df_display.loc[df_display['Kar/Zarar %'].idxmin()]
                
                st.markdown(f"""
                **🏆 En İyi Performans:**
                - **{best_performer['Kod']}** ({best_performer['Adı']})
                - Getiri: **{best_performer['Kar/Zarar %']:.2f}%**
                - Değer: **{best_performer['Güncel Değer']:.2f} {target_currency}**
                """)
                
                if best_performer['Kod'] != worst_performer['Kod']:
                    st.markdown(f"""
                    **📉 En Düşük Performans:**
                    - **{worst_performer['Kod']}** ({worst_performer['Adı']})
                    - Getiri: **{worst_performer['Kar/Zarar %']:.2f}%**
                    - Değer: **{worst_performer['Güncel Değer']:.2f} {target_currency}**
                    """)

def show_bist_data_management():
    """BIST hisse yönetimi sekmesini göster"""
    # Modern CSS stilleri
    st.markdown("""
    <style>
    .modern-subheader {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.78) 0%, rgba(15, 23, 42, 0.82) 100%);
        color: var(--text-primary);
        padding: 18px 22px;
        border-radius: 16px;
        font-size: 18px;
        font-weight: 600;
        margin: 18px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.76);
        box-shadow: 0 18px 32px rgba(8, 13, 24, 0.38);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="modern-subheader">📊 BIST Hisse Listesi Yönetimi</div>', unsafe_allow_html=True)
    
    # Mevcut durumu göster
    col1, col2, col3 = st.columns(3)
    
    # Parquet dosyası durumu
    bist_stocks, last_updated = load_bist_stocks_from_parquet()
    
    with col1:
        # Azure blob storage'dan dosya durumunu kontrol et
        if blob_storage and blob_storage.file_exists(BIST_STOCKS_FILE):
            st.metric("📂 Parquet Dosyası", "✅ Mevcut (Azure)", f"{len(bist_stocks)} hisse")
        else:
            st.metric("📂 Parquet Dosyası", "❌ Yok", "0 hisse")
    
    with col2:
        if last_updated:
            try:
                time_ago = datetime.now() - datetime.fromisoformat(last_updated.replace('Z', '+00:00')).replace(tzinfo=None)
                st.metric("🕒 Son Güncelleme", f"{time_ago.days} gün önce", f"{time_ago.seconds//3600} saat")
            except:
                st.metric("🕒 Son Güncelleme", "Bilinmiyor", "")
        else:
            st.metric("🕒 Son Güncelleme", "Bilinmiyor", "")
    
    with col3:
        is_stale = is_bist_data_stale(last_updated, hours=24)
        status = "🔴 Eski" if is_stale else "🟢 Güncel"
        st.metric("📊 Veri Durumu", status, f"24 saat kontrolü")
    
    st.markdown("---")
    
    # Periyodik Güncelleme Ayarları
    with st.expander("⏰ Periyodik Güncelleme Ayarları", expanded=False):
        st.markdown("### 🔄 Otomatik BIST Listesi Güncelleme")

        col1, col2, col3 = st.columns(3)

        # Use the persisted settings from blob/session_state so UI reflects saved job settings
        period_options = ['günlük', 'haftalık', 'aylık']
        # Read the latest settings directly from blob each render
        try:
            current_settings = load_job_settings() or {}
            bist_setting = current_settings.get('bist', {})
        except Exception:
            bist_setting = {}

        period_value = bist_setting.get('period', 'günlük')
        try:
            period_index = period_options.index(period_value)
        except Exception:
            period_index = 0

        with col1:
            period = st.selectbox(
                "📅 Güncelleme Periyodu",
                period_options,
                index=period_index,
                key="bist_update_period"
            )

        with col2:
            tstr = bist_setting.get('time')
            update_time = st.time_input(
                "🕐 Güncelleme Saati",
                value=safe_parse_time(tstr, datetime_time(9, 0)),
                key="bist_update_time"
            )
        
        with col3:
            st.write("") # Boşluk
            if st.button("⚙️ Periyodik Güncellemeyi Ayarla", type="secondary"):
                time_str = update_time.strftime("%H:%M")
                success = setup_bist_periodic_update(period, time_str)
                if success:
                    st.success(f"✅ {period} güncelleme {time_str} saatinde ayarlandı!")
                    # Persist settings
                    try:
                        settings = load_job_settings() or {}
                        settings['bist'] = {
                            'active': True,
                            'period': period,
                            'time': time_str
                        }
                        save_job_settings(settings)
                    except Exception:
                        pass
                    init_bist_scheduler()
                else:
                    st.error("❌ Periyodik güncelleme ayarlanamadı!")
        
        # Mevcut schedule durumunu göster
        if 'bist_schedule' in st.session_state and st.session_state['bist_schedule'].get('active'):
            schedule_info = st.session_state['bist_schedule']
            st.info(f"🔄 Aktif: {schedule_info['period']} güncelleme {schedule_info['time']} saatinde")
            
            if st.button("🛑 Periyodik Güncellemeyi Durdur", type="secondary"):
                schedule.clear('bist')
                st.session_state['bist_schedule']['active'] = False
                try:
                    settings = load_job_settings() or {}
                    settings['bist'] = {'active': False}
                    save_job_settings(settings)
                except Exception:
                    pass
                st.success("✅ Periyodik güncelleme durduruldu!")
        
        # Son güncelleme logları (blob'dan okunur)
        log_file = 'bist_update_log.json'
        try:
            logs = read_logs_from_blob(log_file) or []
            if logs:
                st.markdown("#### 📋 Son Güncelleme Logları")
                log_df = pd.DataFrame(logs[-10:])  # Son 10 log
                log_df['timestamp'] = pd.to_datetime(log_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
                log_df = log_df.rename(columns={
                    'timestamp': 'Tarih/Saat',
                    'stocks_count': 'Hisse Sayısı',
                    'backup_created': 'Backup',
                    'success': 'Başarılı'
                })
                st.dataframe(log_df[['Tarih/Saat', 'Hisse Sayısı', 'Backup', 'Başarılı']], use_container_width=True)
        except Exception:
            pass
    
    st.markdown("---")
    
    # İşlem butonları
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔄 BIST Listesini Güncelle", type="primary"):
            with st.spinner("Yahoo Finance'den BIST hisse listesi çekiliyor..."):
                # Önce backup al
                backup_success = backup_bist_file()
                if backup_success:
                    st.info("✅ Mevcut dosya yedeklendi")
                
                new_stocks = fetch_and_save_bist_stocks()
                if new_stocks:
                    st.success(f"✅ {len(new_stocks)} BIST hissesi güncellendi!")
                    # Cache'i temizle ki yeni veriler görünsün
                    st.cache_data.clear()
    
    with col2:
        if st.button("👁️ Mevcut Listeyi Görüntüle"):
            if bist_stocks:
                st.write(f"**Toplam {len(bist_stocks)} BIST Hissesi:**")
                df_view = pd.DataFrame([
                    {"Kod": code, "Şirket Adı": name}
                    for code, name in sorted(bist_stocks.items())
                ])
                st.dataframe(df_view, use_container_width=True, height=300)
            else:
                st.warning("📊 Henüz BIST hisse verisi yüklenmemiş.")
    
    with col3:
        if st.button("🧹 Cache Temizle", key="clear_bist_cache", help="BIST verilerinin cache'ini temizler"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ BIST cache temizlendi!")
            st.rerun()
    
    with col4:
        if st.button("🗑️ Parquet Dosyasını Sil"):
            try:
                if blob_storage and blob_storage.file_exists(BIST_STOCKS_FILE):
                    blob_storage.delete_file(BIST_STOCKS_FILE)
                    st.success(f"✅ {BIST_STOCKS_FILE} (blob) silindi!")
                    st.cache_data.clear()
                else:
                    st.warning("📂 Silinecek blob dosyası bulunamadı.")
            except Exception as e:
                st.error(f"❌ Blob dosya silme hatası: {str(e)}")
    
    # Bilgilendirme
    with st.expander("ℹ️ BIST Hisse Yönetimi Hakkında"):
        st.markdown("""
        ### 📊 BIST Hisse Listesi Nasıl Çalışır?
        
        1. **Parquet Dosyası**: BIST hisse listesi `bist_stocks.parquet` dosyasında saklanır
        2. **Yahoo Finance**: Hisse listesi Yahoo Finance API'sinden çekilir
        3. **Otomatik Güncelleme**: 24 saatten eski veriler otomatik güncellenir
        4. **Performans**: Parquet formatı sayesinde çok hızlı yüklenir
        
        ### 🔧 Öneriler:
        - Günde bir kez hisse listesini güncelleyin
        - Yeni hisseler için manuel güncelleme yapın
        - Dosya boyutu çok küçük olduğu için performans sorunu yaşamazsınız
        """)

def show_add_transaction():
    """İşlem ekleme formunu göster"""
    # Modern CSS stilleri
    st.markdown("""
    <style>
    .modern-subheader {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.78) 0%, rgba(15, 23, 42, 0.82) 100%);
        color: var(--text-primary);
        padding: 18px 22px;
        border-radius: 16px;
        font-size: 18px;
        font-weight: 600;
        margin: 18px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.76);
        box-shadow: 0 18px 32px rgba(8, 13, 24, 0.38);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="modern-subheader">➕ Yeni İşlem Ekle</div>', unsafe_allow_html=True)
    
    # Form temizlik mekanizması - her başlangıçta sayaçları sıfırla
    if 'form_clear_counter' not in st.session_state:
        st.session_state['form_clear_counter'] = 0
    
    # İşlem başarılıysa form'u temizle
    if 'transaction_success' in st.session_state and st.session_state['transaction_success']:
        st.session_state['active_portfolio_tab'] = 1
        st.session_state['form_clear_counter'] += 1
        st.session_state['transaction_success'] = False
        st.rerun()
    
    # İşlem türü - Modern tasarım
    st.markdown("""
    <style>
    .transaction-type-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        text-align: center;
        font-weight: 600;
        font-size: 16px;
        margin: 20px 0;
        box-shadow: 0 4px 15px 0 rgba(102, 126, 234, 0.3);
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="transaction-type-header">📊 İşlem Türü Seçimi</div>', unsafe_allow_html=True)
    
    # Modern selectbox ile işlem türü seçimi
    transaction_type = st.selectbox(
        "🔄 İşlem Türünü Seçin:",
        options=["İşlem Ekle", "İşlem Çıkar"],
        format_func=lambda x: f"➕ {x}" if x == "İşlem Ekle" else f"➖ {x}",
        help="Yapmak istediğiniz işlem türünü seçin",
        index=0
    )
    
    # Değişkenleri initialize et
    selected_instrument = None
    selected_category = None
    portfolio_instruments = {}
    current_instruments = {}
    instrument_currency = "₺"
    current_price = 100.0
    
    # İşlem Çıkar işlemi için portföy kontrolü
    if transaction_type == "İşlem Çıkar":
        # Kullanıcının portföyündeki pozitif pozisyonları al
        portfolio_data, _, positions = get_portfolio_summary(st.session_state['user_email'])
        
        if not portfolio_data:
            st.warning("⚠️ Portföyünüzde satılabilecek hiç enstrüman bulunmuyor!")
            st.info("💡 Önce 'İşlem Ekle' ile portföyünüze enstrüman ekleyin.")
            return
        
        # Portföydeki enstrümanları dropdown için hazırla
        portfolio_instruments = {}
        for item in portfolio_data:
            code = item['Kod']
            name = item['Adı']
            category = item['Kategori']
            quantity = item['Miktar']
            portfolio_instruments[code] = {
                'name': name,
                'category': category,
                'quantity': quantity
            }
        
        # Portföydeki enstrüman seçimi
        st.markdown("### 📦 Portföyünüzdeki Enstrümanlar")
        
        # Kod ve display listelerini sıralı şekilde oluştur
        portfolio_codes = list(portfolio_instruments.keys())
        portfolio_display_options = []
        for code in portfolio_codes:
            details = portfolio_instruments[code]
            display_text = f"{code} - {details['name']} (Mevcut: {format_quantity_display(details['quantity'], decimals=4)})"
            portfolio_display_options.append(display_text)
        
        selected_display = st.selectbox(
            "🎯 Satılacak Enstrümanı Seçin:",
            options=portfolio_display_options,
            help="Sadece portföyünüzde bulunan enstrümanları satabilirsiniz"
        )
        
        # Seçilen display'in index'ini bul ve ona karşılık gelen kodu al
        if selected_display:
            selected_index = portfolio_display_options.index(selected_display)
            selected_instrument = portfolio_codes[selected_index]
        else:
            selected_instrument = None
        
        if selected_instrument:
            selected_category = portfolio_instruments[selected_instrument]['category']
            max_quantity = portfolio_instruments[selected_instrument]['quantity']
            
            # Seçilen enstrümanın bilgilerini göster
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📊 **Kategori:** {selected_category}")
            with col2:
                if selected_category == "CASH":
                    st.info(f"💰 **Mevcut Tutar:** {max_quantity:.2f}")
                else:
                    st.info(f"📦 **Mevcut Miktar:** {format_quantity_display(max_quantity, decimals=4)}")
            with col3:
                if selected_category != "CASH":
                    current_price = get_current_price(selected_instrument, selected_category)
                    instrument_currency = get_specific_instrument_currency(selected_instrument, selected_category)
                else:
                    st.info("💡 **Nakit:** Fiyat = 1.0")
    
    else:
        # İşlem Ekle işlemi için normal kategori seçimi
        # Kategori seçimi (FOREX hariç)
        current_categories = get_portfolio_allowed_categories()
        
        # Kategori listelerini sıralı şekilde oluştur
        category_keys = list(current_categories.keys())
        category_display_options = [f"{key} - {current_categories[key]['name']}" 
                                    for key in category_keys]
        
        selected_category_display = st.selectbox(
            "📂 Enstrüman Kategorisi:",
            options=category_display_options
        )
        
        # Seçilen kategorinin index'ini bul ve ona karşılık gelen key'i al
        selected_index = category_display_options.index(selected_category_display)
        selected_category = category_keys[selected_index]
        
        # Enstrüman seçimi - Arama özelliği ile
        if selected_category == "TEFAS":
            current_instruments = get_tefas_funds_dynamic()
            current_categories[selected_category]["data"] = current_instruments
        else:
            current_instruments = current_categories[selected_category]["data"]
        
        # Arama kutusu
        search_term = st.text_input(
            f"🔍 {current_categories[selected_category]['name']} Ara:",
            placeholder="Kod veya adı girin...",
            help="Enstrüman kodu veya adını girerek filtreleyebilirsiniz"
        )
        
        # Filtreleme uygula
        if search_term:
            filtered_instruments = {}
            search_lower = search_term.lower()
            for code, name in current_instruments.items():
                if (search_lower in code.lower() or 
                    search_lower in name.lower()):
                    filtered_instruments[code] = name
        else:
            filtered_instruments = current_instruments
        
        # Bulunan sonuç sayısını göster
        if search_term:
            st.caption(f"🔍 {len(filtered_instruments)} sonuç bulundu")
            
            if "PPK" in search_term.upper():
                st.info(f"🔍 Bulunan PPK fonları:")
                for code, name in list(filtered_instruments.items())[:10]:  # İlk 10 tanesini göster
                    st.write(f"• {code}: {name}")
        
        # Enstrüman seçimi
        if filtered_instruments:
            # Kod listesini ve display listesini sıralı şekilde oluştur
            codes_list = list(filtered_instruments.keys())
            names_list = list(filtered_instruments.values())
            display_options = [f"{code} - {name}" for code, name in zip(codes_list, names_list)]
            
            # Selectbox'ı display string'ler ile göster
            selected_display = st.selectbox(
                f"🎯 {current_categories[selected_category]['name']} Seçin:",
                options=display_options,
                help=f"Toplam {len(filtered_instruments)} enstrüman mevcut"
            )
            
            # Seçilen display'in index'ini bul ve ona karşılık gelen kodu al
            if selected_display:
                selected_index = display_options.index(selected_display)
                selected_instrument = codes_list[selected_index]
            else:
                selected_instrument = None
        else:
            st.warning("❌ Arama kriterinize uygun enstrüman bulunamadı. Lütfen farklı bir terim deneyin.")
            selected_instrument = None
        
        # Seçilen enstrümanın güncel fiyatını ve para birimini al
        if selected_instrument:
            with st.spinner("📊 Güncel fiyat bilgisi alınıyor..."):
                current_price = get_current_price(selected_instrument, selected_category)
                instrument_currency = get_specific_instrument_currency(selected_instrument, selected_category)
        else:
            current_price = 100.0
            instrument_currency = "₺" if selected_category == "BIST" else "$"
    
    # İşlem detayları
    col1, col2 = st.columns(2)
    
    with col1:
        transaction_date = st.date_input(
            "📅 İşlem Tarihi:",
            value=datetime.now(),
            max_value=datetime.now(),
            format="DD/MM/YYYY"
        )
        
        # Nakit para birimleri için özel form
        if selected_category == "CASH":
            # Nakit için sadece tutar girişi
            total_amount = st.number_input(
                "💰 Nakit Tutarı:",
                min_value=0.01,
                value=1000.0,
                step=0.01,
                format="%.2f",
                help="Portföyünüze eklemek/çıkarmak istediğiniz nakit tutarı"
            )
            
            # Para birimini enstrüman kodundan belirle
            if selected_instrument:
                instrument_currency = get_specific_instrument_currency(selected_instrument, selected_category)
            else:
                instrument_currency = "₺"
            
            # Nakit için miktar = tutar, fiyat = 1.0
            quantity = total_amount
            price = 1.0
            currency = instrument_currency
            
            # Bilgi göster
            st.info(f"💡 **{filtered_instruments.get(selected_instrument, selected_instrument)}** olarak kaydedilecek")
            
        else:
            # Normal enstrümanlar için miktar girişi - Sat işlemi için maksimum kontrolü
            if transaction_type == "İşlem Çıkar" and selected_instrument and selected_instrument in portfolio_instruments:
                max_quantity = portfolio_instruments[selected_instrument]['quantity']
                sell_category = portfolio_instruments[selected_instrument]['category']
                
                if sell_category == "CASH":
                    # Nakit satışı için tutar girişi
                    quantity = st.number_input(
                        f"Çekilecek Tutar (Maks: {max_quantity:.2f}):",
                        min_value=0.01,
                        max_value=float(max_quantity),
                        value=min(1000.0, float(max_quantity)),
                        step=0.01,
                        format="%.2f",
                        help=f"Portföyünüzde {max_quantity:.2f} tutar bulunuyor"
                    )
                    price = 1.0
                    currency = get_specific_instrument_currency(selected_instrument, sell_category)
                else:
                    # Normal enstrüman satışı
                    quantity = st.number_input(
                        f"�📦 Satış Miktarı (Maks: {format_quantity_display(max_quantity, decimals=4)}):",
                        min_value=0.0001,
                        max_value=float(max_quantity),
                        value=min(1.0, float(max_quantity)),
                        step=0.0001,
                        format="%.4f",
                        help=f"Portföyünüzde {format_quantity_display(max_quantity, decimals=4)} adet bulunuyor"
                    )
                
                # Miktar kontrolü uyarısı
                if quantity > max_quantity:
                    if sell_category == "CASH":
                        st.error(f"❌ Portföyünüzde sadece {max_quantity:.2f} tutar bulunuyor!")
                    else:
                        st.error(f"❌ Portföyünüzde sadece {format_quantity_display(max_quantity, decimals=4)} adet bulunuyor!")
                elif quantity == max_quantity:
                    st.warning("⚠️ Tüm pozisyonunuzu kapatıyorsunuz.")
                else:
                    remaining = max_quantity - quantity
                    if sell_category == "CASH":
                        st.info(f"📊 Çekim sonrası kalan: {remaining:.2f}")
                    else:
                        st.info(f"📊 Satış sonrası kalan: {format_quantity_display(remaining, decimals=4)} adet")
            else:
                # Normal miktar girişi (İşlem Ekle için)
                quantity = st.number_input(
                    "📦 Miktar:",
                    min_value=0.0001,
                    value=1.0,
                    step=0.0001,
                    format="%.4f"
                )
    
    # Tarih değiştiğinde fiyatı güncelle (nakit dışındaki enstrümanlar için)
    if selected_instrument and transaction_date and selected_category != "CASH":
        is_today = transaction_date == datetime.now().date()
        
        # Güncel fiyat ve para birimi bilgilerini al
        if transaction_type == "İşlem Çıkar":
            # İşlem Çıkar işlemi için zaten yukarıda alınmış
            pass
        else:
            # İşlem Ekle için güncel fiyat al
            if not 'current_price' in locals() or not 'instrument_currency' in locals():
                current_price = get_current_price(selected_instrument, selected_category)
                instrument_currency = get_specific_instrument_currency(selected_instrument, selected_category)
        
        if is_today:
            # Bugün için güncel fiyat
            price_for_date = current_price
            price_info = f"📊 Güncel piyasa fiyatı"
            price_status = "success"
        else:
            # Geçmiş tarih için o tarihteki fiyat
            with st.spinner(f"📈 {transaction_date} tarihindeki fiyat alınıyor..."):
                price_for_date = get_historical_price(selected_instrument, selected_category, transaction_date)
                price_info = f"📅 {transaction_date} tarihindeki fiyat"
            
            if price_for_date > 0:
                price_status = "success"
            else:
                price_status = "warning"
        
        if price_for_date > 0:
            if price_status != "success":
                st.warning(f"⚠️ Geçmiş fiyat sistemi tarafından bulunamadı. Lütfen manuel olarak girin:")
            default_price = price_for_date
        else:
            st.warning(f"⚠️ {transaction_date} tarihi için fiyat bilgisi bulunamadı.\n💡 Lütfen aşağıda manuel olarak fiyat giriniz.")
            default_price = current_price if current_price > 0 else 100.0
    else:
        if 'current_price' in locals():
            default_price = current_price if current_price > 0 else 100.0
        else:
            default_price = 100.0
        if 'instrument_currency' not in locals():
            instrument_currency = "₺"
    
    with col2:
        # Nakit para birimleri için fiyat ve para birimi girişi gösterme
        if selected_category != "CASH":
            # Tarihe göre fiyatı varsayılan değer olarak kullan
            if 'price_status' in locals() and price_status == "warning":
                help_text = "API'den fiyat bulunamadı. Lütfen o tarihteki gerçek fiyatı giriniz."
            elif default_price > 0:
                help_text = f"Önerilen fiyat: {default_price:.6f} {instrument_currency} (İstenirse değiştirebilirsiniz)"
            else:
                help_text = "Lütfen işlem fiyatını giriniz"
            
            price = st.number_input(
                "💰 Birim Fiyat:",
                min_value=0.000001,
                value=float(default_price),
                step=0.00000001,
                help=help_text
            )
            
            # Para birimini enstrümanın para birimine göre ayarla
            currency_options = ["₺", "$", "€", "£"]
            default_currency_index = 0  # ₺
            
            if instrument_currency in currency_options:
                default_currency_index = currency_options.index(instrument_currency)
            
            currency = st.selectbox(
                "💱 Para Birimi:",
                options=currency_options,
                index=default_currency_index,
                help=f"Önerilen para birimi: {instrument_currency}"
            )
        else:
            # Nakit için para birimi bilgisini göster (salt okunur)
            st.info(f"💱 **Para Birimi:** {currency}")
            st.caption("💡 Nakit işlemlerinde birim fiyat her zaman 1.0'dır")
    
    # Toplam değer hesaplama
    total_value = quantity * price
    
    if selected_category == "CASH":
        st.success(f"💰 **Nakit Tutarı:** {total_value:.2f} {currency}")
    else:
        st.info(f"💵 Toplam İşlem Değeri: {total_value:.2f} {currency}")
    
    # İşlem ekleme butonu
    button_key = f"save_transaction_{transaction_type}_{selected_instrument}_{st.session_state['form_clear_counter']}"
    button_label = "➕ İşlem Ekle" if transaction_type == "İşlem Ekle" else "➖ İşlem Çıkar"
    if st.button(button_label, type="primary", key=button_key):
        try:
            # Sat işlemi için miktar kontrolü
            if transaction_type == "İşlem Çıkar" and selected_instrument in portfolio_instruments:
                max_quantity = portfolio_instruments[selected_instrument]['quantity']
                if quantity > max_quantity:
                    st.error(f"❌ Hata: Portföyünüzde sadece {format_quantity_display(max_quantity, decimals=4)} adet bulunuyor!")
                    return
            
            # Fiyat validasyonu - İlgili tarihte fiyat var mı kontrol et
            price_validation_result = validate_price_for_date(selected_instrument, selected_category, transaction_date, price)
            
            # Hata mesajı varsa uyarı olarak göster (artık engellemez)
            if price_validation_result["error_message"]:
                st.warning(price_validation_result["error_message"])
            
            # Fiyat önerisi farklıysa kullanıcıyı bilgilendir
            if price_validation_result["suggested_price"] and abs(price - price_validation_result["suggested_price"]) > 0.01:
                st.info(f"💡 Önerilen fiyat: {price_validation_result['suggested_price']:.2f} {instrument_currency}, "
                       f"Girilen: {price:.2f} {instrument_currency}")
            
            trans_type = "BUY" if transaction_type == "İşlem Ekle" else "SELL"
            
            # Enstrüman adını al
            if transaction_type == "İşlem Çıkar":
                instrument_name = portfolio_instruments[selected_instrument]['name']
            else:
                instrument_name = current_instruments.get(selected_instrument, selected_instrument)
            
            success = add_transaction(
                st.session_state['user_email'],
                trans_type,
                selected_instrument,
                instrument_name,
                selected_category,
                quantity,
                price,
                currency,
                transaction_date
            )
            
            if success:
                st.success(f"✅ {transaction_type} işlemi başarıyla kaydedildi!")
                
                # Sat işlemi için ek bilgi
                if transaction_type == "İşlem Çıkar":
                    remaining = portfolio_instruments[selected_instrument]['quantity'] - quantity
                    if remaining <= 0.0001:  # Neredeyse sıfır
                        st.info("📊 Bu enstrümandaki pozisyonunuz tamamen kapatıldı.")
                    else:
                        st.info(f"📊 Kalan pozisyon: {format_quantity_display(remaining, decimals=4)} adet")
                
                # Form'u temizlemek için success flag set et
                st.session_state['active_portfolio_tab'] = 1
                st.session_state['transaction_success'] = True
                
            else:
                st.error("❌ İşlem kaydedilirken bir hata oluştu!")
                
        except Exception as e:
            st.error(f"❌ Hata: {str(e)}")
    
    # Sidebar alt butonları
    show_sidebar_bottom_buttons_portfolio()

def show_transaction_history():
    """İşlem geçmişini göster"""
    st.subheader("📋 İşlem Geçmişi")
    
    portfolios = load_portfolios()
    user_email = st.session_state['user_email']
    
    if user_email in portfolios and portfolios[user_email]['transactions']:
        transactions = portfolios[user_email]['transactions']
        
        # İşlem geçmişini DataFrame'e çevir
        df = pd.DataFrame(transactions)
        df = df.sort_values('created_at', ascending=False)
        
        # Görüntüleme için sütun düzenlemesi
        display_columns = [
            'id', 'date', 'type', 'instrument_code', 'instrument_name', 
            'category', 'quantity', 'price', 'currency', 'total_value'
        ]
        
        display_df = df[display_columns].copy()
        display_df.columns = [
            'ID', 'Tarih', 'İşlem', 'Kod', 'Enstrüman', 
            'Kategori', 'Miktar', 'Fiyat', 'Para Birimi', 'Toplam'
        ]
        
        # Tarih formatını standartlaştır (YYYY-MM-DD'den DD/MM/YYYY'ye)
        def format_date(date_str):
            try:
                if isinstance(date_str, str):
                    if '/' in date_str:
                        # Zaten DD/MM/YYYY formatında
                        return date_str
                    elif '-' in date_str:
                        # YYYY-MM-DD formatından DD/MM/YYYY'ye dönüştür
                        d = datetime.strptime(date_str, '%Y-%m-%d')
                        return d.strftime('%d/%m/%Y')
                return date_str
            except:
                return date_str
        
        display_df['Tarih'] = display_df['Tarih'].apply(format_date)
        
        # İşlem türünü Türkçe'ye çevir
        display_df['İşlem'] = display_df['İşlem'].map({'BUY': '🟢 Alış', 'SELL': '🔴 Satış'})
        
        # İşlem tablosunu göster (dark Plotly table for consistent theme)
        try:
            header_vals = list(display_df.columns)
            cell_vals = [display_df[col].astype(str).tolist() for col in display_df.columns]
            table_fig = go.Figure(data=[go.Table(
                header=dict(values=header_vals,
                            fill_color='rgba(22, 30, 46, 0.96)',
                            font=dict(color='#e2e8f0', size=12),
                            align='left'),
                cells=dict(values=cell_vals,
                           fill_color='rgba(11, 18, 30, 0.78)',
                           font=dict(color='#e2e8f0', size=11),
                           align='left')
            )])
            table_fig.update_layout(
                margin=dict(l=8, r=8, t=8, b=8),
                paper_bgcolor='rgba(9,13,24,0.0)',
                plot_bgcolor='rgba(10,18,32,0.92)',
                height=min(700, 36 * (len(display_df) + 2))
            )
            st.plotly_chart(table_fig, use_container_width=True)
        except Exception:
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Toplu silme seçenekleri
        st.markdown("---")
        st.subheader("🗑️ Toplu Silme Seçenekleri")
        
        st.info("""
        ℹ️ **Önemli Bilgiler:**
        - İşlem silme işlemi **geri alınamaz**
        - Silinen işlemler portföy hesaplamalarından otomatik olarak çıkarılır
        - Portföy değerleriniz yeniden hesaplanır
        - Bu işlem sadece işlem geçmişinizi etkiler, gerçek yatırımlarınızı etkilemez
        """)
        
        col_del1, col_del2 = st.columns([1, 1])
        
        with col_del1:
            # Tüm işlemleri silme
            total_transactions = len(display_df)
            if st.button(f"🗑️ Tüm İşlem Geçmişini Sil ({total_transactions} işlem)", type="secondary", help="Portföyünüzdeki tüm işlemleri siler"):
                if st.session_state.get('confirm_delete_all', False):
                    # Onay verildi, işlemleri sil
                    success = delete_all_transactions(user_email)
                    if success:
                        st.success("✅ Tüm işlem geçmişi başarıyla silindi!")
                        st.success("🔄 Portföy sıfırlandı - Tüm pozisyonlar temizlendi!")
                        st.session_state['confirm_delete_all'] = False
                        # İşlem Geçmişi sekmesinde kalmak için özel rerun
                        st.session_state.active_portfolio_tab = 2  # İşlem Geçmişi sekmesi
                        st.rerun()
                    else:
                        st.error("❌ İşlem geçmişi silinirken hata oluştu!")
                else:
                    # Onay iste
                    st.session_state['confirm_delete_all'] = True
                    st.warning("⚠️ Bu işlem geri alınamaz! Tüm işlem geçmişiniz silinecek.")
                    st.warning(f"📊 Silinecek: {total_transactions} işlem")
        
        with col_del2:
            st.info("� Sadece toplu silme seçeneği mevcuttur")
        
        # Onay iptal etme butonu
        if st.session_state.get('confirm_delete_all', False):
            st.markdown("**🚨 Onay Bekleniyor:**")
            if st.button("❌ Tüm Silme İşlemini İptal Et", type="primary"):
                st.session_state['confirm_delete_all'] = False
                # İşlem Geçmişi sekmesinde kalmak için özel rerun
                st.session_state.active_portfolio_tab = 2  # İşlem Geçmişi sekmesi
                st.rerun()
            
        # Onay iptal etme butonları için ayrı satır
        if st.session_state.get('confirm_delete_filtered', False):
            if st.button("❌ Filtrelenmiş Silme İşlemini İptal Et", type="primary"):
                st.session_state['confirm_delete_filtered'] = False
                # İşlem Geçmişi sekmesinde kalmak için özel rerun
                st.session_state.active_portfolio_tab = 2  # İşlem Geçmişi sekmesi
                st.rerun()
        
        # Kalem bazlı işlem silme - Her satırda buton
        st.markdown("---")
        st.subheader("📋 İşlem Listesi")
        
        if len(display_df) == 0:
            st.info(" Henüz hiç işlem yapmadınız. 'İşlem Ekle' sekmesinden başlayabilirsiniz!")
        else:
            # İşlem sayısını göster
            st.caption(f"📊 Toplam {len(display_df)} işlem görüntüleniyor")
            
            # Her işlem için satır satır gösterim
        for idx, row in display_df.iterrows():
            # Container ile daha güzel görünüm
            with st.container():
                col_info, col_buttons = st.columns([10, 2])
                
                with col_info:
                    # İşlem bilgilerini detaylı ve okunaklı şekilde göster
                    header_text = f"**#{row['ID']}** | {row['Tarih']} | {row['İşlem']} | **{row['Kod']}** ({row['Enstrüman']})"

                    if st.session_state.get(f"confirm_delete_{row['ID']}", False):
                        # Onay bekleyen işlem için uyarı stili
                        st.error(f"⚠️ **SİLİNECEK:** {header_text}")
                        # Detayları yine göster (onay ekranında da görünür olsun)
                        d1, d2, d3 = st.columns([1, 1, 1])
                        with d1:
                            st.markdown(f"**Birim Fiyat:** {row['Fiyat']:,.4f} {row['Para Birimi']}")
                            st.markdown(f"**Miktar:** {format_quantity_display(row['Miktar'], decimals=4)}")
                        with d2:
                            st.markdown(f"**Toplam:** {row['Toplam']:,.2f} {row['Para Birimi']}")
                            st.markdown(f"**Kategori:** {row['Kategori']}")
                        with d3:
                            st.markdown(f"**Enstrüman:** {row['Enstrüman']}")
                            st.markdown(f"**Kod:** {row['Kod']}")
                    else:
                        # Normal işlem görünümü - renk kodlu başlık, detaylar alt satırlarda
                        if row['İşlem'] == '🟢 Alış':
                            st.success(header_text)
                        else:
                            st.error(header_text)

                        # Detayları üç sütunda göster: birim fiyat, miktar, toplam ve ek bilgiler
                        d1, d2, d3 = st.columns([1, 1, 1])
                        with d1:
                            st.markdown(f"**Birim Fiyat:** {row['Fiyat']:,.4f} {row['Para Birimi']}")
                        with d2:
                            st.markdown(f"**Miktar:** {format_quantity_display(row['Miktar'], decimals=4)}")
                            st.markdown(f"**Toplam:** {row['Toplam']:,.2f} {row['Para Birimi']}")
                        with d3:
                            st.markdown(f"**Kategori:** {row['Kategori']}")
                            st.markdown(f"**Enstrüman:** {row['Enstrüman']}")
                
                with col_buttons:
                    # Butonları yan yana getir
                    btn_col1, btn_col2 = st.columns(2)
                    
                    with btn_col1:
                        # Her işlem için benzersiz buton key'i
                        delete_key = f"delete_{row['ID']}_{idx}"
                        
                        # Onay bekleyen işlemler için farklı stil
                        if st.session_state.get(f"confirm_delete_{row['ID']}", False):
                            button_label = "⚠️"
                            button_type = "primary"
                            button_help = f"#{row['ID']} işlemini silmek için tekrar tıklayın"
                        else:
                            button_label = "🗑️"
                            button_type = "secondary"
                            button_help = f"#{row['ID']} numaralı işlemi sil"
                        
                        if st.button(button_label, key=delete_key, help=button_help, type=button_type):
                            # Onay mekanizması için session state key'i
                            confirm_key = f"confirm_delete_{row['ID']}"
                            
                            if st.session_state.get(confirm_key, False):
                                # Onay verildi, işlemi sil
                                success = delete_transactions_by_ids(user_email, [row['ID']])
                                if success:
                                    st.success(f"✅ İşlem #{row['ID']} başarıyla silindi!")
                                    # Onay state'ini temizle
                                    if confirm_key in st.session_state:
                                        del st.session_state[confirm_key]
                                    # İşlem Geçmişi sekmesinde kalmak için özel rerun
                                    st.session_state.active_portfolio_tab = 2  # İşlem Geçmişi sekmesi
                                    st.rerun()
                                else:
                                    st.error("❌ İşlem silinirken hata oluştu!")
                            else:
                                # Onay iste
                                st.session_state[confirm_key] = True
                                st.rerun()
                    
                    with btn_col2:
                        # İptal butonu - onay bekleyen işlemlerde görünür
                        if st.session_state.get(f"confirm_delete_{row['ID']}", False):
                            cancel_key = f"cancel_{row['ID']}_{idx}"
                            if st.button("❌", key=cancel_key, help="Silme işlemini iptal et", type="primary"):
                                if f"confirm_delete_{row['ID']}" in st.session_state:
                                    del st.session_state[f"confirm_delete_{row['ID']}"]
                                # İşlem Geçmişi sekmesinde kalmak için özel rerun
                                st.session_state.active_portfolio_tab = 2  # İşlem Geçmişi sekmesi
                                st.rerun()
                        else:
                            # Boş alan (normal durumlarda iptal butonu yok)
                            st.write("")
                
                # Ayırıcı çizgi
                st.markdown("---")
        
        # Özet istatistikler
        st.subheader("📊 İşlem Özeti")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_transactions = len(display_df)
            st.metric("📊 Toplam İşlem", total_transactions)
        
        with col2:
            buy_count = len(display_df[display_df['İşlem'] == '🟢 Alış'])
            st.metric("🟢 Alış İşlemi", buy_count)
        
        with col3:
            sell_count = len(display_df[display_df['İşlem'] == '🔴 Satış'])
            st.metric("🔴 Satış İşlemi", sell_count)
        
        with col4:
            unique_instruments = display_df['Kod'].nunique()
            st.metric("🎯 Farklı Enstrüman", unique_instruments)
        
    else:
        st.info("📝 Henüz hiç işlem yapmadınız. 'İşlem Ekle' sekmesinden başlayabilirsiniz!")
    
    # Sidebar alt butonları
    show_sidebar_bottom_buttons_portfolio()

def show_nasdaq_data_management():
    """NASDAQ hisse yönetimi sekmesi"""
    st.subheader("🏛️ NASDAQ Hisse Yönetimi")
    st.markdown("NASDAQ hisse listesini dinamik olarak yönetin ve Parquet dosyasına kaydedin.")
    
    # Mevcut durum
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Mevcut Durum")

        # Azure blob storage varlığını kontrol et (lokal dosya kullanımı kaldırıldı)
        azure_exists = blob_storage and blob_storage.file_exists(NASDAQ_STOCKS_FILE)

        if azure_exists:
            stocks_dict, last_updated = load_nasdaq_stocks_from_parquet()

            if stocks_dict:
                st.success(f"✅ {len(stocks_dict)} NASDAQ hissesi mevcut (Azure)")

                if last_updated:
                    try:
                        if isinstance(last_updated, str):
                            update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        else:
                            update_time = last_updated
                        st.info(f"🕒 Son güncelleme: {update_time.strftime('%Y-%m-%d %H:%M:%S')}")

                        # Veri yaşını göster
                        age_hours = (datetime.now() - update_time).total_seconds() / 3600
                        if age_hours < 24:
                            pass
                        else:
                            st.warning(f"🟡 Veri eski ({age_hours/24:.1f} gün önce)")
                    except:
                        st.warning("⚠️ Güncelleme tarihi okunamadı")

                # İlk 10 hisseyi göster
                st.markdown("**📝 İlk 10 Hisse:**")
                sample_stocks = dict(list(stocks_dict.items())[:10])
                for symbol, name in sample_stocks.items():
                    st.write(f"• {symbol}: {name}")

                if len(stocks_dict) > 10:
                    st.write(f"... ve {len(stocks_dict)-10} hisse daha")
            else:
                st.warning("⚠️ Parquet dosyası mevcut ama boş")
        else:
            st.warning("⚠️ NASDAQ hisse dosyası mevcut değil")
    
    with col2:
        st.markdown("### 🚀 Veri İşlemleri")
        
        # Buton satırı
        col1, col2 = st.columns(2)
        
        with col1:
            # Yeni veri çekme butonu
            if st.button("📈 NASDAQ Hisselerini Çek ve Kaydet", type="primary", key="nasdaq_fetch"):
                # Session state ile tekrar çekmeyi önle
                if 'nasdaq_fetch_completed' not in st.session_state:
                    st.session_state.nasdaq_fetch_completed = False
                
                if not st.session_state.nasdaq_fetch_completed:
                    with st.spinner("🔄 NASDAQ hisseleri çekiliyor ve güncelleniyor..."):
                        # Önce cache'i temizle
                        st.cache_data.clear()
                        st.info("🧹 Cache temizlendi, yeni veriler çekiliyor...")
                        
                        result = fetch_and_save_nasdaq_stocks()
                        if result:
                            st.success("🎉 İşlem tamamlandı! Güncel veriler kullanıma hazır.")
                            st.balloons()
                            st.session_state.nasdaq_fetch_completed = True
                            # 2 saniye bekle ve sayfayı yenile
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("❌ NASDAQ hisseler çekilemedi!")
                else:
                    st.info("⚠️ Veriler zaten çekildi.")
        
        with col2:
            # Cache temizleme butonu
            if st.button("🧹 Cache Temizle", key="clear_cache", help="NASDAQ verilerinin cache'ini temizler"):
                st.cache_data.clear()

        # Son güncelleme logları (blob'dan okunur) - benzer görünüm BIST ile
        nasdaq_log_file = 'nasdaq_update_log.json'
        try:
            nasdaq_logs = read_logs_from_blob(nasdaq_log_file) or []
            if nasdaq_logs:
                st.markdown("#### 📋 Son Güncelleme Logları")
                nlog_df = pd.DataFrame(nasdaq_logs[-10:])  # Son 10 log
                nlog_df['timestamp'] = pd.to_datetime(nlog_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
                nlog_df = nlog_df.rename(columns={
                    'timestamp': 'Tarih/Saat',
                    'stocks_count': 'Hisse Sayısı',
                    'backup_created': 'Backup',
                    'success': 'Başarılı'
                })
                st.dataframe(nlog_df[['Tarih/Saat', 'Hisse Sayısı', 'Backup', 'Başarılı']], use_container_width=True)
        except Exception:
            pass
        
        st.markdown("---")
        
        # Mevcut verileri görüntüleme
        if st.button("📋 Tüm NASDAQ Hisselerini Görüntüle", key="nasdaq_show_all"):
            stocks_dict, _ = load_nasdaq_stocks_from_parquet()
            if stocks_dict:
                st.subheader(f"📊 Tüm NASDAQ Hisseleri ({len(stocks_dict)} adet)")
                
                # Arama kutusu
                search_nasdaq = st.text_input("🔍 NASDAQ Hissesi Ara:", placeholder="Sembol veya şirket adı...", key="nasdaq_search")
                
                if search_nasdaq:
                    filtered_nasdaq = {k: v for k, v in stocks_dict.items() 
                                     if search_nasdaq.upper() in k.upper() or 
                                        search_nasdaq.lower() in v.lower()}
                    if filtered_nasdaq:
                        st.success(f"🔍 {len(filtered_nasdaq)} sonuç bulundu")
                        for symbol, name in filtered_nasdaq.items():
                            st.write(f"**{symbol}**: {name}")
                    else:
                        st.warning("❌ Arama kriterine uygun hisse bulunamadı")
                else:
                    # Tüm hisseleri kategorilere ayırarak göster
                    df_stocks = pd.DataFrame(list(stocks_dict.items()), columns=['Sembol', 'Şirket Adı'])
                    st.dataframe(df_stocks, use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ Henüz NASDAQ hisse verisi yok")
        
        st.markdown("---")
        
        # Dosya silme
        if st.button("🗑️ NASDAQ Veri Dosyasını Sil", type="secondary", key="nasdaq_delete"):
            if blob_storage and blob_storage.file_exists(NASDAQ_STOCKS_FILE):
                try:
                    blob_storage.delete_file(NASDAQ_STOCKS_FILE)
                    st.success(f"✅ {NASDAQ_STOCKS_FILE} blob'dan silindi!")
                    # Cache'i temizle
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ Blob dosyası silinirken hata: {str(e)}")
            else:
                st.warning("⚠️ Silinecek blob dosyası bulunamadı")
    
    # Periyodik Güncelleme Ayarları
    st.markdown("---")
    with st.expander("🔄 Periyodik Güncelleme Ayarları"):
        st.markdown("### ⏰ Otomatik NASDAQ Güncelleme")
        
        # Güncelleme periyodu seçimi (oku: blob)
        period_options = ['günlük', 'haftalık', 'aylık']
        try:
            current_settings = load_job_settings() or {}
            nasdaq_setting = current_settings.get('nasdaq', {})
        except Exception:
            nasdaq_setting = {}

        period_value = nasdaq_setting.get('period', 'günlük')
        try:
            period_index = period_options.index(period_value)
        except Exception:
            period_index = 0

        col1, col2 = st.columns(2)

        with col1:
            update_period = st.selectbox(
                "📅 Güncelleme Periyodu:",
                options=period_options,
                index=period_index,
                key="nasdaq_period_select"
            )

        tstr = nasdaq_setting.get('time')
        update_time = st.time_input(
            "🕒 Güncelleme Saati:",
            value=safe_parse_time(tstr, datetime_time(9, 0)),
            key="nasdaq_time_select"
        )
        
        # Setup butonu
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚀 Periyodik Güncellemeyi Başlat", type="primary", key="nasdaq_setup_periodic"):
                success = setup_nasdaq_periodic_update(update_period, update_time)
                if success:
                    st.session_state['nasdaq_scheduler_active'] = True
                    st.session_state['nasdaq_update_period'] = update_period
                    st.session_state['nasdaq_update_time'] = update_time
                    st.success(f"✅ NASDAQ periyodik güncelleme ayarlandı! ({update_period} - {update_time})")
                    # Persist job settings
                    try:
                        settings = load_job_settings() or {}
                        settings['nasdaq'] = {
                            'active': True,
                            'period': update_period,
                            'time': update_time.strftime('%H:%M:%S') if hasattr(update_time, 'strftime') else str(update_time)
                        }
                        save_job_settings(settings)
                    except Exception:
                        pass
                    # Do not force an immediate rerun here to avoid refresh loops.
                    # The UI will update naturally on the next user interaction.
                else:
                    st.error("❌ Periyodik güncelleme ayarlanırken hata oluştu")
        
        with col2:
            if st.button("⏹️ Periyodik Güncellemeyi Durdur", key="nasdaq_stop_periodic"):
                # NASDAQ schedule'larını temizle
                schedule.clear('nasdaq')
                st.session_state['nasdaq_scheduler_active'] = False
                st.success("⏹️ NASDAQ periyodik güncelleme durduruldu")
                try:
                    settings = load_job_settings() or {}
                    settings['nasdaq'] = {
                        'active': False
                    }
                    save_job_settings(settings)
                except Exception:
                    pass
                st.rerun()
        
        # Aktif schedule bilgisi
        if st.session_state.get('nasdaq_scheduler_active', False):
            period = st.session_state.get('nasdaq_update_period', 'Bilinmiyor')
            time_str = str(st.session_state.get('nasdaq_update_time', 'Bilinmiyor'))
            st.info(f"🟢 **Aktif Schedule:** {period} güncelleme, saat {time_str}")
        else:
            st.warning("🔴 Periyodik güncelleme aktif değil")
        
        # Son job update'lerini blob'dan oku ve tablo olarak göster (BIST ile uyumlu)
        try:
            nasdaq_log_file = 'nasdaq_update_log.json'
            nasdaq_logs = read_logs_from_blob(nasdaq_log_file) or []
            if nasdaq_logs:
                st.markdown('#### 📋 Son Güncelleme Logları')
                nlog_df = pd.DataFrame(nasdaq_logs[-10:])
                nlog_df['timestamp'] = pd.to_datetime(nlog_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
                nlog_df = nlog_df.rename(columns={
                    'timestamp': 'Tarih/Saat',
                    'stocks_count': 'Hisse Sayısı',
                    'backup_created': 'Backup',
                    'success': 'Başarılı'
                })
                cols = [c for c in ['Tarih/Saat', 'Hisse Sayısı', 'Backup', 'Başarılı'] if c in nlog_df.columns]
                if cols:
                    st.dataframe(nlog_df[cols], use_container_width=True)
        except Exception:
            pass

def show_tefas_data_management():
    """TEFAS veri yönetimi sekmesini göster"""
    st.header("TEFAS Hızlı Veri Yönetimi (Parquet)")
    st.markdown("Bu bölümde TEFAS fonlarının verilerini **Parquet formatında** saklayabilir ve yönetebilirsiniz.")
    st.info("⚡ **Parquet format ile 10-50x daha hızlı** okuma/yazma performansı!")
    
    # Alt sekmeler
    ttab1, ttab2 = st.tabs([
        "📥 Veri İndirme", 
        "📊 Parquet Verileri"
    ])
    
    with ttab1:
        show_tefas_data_download()
    
    with ttab2:
        show_tefas_parquet_viewer()
        show_tefas_statistics()

def show_tefas_data_download():
    """TEFAS veri indirme sekmesi"""
    st.subheader("📥 TEFAS Verilerini İndir")
    
    # Tarih aralığı seçimi
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "📅 Başlangıç Tarihi:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now(),
            format="DD/MM/YYYY",
            help="TEFAS verilerini çekmek istediğiniz başlangıç tarihi"
        )
    
    with col2:
        end_date = st.date_input(
            "📅 Bitiş Tarihi:",
            value=datetime.now(),
            max_value=datetime.now(),
            format="DD/MM/YYYY",
            help="TEFAS verilerini çekmek istediğiniz bitiş tarihi"
        )
    
    # Bilgi kutusu
    st.info("📊 Tüm TEFAS fonları otomatik olarak çekilip Excel'e kaydedilecek")
    
    # Tahmini süre hesaplama
    estimated_time = ((end_date - start_date).days + 1) * 2  # Dakika (tüm fonlar için)
    st.warning(f"⏱️ Tahmini süre: {estimated_time:.0f} dakika (1800+ fon)")
    
    # Veri çekme butonu
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("🚀 TÜM TEFAS Verilerini İndir ve Excel'e Kaydet", type="primary"):
            if start_date > end_date:
                st.error("❌ Başlangıç tarihi bitiş tarihinden sonra olamaz!")
            else:
                with st.spinner("Tüm TEFAS verileri çekiliyor ve Parquet'e kaydediliyor (10x hızlı)..."):
                    success = update_tefas_data_to_parquet(start_date, end_date, selected_funds=None)
                    
                    if success:
                        st.success(f"✅ Tüm TEFAS fonları için {start_date} - {end_date} arası veriler başarıyla Excel'e kaydedildi!")
                        
                        # Excel dosya bilgisi
                        if tefas_dm.blob_storage.file_exists(TEFAS_DATA_FILE):
                            try:
                                content = tefas_dm.blob_storage.download_file(TEFAS_DATA_FILE)
                                if content is not None:
                                    file_size = len(content) / 1024  # KB
                                    st.info(f"📁 TEFAS Parquet (blob): {TEFAS_DATA_FILE} ({file_size:.1f} KB)")
                            except Exception:
                                pass
    
    with col2:
        if st.button("🔍 Test (Tek Gün)", type="secondary", help="Sadece bugün için test çek"):
            test_date = datetime.now()
            with st.spinner("🧪 Test verisi çekiliyor..."):
                success = update_tefas_data_to_parquet(test_date, test_date, selected_funds=None)
                
                if success:
                    st.success(f"✅ Test tamamlandı! {test_date.strftime('%d.%m.%Y')} için veriler kaydedildi.")
                else:
                    st.error("❌ Test başarısız!")
    
    # Periyodik Güncelleme Ayarları
    st.markdown("---")
    with st.expander("🔄 Periyodik Güncelleme Ayarları"):
        st.markdown("### ⏰ Otomatik TEFAS Güncelleme")
        st.info("🎯 Periyodik güncelleme her çalıştığında o günün TEFAS verilerini indirir ve Excel'e kaydeder")

        # Güncelleme periyodu seçimi (oku: blob)
        period_options = ['günlük', 'haftalık', 'aylık']
        try:
            current_settings = load_job_settings() or {}
            tefas_setting = current_settings.get('tefas', {})
        except Exception:
            tefas_setting = {}

        period_value = tefas_setting.get('period', 'günlük')
        try:
            period_index = period_options.index(period_value)
        except Exception:
            period_index = 0

        col1, col2, col3 = st.columns(3)

        with col1:
            update_period = st.selectbox(
                "📅 Güncelleme Periyodu:",
                options=period_options,
                index=period_index,
                key="tefas_period_select"
            )

        with col2:
            tstr = tefas_setting.get('time')
            update_time = st.time_input(
                "🕒 Güncelleme Saati:",
                value=safe_parse_time(tstr, datetime_time(9, 0)),
                key="tefas_time_select"
            )

        with col3:
            st.write("")
            if st.button("🚀 Periyodik Güncellemeyi Başlat", type="primary", key="tefas_setup_periodic"):
                success = setup_tefas_periodic_update(update_period, update_time)
                if success:
                    st.session_state['tefas_scheduler_active'] = True
                    st.session_state['tefas_update_period'] = update_period
                    st.session_state['tefas_update_time'] = update_time
                    st.success(f"✅ TEFAS periyodik güncelleme ayarlandı! ({update_period} - {update_time})")
                    try:
                        settings = load_job_settings() or {}
                        settings['tefas'] = {
                            'active': True,
                            'period': update_period,
                            'time': update_time.strftime('%H:%M:%S') if hasattr(update_time, 'strftime') else str(update_time)
                        }
                        save_job_settings(settings)
                    except Exception:
                        pass
                    # Do not force rerun to avoid refresh loops
                    # Ensure scheduler thread is running and will pick up the new job
                    try:
                        init_tefas_scheduler()
                    except Exception:
                        pass
                else:
                    st.error("❌ Periyodik güncelleme ayarlanırken hata oluştu")

        # Stop button in the same expander
        if st.button("⏹️ Periyodik Güncellemeyi Durdur", key="tefas_stop_periodic"):
            schedule.clear('tefas')
            st.session_state['tefas_scheduler_active'] = False
            st.success("⏹️ TEFAS periyodik güncelleme durduruldu")
            try:
                settings = load_job_settings() or {}
                settings['tefas'] = {'active': False}
                save_job_settings(settings)
            except Exception:
                pass

        # Aktif schedule bilgisi
        if st.session_state.get('tefas_scheduler_active', False):
            period = st.session_state.get('tefas_update_period', 'Bilinmiyor')
            time_str = str(st.session_state.get('tefas_update_time', 'Bilinmiyor'))
            st.info(f"🟢 **Aktif Schedule:** {period} güncelleme, saat {time_str}")
            st.info(f"📅 **Veri Kapsamı:** Her çalıştığında o günün tüm TEFAS fon verileri")
        else:
            st.warning("🔴 Periyodik güncelleme aktif değil")

        # Son job update'lerini blob'dan oku ve tablo olarak göster (BIST ile uyumlu)
        try:
            tefas_log_file = 'tefas_update_log.json'
            tefas_logs = read_logs_from_blob(tefas_log_file) or []
            if tefas_logs:
                st.markdown('#### 📋 Son Güncelleme Logları')
                tlog_df = pd.DataFrame(tefas_logs[-10:])
                tlog_df['timestamp'] = pd.to_datetime(tlog_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
                tlog_df = tlog_df.rename(columns={
                    'timestamp': 'Tarih/Saat',
                    'records_count': 'Kayıt Sayısı',
                    'backup_created': 'Backup',
                    'success': 'Başarılı'
                })
                cols = [c for c in ['Tarih/Saat', 'Kayıt Sayısı', 'Backup', 'Başarılı'] if c in tlog_df.columns]
                if cols:
                    st.dataframe(tlog_df[cols], use_container_width=True)
        except Exception:
            pass

def show_tefas_parquet_viewer():
    """Azure Blob Storage'daki Parquet verilerini görüntüleme sekmesi - HIZLI"""
    st.subheader("Azure Blob Storage'da Saklanan TEFAS Verileri")
    
    # Refresh butonu ekle
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("🔄 Verileri Yenile", type="secondary"):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        if st.button("📊 Azure Durumu", type="secondary"):
            # Azure Blob Storage'dan dosya durumunu kontrol et
            blob_storage = AzureBlobStorage()
            content = blob_storage.download_file(TEFAS_DATA_FILE)
            if content:
                file_size = len(content) / 1024
                st.success(f"✅ Azure'da TEFAS dosyası mevcut ({file_size:.1f} KB)")
                
                # Quick data check - ULTRA HIZLI
                try:
                    parquet_buffer = io.BytesIO(content)
                    df_check = pd.read_parquet(parquet_buffer)
                    st.info(f"📊 Günlük veriler: {len(df_check)} satır")
                    if not df_check.empty and 'Guncelleme_Zamani' in df_check.columns:
                        st.caption(f"⚡ Son güncelleme: {df_check['Guncelleme_Zamani'].max()}")
                except Exception as e:
                    st.error(f"❌ Azure veri okuma hatası: {str(e)}")
            else:
                st.error("❌ Azure'da TEFAS Parquet dosyası bulunamadı")

    # Son güncelleme logları (blob'dan okunur) - tablo görünümü
    tefas_log_file = 'tefas_update_log.json'
    try:
        tefas_logs = read_logs_from_blob(tefas_log_file) or []
        if tefas_logs:
            st.markdown("#### 📋 Son Güncelleme Logları")
            tlog_df = pd.DataFrame(tefas_logs[-10:])
            tlog_df['timestamp'] = pd.to_datetime(tlog_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
            tlog_df = tlog_df.rename(columns={
                'timestamp': 'Tarih/Saat',
                'records_count': 'Kayıt Sayısı',
                'backup_created': 'Backup',
                'success': 'Başarılı'
            })
            # Some TEFAS logs may use different field names; select available columns
            cols = [c for c in ['Tarih/Saat', 'Kayıt Sayısı', 'Backup', 'Başarılı'] if c in tlog_df.columns]
            if cols:
                st.dataframe(tlog_df[cols], use_container_width=True)
    except Exception:
        pass
    
    with col3:
        if st.button("🧪 Demo Veri Ekle", type="secondary"):
            try:
                tefas_dm.ensure_data_structure()
                
                # Demo veriler - Memory cache'e ekle
                demo_data = [
                    {'date': datetime.now() - timedelta(days=1), 'code': 'HPD', 'name': 'Halk Portföy Değişken Fon', 'price': 10.5678, 'total': 1250000.0, 'units': 119023.0},
                    {'date': datetime.now() - timedelta(days=1), 'code': 'GPD', 'name': 'Gedik Portföy Değişken Fon', 'price': 8.9123, 'total': 890000.0, 'units': 98765.0},
                    {'date': datetime.now() - timedelta(days=1), 'code': 'AAL', 'name': 'Ak Altın Fonu', 'price': 15.7832, 'total': 2100000.0, 'units': 132857.0},
                    {'date': datetime.now() - timedelta(days=2), 'code': 'HPD', 'name': 'Halk Portföy Değişken Fon', 'price': 10.4745, 'total': 1241000.0, 'units': 118456.0},
                    {'date': datetime.now() - timedelta(days=2), 'code': 'GPD', 'name': 'Gedik Portföy Değişken Fon', 'price': 8.9526, 'total': 887000.0, 'units': 99123.0},
                ]
                
                for demo in demo_data:
                    tefas_dm.upsert_fund_data(
                        demo['date'], demo['code'], demo['name'], 
                        demo['price'], demo['total'], demo['units']
                    )
                
                # Azure'a kaydet
                if tefas_dm.bulk_save_to_parquet():
                    st.success("✅ Demo veriler Azure'a eklendi!")
                else:
                    st.error("❌ Demo veri ekleme başarısız!")
                    
            except Exception as e:
                st.error(f"❌ Demo veri ekleme hatası: {str(e)}")
    
    # Azure'dan verileri görüntüle - HIZLI
    try:
        blob_storage = AzureBlobStorage()
        content = blob_storage.download_file(TEFAS_DATA_FILE)
        if content:
            # Azure'dan Parquet'i hızlı okuma
            parquet_buffer = io.BytesIO(content)
            df = pd.read_parquet(parquet_buffer)
            
            if not df.empty:
                st.markdown("### 📊 Veri Özeti")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Toplam Satır", len(df))
                with col2:
                    unique_funds = df['Fon_Kodu'].nunique()
                    st.metric("Benzersiz Fonlar", unique_funds)
                with col3:
                    date_range = (df['Tarih'].max() - df['Tarih'].min()).days
                    st.metric("Veri Aralığı (Gün)", date_range)
                with col4:
                    avg_price = df['Fiyat'].mean()
                    st.metric("Ortalama Fiyat", f"{avg_price:.2f}")
                
                # Filtreleme seçenekleri
                st.markdown("### 🔍 Veri Filtreleme")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Fon kodu filtresi
                    fund_codes = ['Tümü'] + sorted(df['Fon_Kodu'].unique().tolist())
                    selected_fund = st.selectbox("Fon Kodu:", fund_codes)
                
                with col2:
                    # Tarih aralığı filtresi
                    max_date = df['Tarih'].max().date()
                    min_date = df['Tarih'].min().date()
                    date_range = st.date_input(
                        "Tarih Aralığı:",
                        value=(min_date, max_date),
                        min_value=min_date,
                        max_value=max_date,
                        format="DD/MM/YYYY"
                    )
                
                # Filtreleme uygula
                filtered_df = df.copy()
                
                if selected_fund != 'Tümü':
                    filtered_df = filtered_df[filtered_df['Fon_Kodu'] == selected_fund]
                
                if len(date_range) == 2:
                    start_date, end_date = date_range
                    filtered_df = filtered_df[
                        (filtered_df['Tarih'].dt.date >= start_date) &
                        (filtered_df['Tarih'].dt.date <= end_date)
                    ]
                
                # Sonuçları göster
                st.markdown(f"### 📋 Filtrelenmiş Veriler ({len(filtered_df)} satır)")
                
                if not filtered_df.empty:
                    # Sıralama
                    filtered_df = filtered_df.sort_values(['Tarih', 'Fon_Kodu'], ascending=[False, True])
                    
                    # Son 100 satırı göster (performans için)
                    display_df = filtered_df.head(100)
                    
                    # Görüntüleme için sütun adlarını düzenle
                    display_df = display_df.rename(columns={
                        'Tarih': 'Tarih',
                        'Fon_Kodu': 'Fon Kodu',
                        'Fon_Adi': 'Fon Adı',
                        'Fiyat': 'Fiyat',
                        'Gunluk_Getiri': 'Günlük Getiri (%)',
                        'Toplam_Deger': 'Toplam Değer',
                        'Pay_Sayisi': 'Pay Sayısı',
                        'Kategori': 'Kategori'
                    })
                    
                    st.dataframe(
                        display_df[['Tarih', 'Fon Kodu', 'Fon Adı', 'Fiyat', 'Günlük Getiri (%)', 'Toplam Değer']],
                        use_container_width=True
                    )
                    
                    if len(filtered_df) > 100:
                        st.info(f"📋 İlk 100 satır gösteriliyor. Toplam {len(filtered_df)} satır mevcut.")
                    
                    # Download butonu
                    if st.button("💾 Filtrelenmiş Veriyi CSV İndir"):
                        csv = filtered_df.to_csv(index=False)
                        st.download_button(
                            label="📁 CSV Dosyasını İndir",
                            data=csv,
                            file_name=f"tefas_filtered_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv"
                        )
                else:
                    st.info("📝 Seçilen filtrelere uygun veri bulunamadı.")
            else:
                st.info("📝 Henüz Parquet dosyasında veri yok. 'Veri İndirme' sekmesinden veri ekleyebilirsiniz.")
        else:
            st.info("📝 Azure'da henüz TEFAS Parquet dosyası yok. 'Veri İndirme' sekmesinden başlayabilirsiniz.")
            
    except Exception as e:
        st.error(f"❌ Azure veri görüntüleme hatası: {str(e)}")
        st.error("Lütfen önce 'Veri İndirme' sekmesinden veri indirmeyi deneyin.")

def show_tefas_fund_search():
    """Fon arama sekmesi - Azure Blob Storage tabanlı"""
    st.subheader("🔍 TEFAS Fon Arama")
    
    # Azure'dan TEFAS dosyasını kontrol et
    blob_storage = AzureBlobStorage()
    content = blob_storage.download_file(TEFAS_DATA_FILE)
    
    if not content:
        st.warning("⚠️ Azure'da TEFAS Parquet dosyası bulunamadı. Önce 'Veri İndirme' sekmesinden veri çekin.")
        return
    
    try:
        # Azure'dan Parquet'i oku
        parquet_buffer = io.BytesIO(content)
        df = pd.read_parquet(parquet_buffer)
        
        # Fon arama
        search_term = st.text_input(
            "🔍 Fon Ara:",
            placeholder="Fon kodu veya adı girin...",
            help="Azure'da kayıtlı fonlar arasında arama yapın"
        )
        
        # Filtreleme uygula
        if search_term:
            search_results = df[
                df['Fon_Kodu'].str.contains(search_term, case=False, na=False) |
                df['Fon_Adi'].str.contains(search_term, case=False, na=False)
            ]
        else:
            search_results = df
        
        if not search_results.empty:
            # Arama sonucu bilgisi
            if search_term:
                st.caption(f"🔍 {len(search_results)} sonuç bulundu")
            
            # En son verilerini göster
            latest_results = search_results.sort_values('Tarih', ascending=False).groupby('Fon_Kodu').first().reset_index()
            
            # Görüntüle
            display_df = latest_results[['Fon_Kodu', 'Fon_Adi', 'Fiyat', 'Tarih', 'Kategori']].copy()
            display_df.columns = ['Fon Kodu', 'Fon Adı', 'Son Fiyat', 'Son Tarih', 'Kategori']
            
            st.info(f"📊 Toplam {len(display_df)} fon görüntüleniyor")
            st.dataframe(display_df, use_container_width=True)
        else:
            if search_term:
                st.warning("⚠️ Arama kriterinize uygun fon bulunamadı.")
            else:
                st.warning("⚠️ Azure'da fon verisi bulunamadı.")
    
    except Exception as e:
        st.error(f"❌ Azure fon arama yapılırken hata: {str(e)}")

def show_tefas_statistics():
    """TEFAS istatistikleri sekmesi - Azure Blob Storage tabanlı"""
    st.subheader("📈 TEFAS İstatistikleri")
    
    # Azure'dan TEFAS dosyasını kontrol et
    blob_storage = AzureBlobStorage()
    content = blob_storage.download_file(TEFAS_DATA_FILE)
    
    if not content:
        st.warning("⚠️ Azure'da TEFAS Parquet dosyası bulunamadı. Önce 'Veri İndirme' sekmesinden veri çekin.")
        return
    
    try:
        # Azure'dan Parquet'i hızlı okuma
        parquet_buffer = io.BytesIO(content)
        df = pd.read_parquet(parquet_buffer)
        
        if df.empty:
            st.warning("⚠️ Azure'daki Parquet dosyasında veri bulunamadı.")
            return
        
        # Genel istatistikler
        st.subheader("📊 Genel İstatistikler")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_records = len(df)
            st.metric("📊 Toplam Kayıt", total_records)
        
        with col2:
            unique_funds = df['Fon_Kodu'].nunique()
            st.metric("🎯 Benzersiz Fonlar", unique_funds)
        
        with col3:
            date_range = (df['Tarih'].max() - df['Tarih'].min()).days
            st.metric("📅 Veri Aralığı (Gün)", date_range)
        
        with col4:
            avg_return = df['Gunluk_Getiri'].mean()
            st.metric("📈 Ort. Günlük Getiri (%)", f"{avg_return:.2f}")
        
        # En iyi/en kötü performans gösterenler
        st.subheader("🏆 Performans Liderleri")
        
        # En yüksek getiri
        top_returns = df.nlargest(10, 'Gunluk_Getiri')[['Tarih', 'Fon_Kodu', 'Fon_Adi', 'Gunluk_Getiri']]
        st.write("**📈 En Yüksek Günlük Getiriler:**")
        st.dataframe(top_returns, use_container_width=True)
        
        # En düşük getiri
        st.write("**📉 En Düşük Günlük Getiriler:**")
        bottom_returns = df.nsmallest(10, 'Gunluk_Getiri')[['Tarih', 'Fon_Kodu', 'Fon_Adi', 'Gunluk_Getiri']]
        st.dataframe(bottom_returns, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ TEFAS istatistikleri yüklenirken hata: {str(e)}")
        debug_logger.error('TEFAS_DATA_PROCESSING', f'İstatistik hatası: {str(e)}', {
            'error_type': type(e).__name__
        })

# Demo kullanıcı oluştur
def create_demo_user():
    """Demo kullanıcı hesabını oluştur"""
    users = load_users()
    demo_email = "erdalural@gmail.com"
    
    if demo_email not in users:
        users[demo_email] = {
            'password': hash_password("Erdal34?"),
            'name': "Erdal Ural",
            'created_at': datetime.now().isoformat()
        }
        save_users(users)

def show_turkish_gold_data_management():
    """Turkish Gold veri yönetimi sekmesini göster"""
    st.subheader("🥇 Turkish Gold Fiyat Veri Yönetimi")
    
    # Alt sekmeler - Mevcut Durum sekmesi kaldırıldı
    tab1, tab2 = st.tabs(["🔧 Veri İşlemleri", "📈 Tarihsel Rapor"])
    
    with tab1:
        st.markdown("### 🔧 Veri Yönetimi İşlemleri")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 API'den Fiyatları Güncelle", type="primary"):
                with st.spinner("🔄 Turkish gold fiyatları API'den çekiliyor..."):
                    success = turkish_gold_dm.update_prices_from_api()
                    
                    if success:
                        st.success("✅ Turkish gold fiyatları başarıyla güncellendi!")
                        st.rerun()
                    else:
                        st.error("❌ Fiyat güncelleme başarısız!")
            
            if st.button("📥 Parquet Storage'dan Yükle", type="secondary"):
                with st.spinner("📥 Parquet storage'dan veriler yükleniyor..."):
                    turkish_gold_dm._load_cache()
                    st.success("✅ Veriler parquet storage'dan yüklendi!")
                    st.rerun()
        
        with col2:
            if st.button("🧪 API Test Et", type="secondary"):
                with st.spinner("🧪 Truncgil API'si test ediliyor..."):
                    api_data = get_turkish_gold_prices()
                    
                    if api_data:
                        st.success(f"✅ API çalışıyor! {len(api_data)} instrument bulundu.")
                        
                        # Test verilerini göster
                        with st.expander("📊 API Test Sonuçları"):
                            for instrument, data in list(api_data.items())[:5]:  # İlk 5 tanesi
                                st.write(f"**{instrument}**: {data.get('price', 0):.2f} ₺")
                    else:
                        st.error("❌ API test başarısız!")
            
            if st.button("📊 Parquet Dosya Bilgisi", type="secondary"):
                summary = turkish_gold_dm.get_data_summary()
                if summary:
                    st.json(summary)
                else:
                    st.warning("Parquet dosyası bulunamadı veya boş")
        
        # Otomatik güncelleme ayarları
        st.markdown("### ⚙️ Otomatik Güncelleme")
        st.info("ℹ️ Bu sistem günde bir kere otomatik olarak Turkish gold fiyatlarını API'den çekip parquet formatında kaydeder.")
        
        blob_prices = turkish_gold_dm.get_prices()
        is_fresh = turkish_gold_dm.is_data_fresh(max_age_hours=24)
        
        if st.checkbox("🔄 Sayfa yüklendiğinde otomatik kontrol et"):
            if not is_fresh:
                with st.spinner("🔄 Veriler eski, otomatik güncelleniyor..."):
                    success = turkish_gold_dm.update_prices_from_api()
                    if success:
                        st.success("✅ Otomatik güncelleme tamamlandı!")
                        st.rerun()
        
        # Periyodik Güncelleme Ayarları
        st.markdown("---")
        with st.expander("🔄 Periyodik Güncelleme Ayarları"):
            st.markdown("### ⏰ Otomatik Turkish Gold Güncelleme")
            st.info("🎯 Periyodik güncelleme Turkish Gold fiyatlarını API'den çeker ve Parquet'e kaydeder")
            
            # Güncelleme periyodu seçimi (oku: blob)
            period_options = ['günlük', 'haftalık', 'aylık']
            try:
                current_settings = load_job_settings() or {}
                tg_setting = current_settings.get('turkish_gold', {})
            except Exception:
                tg_setting = {}

            period_value = tg_setting.get('period', 'günlük')
            try:
                period_index = period_options.index(period_value)
            except Exception:
                period_index = 0

            col1, col2, col3 = st.columns(3)

            with col1:
                update_period = st.selectbox(
                    "📅 Güncelleme Periyodu:",
                    options=period_options,
                    index=period_index,
                    key="turkish_gold_period_select"
                )

            with col2:
                tstr = tg_setting.get('time')
                update_time = st.time_input(
                    "🕒 Güncelleme Saati:",
                    value=safe_parse_time(tstr, datetime_time(9, 0)),
                    key="turkish_gold_time_select"
                )

            with col3:
                st.write("")
                if st.button("🚀 Periyodik Güncellemeyi Ayarla", type="secondary", key="turkish_gold_setup_periodic"):
                    success = setup_turkish_gold_periodic_update(update_period, update_time)
                    if success:
                        st.session_state['turkish_gold_scheduler_active'] = True
                        st.session_state['turkish_gold_update_period'] = update_period
                        st.session_state['turkish_gold_update_time'] = update_time
                        st.success(f"✅ Turkish Gold periyodik güncelleme ayarlandı! ({update_period} - {update_time})")
                        try:
                            settings = load_job_settings() or {}
                            settings['turkish_gold'] = {
                                'active': True,
                                'period': update_period,
                                'time': update_time.strftime('%H:%M:%S') if hasattr(update_time, 'strftime') else str(update_time)
                            }
                            save_job_settings(settings)
                        except Exception:
                            pass
                        # Do not force an immediate rerun here — avoids refresh loops
                        try:
                            init_turkish_gold_scheduler()
                        except Exception:
                            pass
                    else:
                        st.error("❌ Periyodik güncelleme ayarlanırken hata oluştu")

            # Stop button
            if st.button("⏹️ Periyodik Güncellemeyi Durdur", key="turkish_gold_stop_periodic"):
                # Turkish Gold schedule'larını temizle
                schedule.clear('turkish_gold')
                st.session_state['turkish_gold_scheduler_active'] = False
                try:
                    settings = load_job_settings() or {}
                    settings['turkish_gold'] = {'active': False}
                    save_job_settings(settings)
                except Exception:
                    pass

        # Aktif schedule bilgisi
        if st.session_state.get('turkish_gold_scheduler_active', False):
            period = st.session_state.get('turkish_gold_update_period', 'Bilinmiyor')
            time_str = str(st.session_state.get('turkish_gold_update_time', 'Bilinmiyor'))
            st.info(f"🟢 **Aktif Schedule:** {period} güncelleme, saat {time_str}")
            st.info(f"🥇 **Veri Kapsamı:** Turkish Gold fiyat verileri (API'den)")
        else:
            st.warning("🔴 Periyodik güncelleme aktif değil")

        # Son job update'lerini blob'dan oku ve tablo olarak göster (BIST ile uyumlu)
        try:
            tg_log_file = 'turkish_gold_update_log.json'
            tg_logs = read_logs_from_blob(tg_log_file) or []
            if tg_logs:
                st.markdown('#### 📋 Son Güncelleme Logları')
                tg_df = pd.DataFrame(tg_logs[-10:])
                tg_df['timestamp'] = pd.to_datetime(tg_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
                tg_df = tg_df.rename(columns={
                    'timestamp': 'Tarih/Saat',
                    'items_count': 'Enstrüman Sayısı',
                    'backup_created': 'Backup',
                    'success': 'Başarılı'
                })
                cols = [c for c in ['Tarih/Saat', 'Enstrüman Sayısı', 'Backup', 'Başarılı'] if c in tg_df.columns]
                if cols:
                    st.dataframe(tg_df[cols], use_container_width=True)
        except Exception:
            pass

    with tab2:
        st.markdown("### 📈 Tarihsel Turkish Gold Raporu")
        
        # Tarih aralığı seçimi
        col1, col2, col3 = st.columns(3)
        
        with col1:
            start_date = st.date_input(
                "Başlangıç Tarihi:",
                value=datetime.now().date() - timedelta(days=30),
                max_value=datetime.now().date(),
                format="DD/MM/YYYY"
            )
        
        with col2:
            end_date = st.date_input(
                "Bitiş Tarihi:",
                value=datetime.now().date(),
                max_value=datetime.now().date(),
                format="DD/MM/YYYY"
            )
        
        with col3:
            if st.button("📊 Rapor Oluştur", type="primary"):
                with st.spinner("📊 Tarihsel veriler analiz ediliyor..."):
                    historical_df = turkish_gold_dm.get_historical_data(start_date, end_date)
                    
                    if not historical_df.empty:
                        st.session_state['turkish_gold_report'] = historical_df
                        st.success(f"✅ {len(historical_df)} kayıt bulundu!")
                    else:
                        st.warning("⚠️ Belirtilen tarih aralığında veri bulunamadı!")
        
        # Rapor gösterimi
        if 'turkish_gold_report' in st.session_state:
            df = st.session_state['turkish_gold_report']
            
            if not df.empty:
                # Özet istatistikler
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Toplam Kayıt", len(df))
                
                with col2:
                    unique_dates = df['Tarih'].nunique()
                    st.metric("Gün Sayısı", unique_dates)
                
                with col3:
                    unique_instruments = df['Instrument_Code'].nunique()
                    st.metric("Enstrüman Sayısı", unique_instruments)
                
                with col4:
                    latest_date = df['Tarih'].max()
                    st.metric("Son Tarih", latest_date.strftime('%d.%m.%Y'))
                
                # Enstrüman seçimi
                st.markdown("#### 📋 Detaylı Veriler")
                
                instruments = df['Instrument_Code'].unique().tolist()
                selected_instruments = st.multiselect(
                    "Gösterilecek enstrümanları seçin:",
                    options=instruments,
                    default=instruments[:5] if len(instruments) > 5 else instruments
                )
                
                if selected_instruments:
                    # Filtreleme
                    filtered_df = df[df['Instrument_Code'].isin(selected_instruments)]
                    
                    # Pivot tablo oluştur (tarih x enstrüman)
                    pivot_df = filtered_df.pivot_table(
                        index='Tarih',
                        columns='Instrument_Code',
                        values='Price',
                        aggfunc='last'
                    ).round(2)
                    
                    st.dataframe(pivot_df, use_container_width=True)
                    
                    # Grafik gösterimi
                    if len(selected_instruments) <= 10:  # Çok fazla line olmaması için
                        st.markdown("#### 📈 Fiyat Trendi")
                        
                        fig = go.Figure()
                        
                        for instrument in selected_instruments:
                            instrument_data = filtered_df[filtered_df['Instrument_Code'] == instrument]
                            fig.add_trace(go.Scatter(
                                x=instrument_data['Tarih'],
                                y=instrument_data['Price'],
                                mode='lines+markers',
                                name=instrument,
                                line=dict(width=2),
                                marker=dict(size=4)
                            ))
                        
                        fig.update_layout(
                            title="Turkish Gold Fiyat Trendi",
                            xaxis_title="Tarih",
                            yaxis_title="Fiyat (₺)",
                            hovermode='x unified',
                            height=500,
                            paper_bgcolor='rgba(9, 13, 24, 0.0)',
                            plot_bgcolor='rgba(10, 18, 32, 0.92)',
                            font=dict(color='#e2e8f0'),
                            xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                            yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                            legend=dict(font=dict(color='#e2e8f0'))
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Ham veri görüntüleme
                    if st.checkbox("📋 Ham verileri göster"):
                        st.dataframe(filtered_df, use_container_width=True)
                    
                    # CSV export
                    csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label="📥 CSV olarak indir",
                        data=csv,
                        file_name=f"turkish_gold_report_{start_date}_{end_date}.csv",
                        mime="text/csv"
                    )

def show_market_analysis():
    """Piyasa analizi sekmesini göster"""
    
    # Modern sidebar CSS stilleri - Piyasa analizi için
    st.sidebar.markdown("""
    <style>
    /* Modern section başlıkları - karanlık temaya uyum */
    .section-header {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.85) 0%, rgba(15, 23, 42, 0.92) 100%);
        color: var(--text-primary);
        padding: 12px 16px;
        border-radius: 12px;
        font-size: 13px;
        font-weight: 600;
        margin: 20px 0 14px 0;
        border-left: 4px solid rgba(37, 99, 235, 0.7);
        box-shadow: 0 14px 24px rgba(8, 13, 24, 0.45);
    }

    /* Modern multiselect ve selectbox stilleri */
    div[data-testid="stMultiSelect"],
    div[data-testid="stSelectbox"] {
        background: linear-gradient(135deg, rgba(16, 24, 40, 0.96) 0%, rgba(12, 19, 33, 0.88) 100%);
        border-radius: 18px;
        padding: 16px 18px 20px 18px;
        border: 1px solid rgba(59, 130, 246, 0.28);
        box-shadow: 0 22px 44px rgba(6, 11, 22, 0.55);
        margin-bottom: 18px;
    }

    div[data-testid="stMultiSelect"] > label,
    div[data-testid="stSelectbox"] > label {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.58) 0%, rgba(29, 78, 216, 0.48) 100%);
        border-radius: 12px;
        padding: 9px 14px;
        font-weight: 600;
        font-size: 13px;
        color: #f8fafc;
        margin-bottom: 14px;
        box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.18);
        letter-spacing: 0.01em;
    }

    div[data-testid="stMultiSelect"] > label p,
    div[data-testid="stSelectbox"] > label p {
        color: #f8fafc !important;
        margin: 0 !important;
    }

    /* Kategori seçimi değer metnini görünür tut */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child {
        display: flex !important;
        align-items: center !important;
        min-height: 48px !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child p {
        color: #f8fafc !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        letter-spacing: 0.01em !important;
    }

    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input {
        color: #f8fafc !important;
    }
    
    /* Multiselect input alanı - placeholder ve metin okunabilirliği */
    div[data-testid="stMultiSelect"] input {
        color: #ffffff !important;
        font-size: 14px !important;
        font-weight: 500 !important;
    }
    
    div[data-testid="stMultiSelect"] input::placeholder {
        color: #d1d5db !important;
        opacity: 0.9 !important;
    }
    
    /* Seçili öğeler (tags) */
    div[data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background: rgba(37, 99, 235, 0.8) !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        padding: 4px 8px !important;
        border-radius: 6px !important;
    }
    
    div[data-testid="stMultiSelect"] ul {
        max-height: 280px;
        background-color: rgba(13, 20, 34, 0.96);
        border-radius: 10px;
        color: var(--text-primary);
    }
    /* Force closed multiselect/select control to be dark and show muted placeholder */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:nth-child(1),
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:nth-child(1) {
        background: rgba(15, 23, 42, 0.92) !important;
        color: var(--text-primary) !important;
        border: 1px solid rgba(59, 130, 246, 0.18) !important;
        border-radius: 10px !important;
        padding: 10px 12px !important;
        box-shadow: none !important;
    }

    /* Placeholder text inside the closed select control */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input::placeholder,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] > div:first-child input::placeholder {
        color: #cbd5e1 !important; /* muted light */
        opacity: 0.95 !important;
    }

    /* Ensure selected text uses readable font like Enstrüman selection */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    div[data-testid="stSelectbox"] > div,
    div[data-testid="stSelectbox"] div {
        font-size: 14px !important;
        font-weight: 500 !important;
        line-height: 1.4 !important;
        color: var(--text-primary) !important;
    }

    /* Stronger, sidebar-specific selector to ensure Kategori Select matches Enstrüman Seçimi */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child,
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:nth-child(1) {
        background: linear-gradient(135deg, rgba(30,41,59,0.85) 0%, rgba(15,23,42,0.92) 100%) !important;
        border: 1px solid rgba(59, 130, 246, 0.18) !important;
        box-shadow: inset 0 0 0 1px rgba(15, 23, 42, 0.45) !important;
        border-radius: 12px !important;
        padding: 12px 14px !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
    }

    /* Sidebar placeholder/selected text clarity */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child span,
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child input::placeholder {
        color: #e2e8f0 !important;
        opacity: 0.95 !important;
    }

    /* Sidebar chevron color */
    [data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] svg {
        fill: #e2e8f0 !important;
        color: #e2e8f0 !important;
        opacity: 0.95 !important;
    }

    /* Ensure the dropdown chevron is visible and muted */
    div[data-testid="stSelectbox"] div[data-baseweb="select"] svg,
    div[data-testid="stMultiSelect"] div[data-baseweb="select"] svg {
        fill: #cbd5e1 !important;
        color: #cbd5e1 !important;
        opacity: 0.95 !important;
    }
    div[data-testid="stSelectbox"] div {
        color: var(--text-primary);
        font-weight: 500;
        font-size: 14px;
    }

    /* Modern input stilleri */
    div[data-testid="stTextInput"] > div > div > input,
    div[data-testid="stNumberInput"] input {
        background: rgba(13, 20, 34, 0.92);
        border: 1px solid rgba(59, 130, 246, 0.3);
        border-radius: 12px;
        padding: 12px;
        font-size: 13px;
        transition: all 0.3s ease;
        color: var(--text-primary);
    }
    div[data-testid="stTextInput"] > div > div > input:focus,
    div[data-testid="stNumberInput"] input:focus {
        border-color: rgba(37, 99, 235, 0.6);
        box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.45);
    }

    /* Modern date input stilleri */
    div[data-testid="stDateInput"] > div > div > input {
        background: rgba(13, 20, 34, 0.92);
        border: 1px solid rgba(37, 99, 235, 0.35);
        border-radius: 12px;
        padding: 10px 12px;
        color: var(--text-primary);
        font-size: 13px;
    }

    /* Modern buton stilleri */
    div[data-testid="stButton"] > button {
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.95) 0%, rgba(29, 78, 216, 0.95) 100%);
        color: #f8fafc;
        border: 1px solid rgba(59, 130, 246, 0.55);
        border-radius: 18px;
        padding: 12px 22px;
        font-weight: 600;
        font-size: 14px;
        transition: all 0.3s ease;
        box-shadow: 0 16px 30px rgba(15, 23, 42, 0.5);
    }
    div[data-testid="stButton"] > button:hover {
        transform: translateY(-2px) scale(1.01);
        box-shadow: 0 22px 36px rgba(37, 99, 235, 0.32);
        border-color: rgba(148, 163, 184, 0.3);
    }

    /* Primary buton özel stili */
    div[data-testid="stButton"] > button[kind="primary"] {
        background: linear-gradient(135deg, #22d3ee 0%, #0ea5e9 100%);
        color: #041120;
        box-shadow: 0 20px 30px rgba(14, 165, 233, 0.4);
        font-weight: 700;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #06b6d4 0%, #0ea5e9 100%);
        box-shadow: 0 26px 36px rgba(14, 165, 233, 0.45);
        transform: translateY(-3px) scale(1.02);
    }
    </style>
    """, unsafe_allow_html=True)

    # Dinamik kategorileri al
    CURRENT_INSTRUMENT_CATEGORIES = get_instrument_categories()
    
    # Kategori listelerini sıralı şekilde oluştur - CASH kategorisini hariç tut
    category_keys_list = [key for key in CURRENT_INSTRUMENT_CATEGORIES.keys() if key != "CASH"]
    category_display_list = [f"{key} - {CURRENT_INSTRUMENT_CATEGORIES[key]['name']}" 
                             for key in category_keys_list]
    
    selected_category_display = st.sidebar.selectbox(
        "🎯 Finansal Enstrüman Kategorisi Seçin:",
        options=category_display_list,
        help="Analiz etmek istediğiniz finansal enstrüman kategorisini seçin"
    )
    
    # Seçilen kategorinin index'ini bul ve ona karşılık gelen key'i al
    selected_index = category_display_list.index(selected_category_display)
    selected_category = category_keys_list[selected_index]

    # Kategori değiştiğinde önceki verileri temizle
    if 'last_selected_category' not in st.session_state:
        st.session_state['last_selected_category'] = selected_category
    
    if st.session_state['last_selected_category'] != selected_category:
        # Kategori değişti, önceki verileri temizle
        if 'detailed_data' in st.session_state:
            del st.session_state['detailed_data']
        if 'detailed_date_range' in st.session_state:
            del st.session_state['detailed_date_range']
        if 'detailed_category' in st.session_state:
            del st.session_state['detailed_category']
        if 'market_data' in st.session_state:
            del st.session_state['market_data']
        if 'market_date_range' in st.session_state:
            del st.session_state['market_date_range']
        if 'market_category' in st.session_state:
            del st.session_state['market_category']
        
        # Yeni kategoriyi kaydet
        st.session_state['last_selected_category'] = selected_category

    # Seçilen kategoriye göre enstrümanları al
    if selected_category == "TEFAS":
        current_instruments = get_tefas_funds_dynamic()
        CURRENT_INSTRUMENT_CATEGORIES[selected_category]["data"] = current_instruments
    else:
        current_instruments = CURRENT_INSTRUMENT_CATEGORIES[selected_category]["data"]
    current_currency = CURRENT_INSTRUMENT_CATEGORIES[selected_category]["currency"]

    # Popüler seçenekleri tanımla
    if selected_category == "BIST":
        popular_instruments = ["AKBNK", "GARAN", "THYAO", "ASELS", "ISCTR"]
    elif selected_category == "NASDAQ":
        popular_instruments = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    elif selected_category == "METALS":
        # Tüm Türk altın enstrümanlarını popüler listesine ekle
        popular_instruments = ["GC=F", "SI=F"] + TURKISH_GOLD_INSTRUMENTS
    elif selected_category == "FOREX":
        popular_instruments = ["EURUSD=X", "GBPUSD=X", "USDTRY=X", "EURTRY=X"]
    elif selected_category == "CRYPTO":
        popular_instruments = ["BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD", "SOL-USD"]
    elif selected_category == "TEFAS":
        popular_instruments = ["HPD", "GPD", "ZPD", "IPD", "APD"]

    # Make the instruments list robust: fix typo and handle cases where current_instruments
    # may not be a dict (fallback to empty list)
    if isinstance(current_instruments, dict):
        all_instrument_options = list(current_instruments.keys())
    else:
        all_instrument_options = list(current_instruments) if current_instruments else []

    # Enstrümanları seç (doğrudan filtreleme olmadan)
    filtered_instruments = all_instrument_options

    # Display options oluştur (multiselect için)
    if isinstance(current_instruments, dict):
        instrument_codes = filtered_instruments  # Bu zaten list
        display_options = [f"{code} - {get_instrument_display_name(code, selected_category, current_instruments)}" 
                          for code in instrument_codes]
    else:
        instrument_codes = filtered_instruments
        display_options = filtered_instruments
    
    # Enstrümanları seç
    selected_displays = st.sidebar.multiselect(
        f"📊 {CURRENT_INSTRUMENT_CATEGORIES[selected_category]['name']} Seçin:",
        options=display_options,
        default=[],
        help=f"Analiz etmek istediğiniz {CURRENT_INSTRUMENT_CATEGORIES[selected_category]['name'].lower()} seçin"
    )
    
    # Seçilen display'lerin index'lerini bul ve ona karşılık gelen kodları al
    if selected_displays and isinstance(current_instruments, dict):
        selected_instruments = []
        for display in selected_displays:
            idx = display_options.index(display)
            selected_instruments.append(instrument_codes[idx])
    else:
        selected_instruments = selected_displays

    # Modern tarih aralığı başlığı
    st.sidebar.markdown('<div class="section-header">📅 Tarih Aralığı</div>', unsafe_allow_html=True)
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Başlangıç Tarihi:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now(),
            format="DD/MM/YYYY"
        )

    with col2:
        end_date = st.date_input(
            "Bitiş Tarihi:",
            value=datetime.now(),
            max_value=datetime.now(),
            format="DD/MM/YYYY"
        )

    # Ana veri çekme butonları
    
    # Analiz Başlat butonunu tam genişlikte yap
    if st.sidebar.button("🚀 Analiz Başlat", type="primary", use_container_width=True):
            try:
                if not selected_instruments:
                    st.error(f"📊 Lütfen en az bir {CURRENT_INSTRUMENT_CATEGORIES[selected_category]['name'].lower()} seçin!")
                elif start_date > end_date:
                    st.error("📅 Başlangıç tarihi bitiş tarihinden sonra olamaz!")
                else:
                    # Ana ekranda büyük bildirim göster
                    progress_placeholder = st.empty()
                    
                    # Seçilen enstrümanları listele
                    instruments_text = ", ".join(selected_instruments[:5])
                    if len(selected_instruments) > 5:
                        instruments_text += f" ve {len(selected_instruments) - 5} enstrüman daha"
                    
                    with progress_placeholder.container():
                        st.markdown(f"""
                        <div style="
                            background: linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(37, 99, 235, 0.08) 100%);
                            border-left: 4px solid #3b82f6;
                            padding: 20px 24px;
                            border-radius: 12px;
                            margin: 20px 0;
                            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.1);
                        ">
                            <div style="font-size: 18px; font-weight: 600; color: #3b82f6; margin-bottom: 8px;">
                                ⏳ Analiz Devam Ediyor...
                            </div>
                            <div style="font-size: 14px; color: rgba(248, 250, 252, 0.85); margin-bottom: 4px;">
                                � Kategori: <strong>{CURRENT_INSTRUMENT_CATEGORIES[selected_category]['name']}</strong>
                            </div>
                            <div style="font-size: 14px; color: rgba(248, 250, 252, 0.85); margin-bottom: 4px;">
                                🎯 Enstrümanlar: <strong>{instruments_text}</strong>
                            </div>
                            <div style="font-size: 14px; color: rgba(248, 250, 252, 0.85);">
                                📅 Tarih Aralığı: <strong>{start_date} - {end_date}</strong>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with st.spinner(f"🔄 {len(selected_instruments)} enstrüman için veri çekiliyor..."):
                        try:
                            if selected_category == "BIST":
                                detailed_data = get_specific_stock_data(selected_instruments, start_date, end_date)
                            else:
                                detailed_data = get_specific_instrument_data(selected_category, selected_instruments, start_date, end_date)
                            
                            # Progress mesajını temizle
                            progress_placeholder.empty()
                            
                            if detailed_data is not None and not detailed_data.empty:
                                st.session_state['detailed_data'] = detailed_data
                                st.session_state['detailed_date_range'] = f"{start_date} - {end_date}"
                                st.session_state['detailed_category'] = selected_category
                                st.success(f"✅ Analiz başarıyla tamamlandı!")
                            else:
                                st.error(f"❌ Seçilen {CURRENT_INSTRUMENT_CATEGORIES[selected_category]['name']} için detaylı veri alınamadı!")
                        except Exception as data_error:
                            progress_placeholder.empty()
                            st.error(f"❌ Detaylı veri çekme hatası: {str(data_error)}")
            except Exception as e:
                st.error(f"❌ Genel bir hata oluştu: {str(e)}")

    # CSS stileri - Şikayet & Öneri ve Hesap Ayarları butonlarını küçült ve aşağıya taşı
    st.sidebar.markdown("""
    <style>
    /* Sidebar buton stillerini özel hale getir - daha küçük font ve daha aşağıya */
    [data-testid="stSidebar"] button[key*="feedback_market"],
    [data-testid="stSidebar"] button[key*="settings_market"] {
        font-size: 10.5px !important;  /* 14px -> 10.5px (%75 küçültü) */
        padding: 8px 10px !important;  /* Daha kompakt padding */
        margin: 20px 0 !important;  /* Daha aşağıya */
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Sidebar'da Şikayet & Öneri ve Ayarlar butonları
    st.sidebar.markdown('<hr style="margin: 300px 0 40px 0; border-color: rgba(59, 130, 246, 0.2);">', unsafe_allow_html=True)
    
    # Session state for sidebar sections
    if 'show_feedback_market' not in st.session_state:
        st.session_state['show_feedback_market'] = False
    if 'show_settings_market' not in st.session_state:
        st.session_state['show_settings_market'] = False
    
    # Şikayet & Öneri Butonu
    if st.sidebar.button("📝 Şikayet & Öneri", key="btn_toggle_feedback_market", use_container_width=True):
        st.session_state['show_feedback_market'] = not st.session_state['show_feedback_market']
        st.session_state['show_settings_market'] = False
    
    if st.session_state['show_feedback_market']:
        with st.sidebar:
            st.markdown('<div style="background: rgba(30, 41, 59, 0.6); padding: 12px; border-radius: 8px; margin-top: 8px;">', unsafe_allow_html=True)
            
            feedback_type = st.radio(
                "Bildirim Türü:",
                options=["sikayet", "oneri", "bilgi_talebi"],
                format_func=lambda x: {"sikayet": "🔴 Şikayet", "oneri": "💡 Öneri", "bilgi_talebi": "❓ Bilgi Talebi"}[x],
                key="feedback_type_market",
                horizontal=True
            )
            
            feedback_subject = st.text_input("Konu:", max_chars=100, key="feedback_subject_market")
            feedback_message = st.text_area("Açıklama:", height=80, key="feedback_message_market")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Gönder", key="btn_send_feedback_market", type="primary", use_container_width=True):
                    if not feedback_subject or not feedback_message:
                        st.error("⚠️ Tüm alanları doldurun!")
                    else:
                        success, message = send_feedback_email(
                            feedback_type=feedback_type,
                            subject=feedback_subject,
                            message=feedback_message,
                            user_email=st.session_state.get('user_email', ''),
                            user_name=st.session_state.get('user_name', '')
                        )
                        if success:
                            st.success("✅ Gönderildi!")
                        else:
                            st.error(message)
            with col2:
                if st.button("❌ Kapat", key="btn_close_feedback_market", use_container_width=True):
                    st.session_state['show_feedback_market'] = False
                    st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Ayarlar Butonu
    if st.sidebar.button("⚙️ Hesap Ayarları", key="btn_toggle_settings_market", use_container_width=True):
        st.session_state['show_settings_market'] = not st.session_state['show_settings_market']
        st.session_state['show_feedback_market'] = False
    
    if st.session_state['show_settings_market']:
        with st.sidebar:
            user_email = st.session_state.get('user_email', '')
            subscription = get_user_subscription(user_email)
            
            if subscription and is_subscription_active(user_email):
                start_date = subscription.get('start_date', 'N/A')
                end_date = subscription.get('end_date', 'N/A')
                plan = subscription.get('plan', 'N/A')
                days_remaining = get_subscription_days_remaining(user_email)
                
                st.markdown(f"""
                <div style="background: rgba(37, 99, 235, 0.08); padding: 12px; border-radius: 8px; border: 1px solid rgba(37, 99, 235, 0.3); margin-top: 8px;">
                    <div style="font-weight: 600; font-size: 12px; color: #60a5fa; margin-bottom: 8px;">✅ Aktif Abonelik</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Başlangıç:</strong> {start_date}</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Bitiş:</strong> {end_date}</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95); margin: 4px 0;"><strong>Plan:</strong> {plan}</div>
                    <div style="font-size: 11px; color: #60a5fa; margin: 6px 0; font-weight: 600;">⏱️ Kalan: {days_remaining} gün</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: rgba(239, 68, 68, 0.08); padding: 12px; border-radius: 8px; border: 1px solid rgba(239, 68, 68, 0.3); margin-top: 8px;">
                    <div style="font-weight: 600; font-size: 12px; color: #fca5a5; margin-bottom: 6px;">⚠️ Abonelik Süresi Dolmuş</div>
                    <div style="font-size: 11px; color: rgba(148, 163, 184, 0.95);">Lütfen aboneliğinizi yenileyin.</div>
                </div>
                """, unsafe_allow_html=True)
            
            if st.button("❌ Kapat", key="btn_close_settings_market", use_container_width=True):
                st.session_state['show_settings_market'] = False
                st.rerun()

    # Ana veri çekme butonları ve işlemler burada devam edecek...
    # (Mevcut piyasa analizi kodları buraya taşınacak)
    
    # Veri görüntüleme bölümü
    show_market_data_display()

def show_market_data_display():
    """Piyasa verilerini görüntüle"""
    # Piyasa verilerini göster (Özet görünüm)
    if 'market_data' in st.session_state and st.session_state['market_data'] is not None:
        
        st.header(f"📊 {get_instrument_categories()[st.session_state.get('market_category', 'BIST')]['name']} Piyasa Verileri")
        
        # Tarih aralığını göster
        if 'market_date_range' in st.session_state:
            st.info(f"📅 Veri Tarihi: {st.session_state['market_date_range']}")
        
        market_df = st.session_state['market_data']
        
        # Filtreler
        col1, col2 = st.columns(2)
        
        with col1:
            # Enstrüman adına göre arama
            search_term = st.text_input("🔍 Enstrüman Ara:", placeholder="Kod veya adı girin...")
            
        with col2:
            # Sıralama seçenekleri
            sort_option = st.selectbox(
                "📊 Sıralama:",
                options=['Değişim % (Azalan)', 'Değişim % (Artan)', 'Son Fiyat (Azalan)', 'Son Fiyat (Artan)']
            )
        
        # Filtreleme uygula
        filtered_df = market_df.copy()
        
        if search_term:
            filtered_df = filtered_df[
                filtered_df['Kod'].str.contains(search_term, case=False, na=False) |
                filtered_df['Adı'].str.contains(search_term, case=False, na=False)
            ]
        
        # Sıralama uygula
        if sort_option == 'Değişim % (Azalan)':
            filtered_df = filtered_df.sort_values('Değişim %', ascending=False)
        elif sort_option == 'Değişim % (Artan)':
            filtered_df = filtered_df.sort_values('Değişim %', ascending=True)
        elif sort_option == 'Son Fiyat (Azalan)':
            filtered_df = filtered_df.sort_values('Son Fiyat', ascending=False)
        elif sort_option == 'Son Fiyat (Artan)':
            filtered_df = filtered_df.sort_values('Son Fiyat', ascending=True)
        
        # Verileri renkli formatta göster
        def color_negative_red(val):
            try:
                if isinstance(val, (int, float)) and val < 0:
                    return 'color: red'
                elif isinstance(val, (int, float)) and val > 0:
                    return 'color: green'
                else:
                    return ''
            except:
                return ''
        
        try:
            # Ana tablo - Render as dark-themed Plotly table
            display_columns = ['Kod', 'Adı', 'Son Fiyat', 'Değişim', 'Değişim %', 'En Yüksek', 'En Düşük', 'Para Birimi']
            available_columns = [col for col in display_columns if col in filtered_df.columns]
            
            display_df = filtered_df[available_columns].copy()
            
            # Format numeric columns
            for col in display_df.columns:
                if col in ['Son Fiyat', 'Değişim', 'En Yüksek', 'En Düşük']:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
                elif col == 'Değişim %':
                    display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "")
            
            market_table_fig = go.Figure(data=[go.Table(
                header=dict(
                    values=[f"<b>{c}</b>" for c in display_df.columns],
                    fill_color='rgba(22, 30, 46, 0.96)',
                    font=dict(color='#e2e8f0', size=12),
                    align='left'
                ),
                cells=dict(
                    values=[display_df[c].tolist() for c in display_df.columns],
                    fill_color=[['rgba(11, 18, 30, 0.78)' if i % 2 == 0 else 'rgba(7, 12, 22, 0.86)' for i in range(len(display_df))]],
                    font=dict(color='#e2e8f0', size=11),
                    align='left'
                )
            )])
            market_table_fig.update_layout(
                margin=dict(t=10, r=10, l=10, b=10),
                paper_bgcolor='rgba(9, 13, 24, 0.0)',
                plot_bgcolor='rgba(10, 18, 32, 0.92)',
                height=400
            )
            st.plotly_chart(market_table_fig, use_container_width=True)
        except Exception as e:
            # Fallback to styled dataframe
            display_columns = ['Kod', 'Adı', 'Son Fiyat', 'Değişim', 'Değişim %', 'En Yüksek', 'En Düşük', 'Para Birimi']
            available_columns = [col for col in display_columns if col in filtered_df.columns]
            st.dataframe(
                filtered_df[available_columns],
                use_container_width=True,
                height=400
            )
        
        # Özet istatistikler
        st.subheader("📈 Piyasa Özeti")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            positive_count = len(filtered_df[filtered_df['Değişim %'] > 0])
            st.metric("📈 Yükselen", positive_count)
        
        with col2:
            negative_count = len(filtered_df[filtered_df['Değişim %'] < 0])
            st.metric("📉 Düşen", negative_count)
        
        with col3:
            neutral_count = len(filtered_df[filtered_df['Değişim %'] == 0])
            st.metric("📊 Değişmeyen", neutral_count)
        
        with col4:
            avg_change = filtered_df['Değişim %'].mean()
            st.metric("📊 Ort. Değişim", f"{avg_change:.2f}%")

    # Detaylı verileri göster
    if 'detailed_data' in st.session_state and st.session_state['detailed_data'] is not None:
        
        st.header(f"📋 {get_instrument_categories()[st.session_state.get('detailed_category', 'BIST')]['name']} Detaylı Verileri")
        
        # Tarih aralığını göster
        if 'detailed_date_range' in st.session_state:
            st.info(f"📅 Veri Tarihi: {st.session_state['detailed_date_range']}")
        
        detailed_df = st.session_state['detailed_data']
        
        # Hangi enstrümanı göstereceğini seçme
        if 'detailed_category' in st.session_state and st.session_state['detailed_category'] == 'BIST':
            instrument_column = 'Hisse Kodu'
            name_column = 'Hisse Adı'
        else:
            instrument_column = 'Kod'
            name_column = 'Adı'
        
        display_instrument = st.selectbox(
            "Gösterilecek Enstrüman:",
            options=detailed_df[instrument_column].unique()
        )
        
        if display_instrument:
            instrument_df = detailed_df[detailed_df[instrument_column] == display_instrument].copy()
            instrument_df = instrument_df.sort_values('Tarih', ascending=False)
            
            # Tabloyu göster
            if 'detailed_category' in st.session_state and st.session_state['detailed_category'] == 'BIST':
                table_columns = ['Tarih', 'Hisse Kodu', 'Hisse Adı', 'Açılış', 'En Yüksek', 'En Düşük', 'Kapanış', 'Hacim', 'Para Birimi']
            else:
                table_columns = ['Tarih', 'Kod', 'Adı', 'Açılış', 'En Yüksek', 'En Düşük', 'Kapanış', 'Hacim', 'Para Birimi']
            
            available_columns = [col for col in table_columns if col in instrument_df.columns]
            
            # Render as dark-themed Plotly table matching Portfolio Details
            try:
                display_df = instrument_df[available_columns].copy()
                
                # Format numeric columns for better display
                for col in display_df.columns:
                    if col in ['Açılış', 'En Yüksek', 'En Düşük', 'Kapanış']:
                        display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
                    elif col == 'Hacim':
                        display_df[col] = display_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) and x != 0 else "0")
                
                table_fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=[f"<b>{c}</b>" for c in display_df.columns],
                        fill_color='rgba(22, 30, 46, 0.96)',
                        font=dict(color='#e2e8f0', size=12),
                        align='left'
                    ),
                    cells=dict(
                        values=[display_df[c].tolist() for c in display_df.columns],
                        fill_color=[['rgba(11, 18, 30, 0.78)' if i % 2 == 0 else 'rgba(7, 12, 22, 0.86)' for i in range(len(display_df))]],
                        font=dict(color='#e2e8f0', size=11),
                        align='left'
                    )
                )])
                table_fig.update_layout(
                    margin=dict(t=10, r=10, l=10, b=10),
                    paper_bgcolor='rgba(9, 13, 24, 0.0)',
                    plot_bgcolor='rgba(10, 18, 32, 0.92)',
                    height=min(700, 36 * (len(display_df) + 2))
                )
                st.plotly_chart(table_fig, use_container_width=True)
            except Exception as e:
                # Fallback to standard dataframe if Plotly fails
                st.dataframe(
                    instrument_df[available_columns],
                    use_container_width=True
                )
            
            # Özet istatistikler
            st.subheader(f"📊 {display_instrument} Özet İstatistikler")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                current_price = instrument_df['Kapanış'].iloc[0]
                prev_price = instrument_df['Kapanış'].iloc[1] if len(instrument_df) > 1 else current_price
                change_percent = ((current_price - prev_price) / prev_price) * 100 if prev_price > 0 else 0
                currency = instrument_df.get('Para Birimi', {}).iloc[0] if 'Para Birimi' in instrument_df.columns else ""
                st.metric(
                    "Son Fiyat",
                    f"{current_price:.2f} {currency}",
                    f"{change_percent:.2f}%"
                )
            
            with col2:
                max_high = instrument_df['En Yüksek'].max()
                st.metric("En Yüksek", f"{max_high:.2f} {currency}")
            
            with col3:
                min_low = instrument_df['En Düşük'].min()
                st.metric("En Düşük", f"{min_low:.2f} {currency}")
            
            with col4:
                avg_volume = instrument_df['Hacim'].mean()
                st.metric("Ortalama Hacim", f"{avg_volume:,.0f}")
            
            # Grafik görünümü
            st.subheader("📈 Fiyat Grafiği")
            
            # Modern grafik türü seçimi
            st.markdown("""
            <style>
            .chart-type-container {
                background: linear-gradient(135deg, #e8f5e8 0%, #f0f8ff 100%);
                padding: 15px;
                border-radius: 10px;
                margin: 15px 0;
                border-left: 4px solid #27ae60;
            }
            </style>
            """, unsafe_allow_html=True)
            
            with st.container():
                st.markdown('<div class="chart-type-container">', unsafe_allow_html=True)
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**🎨 Grafik Türü Seçimi:**")
                with col2:
                    chart_type = st.selectbox(
                        "Grafik türünü seçin:",
                        options=["Çizgi Grafik", "Mum Grafik"],
                        format_func=lambda x: f"📊 {x}" if x == "Çizgi Grafik" else f"🕯️ {x}",
                        help="Görüntülemek istediğiniz grafik türünü seçin",
                        label_visibility="collapsed"
                    )
                st.markdown('</div>', unsafe_allow_html=True)
            
            if chart_type == "Çizgi Grafik":
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=instrument_df['Tarih'],
                    y=instrument_df['Kapanış'],
                    mode='lines+markers',
                    name=f'{display_instrument} Kapanış',
                    line=dict(width=2)
                ))
                
                fig.update_layout(
                    title=f"{display_instrument} Fiyat Grafiği",
                    xaxis_title="Tarih",
                    yaxis_title=f"Fiyat ({currency})",
                    height=400,
                    paper_bgcolor='rgba(9, 13, 24, 0.0)',
                    plot_bgcolor='rgba(10, 18, 32, 0.92)',
                    font=dict(color='#e2e8f0'),
                    xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                    yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                    legend=dict(font=dict(color='#e2e8f0'))
                )
                
            else:  # Mum Grafik
                fig = go.Figure(data=go.Candlestick(
                    x=instrument_df['Tarih'],
                    open=instrument_df['Açılış'],
                    high=instrument_df['En Yüksek'],
                    low=instrument_df['En Düşük'],
                    close=instrument_df['Kapanış'],
                    name=display_instrument
                ))
                
                fig.update_layout(
                    title=f"{display_instrument} Mum Grafiği",
                    xaxis_title="Tarih",
                    yaxis_title=f"Fiyat ({currency})",
                    height=400,
                    paper_bgcolor='rgba(9, 13, 24, 0.0)',
                    plot_bgcolor='rgba(10, 18, 32, 0.92)',
                    font=dict(color='#e2e8f0'),
                    xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                    yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                    legend=dict(font=dict(color='#e2e8f0'))
                )
            
            st.plotly_chart(fig, use_container_width=True)

            # BIST kategorisinde teknik analiz verilerini ekle
            if 'detailed_category' in st.session_state and st.session_state['detailed_category'] == 'BIST':
                st.subheader("📊 Teknik Analiz Verileri")
                
                try:
                    # Teknik indikatörleri hesapla
                    technical_data = calculate_technical_indicators(instrument_df)
                    
                    # Teknik indikatör grafikleri
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**RSI (Relative Strength Index)**")
                        fig_rsi = go.Figure()
                        fig_rsi.add_trace(go.Scatter(
                            x=technical_data['Tarih'],
                            y=technical_data['RSI'],
                            mode='lines',
                            name='RSI',
                            line=dict(color='purple')
                        ))
                        
                        # RSI seviye çizgileri
                        fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Aşırı Alım (70)")
                        fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Aşırı Satım (30)")
                        
                        fig_rsi.update_layout(
                            title="RSI İndikatörü",
                            xaxis_title="Tarih",
                            yaxis_title="RSI",
                            height=300,
                            yaxis_range=[0, 100],
                            paper_bgcolor='rgba(9, 13, 24, 0.0)',
                            plot_bgcolor='rgba(10, 18, 32, 0.92)',
                            font=dict(color='#e2e8f0'),
                            xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                            yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)')
                        )
                        st.plotly_chart(fig_rsi, use_container_width=True)
                        
                        # RSI yorumu
                        latest_rsi = technical_data['RSI'].iloc[-1] if not technical_data['RSI'].empty else 50
                        if latest_rsi > 70:
                            st.warning(f"⚠️ RSI: {latest_rsi:.1f} - Aşırı alım bölgesinde")
                        elif latest_rsi < 30:
                            st.success(f"✅ RSI: {latest_rsi:.1f} - Aşırı satım bölgesinde")
                        else:
                            st.info(f"📊 RSI: {latest_rsi:.1f} - Normal seviyede")
                    
                    with col2:
                        st.markdown("**MACD (Moving Average Convergence Divergence)**")
                        fig_macd = go.Figure()
                        fig_macd.add_trace(go.Scatter(
                            x=technical_data['Tarih'],
                            y=technical_data['MACD'],
                            mode='lines',
                            name='MACD',
                            line=dict(color='blue')
                        ))
                        fig_macd.add_trace(go.Scatter(
                            x=technical_data['Tarih'],
                            y=technical_data['MACD_Signal'],
                            mode='lines',
                            name='Signal',
                            line=dict(color='red')
                        ))
                        fig_macd.add_trace(go.Bar(
                            x=technical_data['Tarih'],
                            y=technical_data['MACD_Histogram'],
                            name='Histogram',
                            marker_color='gray',
                            opacity=0.6
                        ))
                        
                        fig_macd.update_layout(
                            title="MACD İndikatörü",
                            xaxis_title="Tarih",
                            yaxis_title="MACD",
                            height=300,
                            paper_bgcolor='rgba(9, 13, 24, 0.0)',
                            plot_bgcolor='rgba(10, 18, 32, 0.92)',
                            font=dict(color='#e2e8f0'),
                            xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                            yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                            legend=dict(font=dict(color='#e2e8f0'))
                        )
                        st.plotly_chart(fig_macd, use_container_width=True)
                        
                        # MACD yorumu
                        latest_macd = technical_data['MACD'].iloc[-1] if not technical_data['MACD'].empty else 0
                        latest_signal = technical_data['MACD_Signal'].iloc[-1] if not technical_data['MACD_Signal'].empty else 0
                        if latest_macd > latest_signal:
                            st.success(f"✅ MACD > Signal - Yükseliş sinyali")
                        else:
                            st.warning(f"⚠️ MACD < Signal - Düşüş sinyali")
                    
                    # Bollinger Bands ve Hareketli Ortalamalar
                    st.markdown("**Bollinger Bands ve Hareketli Ortalamalar**")
                    fig_bb = go.Figure()
                    
                    # Fiyat çizgisi
                    fig_bb.add_trace(go.Scatter(
                        x=technical_data['Tarih'],
                        y=technical_data['Kapanış'],
                        mode='lines',
                        name='Kapanış Fiyatı',
                        line=dict(color='black', width=2)
                    ))
                    
                    # Bollinger Bands
                    fig_bb.add_trace(go.Scatter(
                        x=technical_data['Tarih'],
                        y=technical_data['BB_Upper'],
                        mode='lines',
                        name='BB Üst',
                        line=dict(color='red', dash='dash'),
                        fill=None
                    ))
                    fig_bb.add_trace(go.Scatter(
                        x=technical_data['Tarih'],
                        y=technical_data['BB_Lower'],
                        mode='lines',
                        name='BB Alt',
                        line=dict(color='red', dash='dash'),
                        fill='tonexty',
                        fillcolor='rgba(255,0,0,0.1)'
                    ))
                    
                    # Hareketli ortalamalar
                    fig_bb.add_trace(go.Scatter(
                        x=technical_data['Tarih'],
                        y=technical_data['SMA_20'],
                        mode='lines',
                        name='SMA 20',
                        line=dict(color='blue')
                    ))
                    fig_bb.add_trace(go.Scatter(
                        x=technical_data['Tarih'],
                        y=technical_data['SMA_50'],
                        mode='lines',
                        name='SMA 50',
                        line=dict(color='orange')
                    ))
                    
                    fig_bb.update_layout(
                        title="Bollinger Bantları ve Hareketli Ortalamalar",
                        xaxis_title="Tarih",
                        yaxis_title=f"Fiyat ({currency})",
                        height=350,
                        paper_bgcolor='rgba(9, 13, 24, 0.0)',
                        plot_bgcolor='rgba(10, 18, 32, 0.92)',
                        font=dict(color='#e2e8f0'),
                        xaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                        yaxis=dict(color='#e2e8f0', gridcolor='rgba(148, 163, 184, 0.18)'),
                        legend=dict(font=dict(color='#e2e8f0'))
                    )
                    st.plotly_chart(fig_bb, use_container_width=True)
                    st.markdown("**📊 Teknik Analiz Özeti**")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        current_price = technical_data['Kapanış'].iloc[-1]
                        sma_20 = technical_data['SMA_20'].iloc[-1]
                        trend_20 = "📈 Yükseliş" if current_price > sma_20 else "📉 Düşüş"
                        st.metric("SMA 20 Trendi", trend_20, f"{current_price - sma_20:.2f}")
                    
                    with col2:
                        sma_50 = technical_data['SMA_50'].iloc[-1]
                        trend_50 = "📈 Yükseliş" if current_price > sma_50 else "📉 Düşüş"
                        st.metric("SMA 50 Trendi", trend_50, f"{current_price - sma_50:.2f}")
                    
                    with col3:
                        bb_position = "Üst Band Yakın" if current_price > technical_data['BB_Upper'].iloc[-1] * 0.98 else \
                                      "Alt Band Yakın" if current_price < technical_data['BB_Lower'].iloc[-1] * 1.02 else \
                                      "Normal Aralık"
                        st.metric("BB Konumu", bb_position)
                
                except Exception as e:
                    st.error(f"❌ Teknik analiz hesaplama hatası: {str(e)}")

# Teknik indikatörleri hesaplayan fonksiyon
def calculate_technical_indicators(df):
    """BIST hisseleri için teknik indikatörleri hesapla"""
    try:
        # DataFrame'i kopyala ve tarihe göre sırala
        data = df.copy().sort_values('Tarih', ascending=True).reset_index(drop=True)
        
        # Temel veriler
        close_prices = data['Kapanış'].astype(float)
        high_prices = data['En Yüksek'].astype(float)
        low_prices = data['En Düşük'].astype(float)
        
        # RSI hesaplama
        def calculate_rsi(prices, window=14):
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            rs = gain / loss
            return 100 - (100 / (1 + rs))
        
        data['RSI'] = calculate_rsi(close_prices)
        
        # MACD hesaplama
        ema_12 = close_prices.ewm(span=12).mean()
        ema_26 = close_prices.ewm(span=26).mean()
        data['MACD'] = ema_12 - ema_26
        data['MACD_Signal'] = data['MACD'].ewm(span=9).mean()
        data['MACD_Histogram'] = data['MACD'] - data['MACD_Signal']
        
        # Hareketli ortalamalar
        data['SMA_20'] = close_prices.rolling(window=20).mean()
        data['SMA_50'] = close_prices.rolling(window=50).mean()
        
        # Bollinger Bands
        sma_20 = data['SMA_20']
        std_20 = close_prices.rolling(window=20).std()
        data['BB_Upper'] = sma_20 + (std_20 * 2)
        data['BB_Lower'] = sma_20 - (std_20 * 2)
        
        return data
        
    except Exception as e:
        st.error(f"Teknik indikatör hesaplama hatası: {str(e)}")
        return df

# Tüm BIST hisse senetlerini almak için yöntem
@st.cache_data(ttl=3600)  # 1 saatlik önbellek
def fetch_all_bist_stocks():
    """GitHub repository API'sinden tüm BIST hisselerini çek - HARDCODEsız"""
    stocks_dict = {}
    
    try:
        st.info("� GitHub BIST API'sinden hisse listesi çekiliyor...")
        
        # Metod 1: GitHub Repository'den JSON verisini çek (logo'lu versiyon)
        github_api_url = "https://cdn.jsdelivr.net/gh/ahmeterenodaci/Istanbul-Stock-Exchange--BIST--including-symbols-and-logos/bist.min.json"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Cache-Control': 'no-cache'
        }
        
        response = requests.get(github_api_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            bist_data = response.json()
            st.success(f"✅ GitHub API'sinden {len(bist_data)} BIST hissesi alındı!")
            
            # Her hisse için detaylı bilgi oluştur
            for stock in bist_data:
                try:
                    symbol = stock.get('symbol', '').strip().upper()
                    name = stock.get('name', '').strip()
                    logo_url = stock.get('logoUrl', '')
                    
                    if symbol and name and len(symbol) >= 3 and len(symbol) <= 6:
                        # Sadece uzun ad ile basit format
                        stocks_dict[symbol] = name
                except Exception as e:
                    continue
            
            # Basit format kullandığımız için ek zenginleştirme yapmıyoruz
            st.success(f"✅ Toplam {len(stocks_dict)} BIST hissesi hazırlandı!")
        
        else:
            st.warning(f"⚠️ GitHub API hatası: {response.status_code}")
            
            # Fallback: Logo'suz versiyonu dene
            fallback_url = "https://cdn.jsdelivr.net/gh/ahmeterenodaci/Istanbul-Stock-Exchange--BIST--including-symbols-and-logos/without_logo.min.json"
            
            response = requests.get(fallback_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                bist_data = response.json()
                st.info(f"📊 Fallback API'sinden {len(bist_data)} BIST hissesi alındı!")
                
                for stock in bist_data:
                    try:
                        symbol = stock.get('symbol', '').strip().upper()
                        name = stock.get('name', '').strip()
                        
                        if symbol and name and len(symbol) >= 3 and len(symbol) <= 6:
                            # Sadece kısa ad ile basit format
                            short_name = name.split()[0] if len(name.split()) > 1 else name[:15]
                            stocks_dict[symbol] = short_name
                    except Exception as e:
                        continue
        
        # Metod 3: Eğer GitHub API'si başarısız olursa KAP API'yi dene
        if len(stocks_dict) < 50:  # Çok az hisse varsa
            st.info("🏛️ GitHub API'sinden yeterli veri alınamadı, KAP API'sini deniyorum...")
            kap_stocks = fetch_from_kap_api()
            
            # KAP API de artık basit format döndürüyor
            stocks_dict.update(kap_stocks)
        
        st.success(f"🎉 Toplam {len(stocks_dict)} BIST hissesi başarıyla çekildi!")
        print(f"📊 GitHub BIST API Success: {len(stocks_dict)} stocks loaded")
        
        return stocks_dict
        
    except Exception as e:
        st.error(f"❌ BIST API çekme hatası: {str(e)}")
        print(f"Error in BIST API fetch: {str(e)}")
        
        # Son fallback: KAP API
        try:
            st.info("🔄 Fallback: KAP API'sinden hisse listesi çekiliyor...")
            kap_stocks = fetch_from_kap_api()
            return kap_stocks
        except:
            return {}

def enrich_stocks_with_yahoo_finance(stocks_dict, max_stocks=50):
    """GitHub'dan alınan hisseleri Yahoo Finance ile zenginleştir"""
    enriched_stocks = {}
    
    try:
        # Sadece ilk max_stocks kadar hisseyi zenginleştir (performans için)
        stocks_to_enrich = list(stocks_dict.keys())[:max_stocks]
        
        # Batch işleme
        batch_size = 20
        total_batches = len(stocks_to_enrich) // batch_size + (1 if len(stocks_to_enrich) % batch_size != 0 else 0)
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(stocks_to_enrich))
            batch_symbols = stocks_to_enrich[start_idx:end_idx]
            
            try:
                # Yahoo Finance'den batch bilgilerini çek
                batch_with_suffix = [f"{symbol}.IS" for symbol in batch_symbols]
                tickers = yf.Tickers(" ".join(batch_with_suffix))
                
                for symbol in batch_symbols:
                    symbol_with_suffix = f"{symbol}.IS"
                    try:
                        ticker = tickers.tickers[symbol_with_suffix]
                        info = ticker.info
                        
                        # Mevcut GitHub bilgilerini al
                        stock_info = stocks_dict[symbol].copy()
                        
                        # Yahoo Finance'den gelen ek bilgileri ekle
                        if info and info.get('symbol'):
                            if info.get('sector'):
                                stock_info['sector'] = info.get('sector', '')
                            if info.get('industry'):
                                stock_info['industry'] = info.get('industry', '')
                            if info.get('marketCap'):
                                stock_info['marketCap'] = info.get('marketCap', 0)
                            if info.get('fullTimeEmployees'):
                                stock_info['employees'] = info.get('fullTimeEmployees', 0)
                            if info.get('website'):
                                stock_info['website'] = info.get('website', '')
                            
                            stock_info['source'] = 'GitHub_BIST_API + Yahoo_Finance'
                            enriched_stocks[symbol] = stock_info
                            
                    except Exception as e:
                        # Yahoo Finance'den bilgi alınamazsa orijinal bilgiyi koru
                        enriched_stocks[symbol] = stocks_dict[symbol]
                        continue
                
                # Rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                # Batch başarısız olursa orijinal bilgileri koru
                for symbol in batch_symbols:
                    enriched_stocks[symbol] = stocks_dict[symbol]
                continue
        
        return enriched_stocks
        
    except Exception as e:
        # Zenginleştirme başarısız olursa orijinal bilgileri döndür
        return stocks_dict

def discover_all_bist_symbols_dynamically():
    """Tüm potansiyel BIST sembollerini dinamik olarak keşfet - HARDCODEsız"""
    potential_symbols = set()
    
    try:
        # Metod 1: Alfabetik kombinasyon ile sembol keşfi
        st.info("🔤 Alfabetik kombinasyonlar ile BIST sembolleri keşfediliyor...")
        alphabet_symbols = generate_alphabet_combinations()
        potential_symbols.update(alphabet_symbols)
        
        # Metod 2: Borsa İstanbul web sitesinden dinamik çekme
        st.info("🌐 Borsa İstanbul web sitesinden sembol listesi çekiliyor...")
        web_symbols = fetch_symbols_from_borsa_istanbul_web()
        potential_symbols.update(web_symbols)
        
        # Metod 3: KAP API'den hisse kodlarını çekme
        st.info("🏛️ KAP API'den hisse kodları çekiliyor...")
        kap_symbols = fetch_symbols_from_kap()
        potential_symbols.update(kap_symbols)
        
        # Metod 4: Yahoo Finance'den IST exchange taraması
        st.info("📊 Yahoo Finance IST exchange taraması yapılıyor...")
        yahoo_symbols = scan_yahoo_ist_exchange()
        potential_symbols.update(yahoo_symbols)
        
        # Dublikatları temizle ve sırala
        final_symbols = sorted(list(potential_symbols))
        st.info(f"✅ {len(final_symbols)} benzersiz BIST sembolü keşfedildi!")
        
        return final_symbols
        
    except Exception as e:
        st.warning(f"⚠️ Dinamik sembol keşfinde hata: {str(e)}")
        # Fallback: Minimal alfabetik keşif
        return generate_minimal_alphabet_combinations()

def generate_alphabet_combinations():
    """Türkçe şirket isimleri ve genel patternlere dayalı sembol kombinasyonları üret"""
    symbols = set()
    
    # Türk şirketlerinde sık kullanılan prefix'ler
    common_prefixes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'U', 'V', 'Y', 'Z']
    
    # Türk şirketlerinde sık kullanılan suffix'ler
    common_patterns = [
        'BNK', 'GYO', 'TEK', 'SAN', 'YAP', 'TAS', 'CAM', 'DEX', 'ENJ', 'FIN',
        'HOL', 'IND', 'KAG', 'LAB', 'MED', 'NET', 'OTO', 'PAZ', 'REY', 'SIG',
        'TUR', 'ULK', 'VEN', 'WEB', 'YAT', 'ZIR'
    ]
    
    # 3-6 karakter kombinasyonları
    for prefix in common_prefixes:
        for pattern in common_patterns[:10]:  # İlk 10 pattern ile sınırla
            # 5-6 karakter semboller
            symbol = (prefix + pattern)[:5]
            if len(symbol) >= 3:
                symbols.add(symbol)
        
        # Kısa semboller (3-4 karakter)
        for i in range(65, 91):  # A-Z
            for j in range(65, 91):
                symbol = prefix + chr(i) + chr(j)
                if len(symbol) <= 4:
                    symbols.add(symbol)
                
                # 4 karakterli
                for k in range(65, 91):
                    symbol4 = prefix + chr(i) + chr(j) + chr(k)
                    if len(symbol4) == 4:
                        symbols.add(symbol4)
    
    return list(symbols)[:2000]  # Çok fazla olmasın diye sınırla

def fetch_symbols_from_borsa_istanbul_web():
    """Borsa İstanbul web sitesinden hisse kodlarını çek"""
    symbols = []
    
    try:
        # Borsa İstanbul'un hisse listesi sayfası
        url = "https://www.borsaistanbul.com/tr/sayfa/1/endeksler"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            # Basit pattern matching ile hisse kodlarını bul
            import re
            
            # BIST hisse kodu pattern'i (3-5 harf + rakam kombinasyonu)
            pattern = r'\b[A-Z]{3,6}\b'
            matches = re.findall(pattern, response.text)
            
            # Filtreleme: Gerçek hisse kodu gibi görünenler
            for match in matches:
                if (len(match) >= 3 and len(match) <= 6 and 
                    not match in ['HTML', 'HTTP', 'HTTPS', 'FORM', 'BODY', 'HEAD', 'SCRIPT']):
                    symbols.append(match)
        
        # Duplikatları temizle
        symbols = list(set(symbols))
        
    except Exception as e:
        st.warning(f"⚠️ Borsa İstanbul web scraping hatası: {str(e)}")
    
    return symbols[:100]  # İlk 100 sembol

def fetch_symbols_from_kap():
    """KAP API'den hisse kodlarını çek"""
    symbols = []
    
    try:
        url = "https://www.kap.org.tr/tr/api/memberCompanies"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.kap.org.tr/'
        }
        
        response = requests.get(url, headers=headers, timeout=20)
        
        if response.status_code == 200:
            data = response.json()
            
            for company in data:
                try:
                    stock_codes = company.get('stockCodes', [])
                    for stock in stock_codes:
                        code = stock.get('code', '').strip()
                        if code and len(code) >= 3 and len(code) <= 6:
                            symbols.append(code)
                except:
                    continue
        
    except Exception as e:
        st.warning(f"⚠️ KAP API hatası: {str(e)}")
    
    return symbols

def scan_yahoo_ist_exchange():
    """Yahoo Finance'den IST exchange'inde hisse tarama"""
    symbols = []
    
    try:
        # Yahoo Finance search API endpoint'i
        search_terms = ['TR', 'BIST', 'Istanbul', 'Turkey']
        
        for term in search_terms:
            try:
                # Yahoo Finance search (genel arama)
                search_url = f"https://query1.finance.yahoo.com/v1/finance/search?q={term}&lang=en-US&region=US&quotesCount=50&newsCount=0"
                
                response = requests.get(search_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for quote in data.get('quotes', []):
                        symbol = quote.get('symbol', '')
                        exchange = quote.get('exchange', '')
                        
                        if '.IS' in symbol and 'IST' in exchange.upper():
                            clean_symbol = symbol.replace('.IS', '')
                            if len(clean_symbol) <= 6:
                                symbols.append(clean_symbol)
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                continue
        
    except Exception as e:
        pass
    
    return list(set(symbols))

def generate_minimal_alphabet_combinations():
    """Minimal fallback sembol kombinasyonları"""
    symbols = []
    
    # En temel kombinasyonlar
    letters = 'ABCDEFGHIKLMNOPRSTUVYZ'
    
    for i in letters:
        for j in letters:
            for k in letters:
                symbols.append(i + j + k)
                if len(symbols) >= 500:  # 500 ile sınırla
                    return symbols
    
    return symbols

def fetch_from_kap_api():
    """KAP API'den detaylı hisse bilgilerini çek"""
    stocks = {}
    
    try:
        url = "https://www.kap.org.tr/tr/api/memberCompanies"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'tr-TR,tr;q=0.9,en;q=0.8',
            'Referer': 'https://www.kap.org.tr/'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            for company in data:
                try:
                    # Şirket bilgilerini çek
                    company_title = company.get('title', '')
                    main_sector = company.get('mainSector', '')
                    sub_sector = company.get('subSector', '')
                    is_active = company.get('isActive', True)
                    market_value = company.get('marketValue', 0)
                    first_trading_date = company.get('firstTradingDate', '')
                    
                    # Hisse kodlarını çek
                    stock_codes = company.get('stockCodes', [])
                    
                    for stock in stock_codes:
                        try:
                            code = stock.get('code', '').strip()
                            if code and len(code) >= 3 and len(code) <= 6 and is_active:
                                # Basit format: Sadece uzun ad
                                stocks[code] = company_title
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    continue
        else:
            st.warning(f"⚠️ KAP API HTTP hatası: {response.status_code}")
            
    except Exception as e:
        st.warning(f"⚠️ KAP API bağlantı hatası: {str(e)}")
    
    return stocks

# BIST hisse kodları - Sadece Parquet dosyasından okunur
try:
    # Parquet dosyası varsa oradan oku
    bist_from_file, _ = load_bist_stocks_from_parquet()
    if bist_from_file:
        BIST_STOCKS = bist_from_file
    else:
        BIST_STOCKS = {}
except:
    BIST_STOCKS = {}

# NASDAQ hisse kodları - Sadece Parquet dosyasından okunur
try:
    # Parquet dosyası varsa oradan oku
    nasdaq_from_file, _ = load_nasdaq_stocks_from_parquet()
    if nasdaq_from_file:
        NASDAQ_STOCKS = nasdaq_from_file
    else:
        NASDAQ_STOCKS = {}
except:
    NASDAQ_STOCKS = {}

def get_bist_stocks_dynamic():
    """BIST hisselerini Parquet dosyasından oku - artık dinamik çekme yapmaz"""
    try:
        # Önce Parquet dosyasından oku
        stocks_dict, _ = load_bist_stocks_from_parquet()
        
        # Eğer dosyada veri varsa kullan
        if stocks_dict and len(stocks_dict) > 10:
            return stocks_dict
        
        # Eğer dosyada veri yoksa minimal liste döndür
        return {
            "AKBNK": "Akbank", "GARAN": "Garanti BBVA", "ISCTR": "İş Bankası",
            "VAKBN": "VakıfBank", "YKBNK": "Yapı Kredi", "HALKB": "Halkbank",
            "THYAO": "Türk Hava Yolları", "ASELS": "Aselsan", "TUPRS": "Tüpraş"
        }
    except Exception as e:
        print(f"⚠️ BIST hisse okuma hatası: {str(e)}")
        return {
            "AKBNK": "Akbank", "GARAN": "Garanti BBVA", "THYAO": "Türk Hava Yolları"
        }

def get_stock_display_name(stock_code):
    """Hisse kodundan görüntüleme adını al"""
    try:
        # Önce dynamic BIST stocks'tan bak
        bist_stocks = get_bist_stocks_dynamic()
        
        if stock_code in bist_stocks:
            stock_info = bist_stocks[stock_code]
            # Artık basit string formatında
            return str(stock_info).strip()
        
        # Eğer BIST stocks'ta yoksa Yahoo Finance'den al (session ile)
        try:
            ticker = yf.Ticker(f"{stock_code}.IS", session=YF_SESSION)
            info = ticker.info
            if info and info.get('longName'):
                return str(info['longName']).strip()
            elif info and info.get('shortName'):
                return str(info['shortName']).strip()
        except:
            pass
        
        # Son fallback: hisse kodunu döndür
        return stock_code
        
    except Exception as e:
        return stock_code

def get_instrument_display_name(instrument_code, category, current_instruments):
    """Enstrüman kodundan kategoriye göre görüntüleme adını al"""
    try:
        if category == "BIST":
            # BIST hisseleri için özel fonksiyon
            return get_stock_display_name(instrument_code)
        
        elif category == "NASDAQ":
            # NASDAQ hisseleri için
            nasdaq_stocks = get_nasdaq_stocks_dynamic()
            if instrument_code in nasdaq_stocks:
                nasdaq_info = nasdaq_stocks[instrument_code]
                if isinstance(nasdaq_info, dict):
                    name = nasdaq_info.get('longName') or nasdaq_info.get('shortName') or instrument_code
                    return str(name).strip()
                else:
                    return str(nasdaq_info).strip()
        
        # Diğer kategoriler için (FOREX, METALS, CRYPTO, TEFAS, CASH)
        if instrument_code in current_instruments:
            instrument_info = current_instruments[instrument_code]
            if isinstance(instrument_info, dict):
                # Detaylı bilgi varsa
                name = instrument_info.get('longName') or instrument_info.get('name') or instrument_info.get('shortName') or instrument_code
                return str(name).strip()
            else:
                # Basit string ise
                return str(instrument_info).strip()
        
        # Fallback: enstrüman kodunu döndür
        return instrument_code
        
    except Exception as e:
        return instrument_code

def get_nasdaq_stocks_dynamic():
    """NASDAQ hisselerini Parquet dosyasından oku - artık dinamik çekme yapmaz"""
    try:
        # Önce Parquet dosyasından oku
        stocks_dict, _ = load_nasdaq_stocks_from_parquet()
        
        # Eğer dosyada veri varsa kullan
        if stocks_dict and len(stocks_dict) > 10:
            return stocks_dict
        
        # Eğer dosyada veri yoksa minimal liste döndür
        return {
            "AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corporation",
            "AMZN": "Amazon.com Inc.", "TSLA": "Tesla Inc.", "META": "Meta Platforms Inc.",
            "NVDA": "NVIDIA Corporation", "NFLX": "Netflix Inc.", "ADBE": "Adobe Inc."
        }
    except Exception as e:
        print(f"⚠️ NASDAQ hisse okuma hatası: {str(e)}")
        return {
            "AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corporation"
        }

# NASDAQ hisse senetleri artık dinamik olarak Parquet dosyasından okunur
# get_nasdaq_stocks_smart() fonksiyonu kullanılır

# Kıymetli Madenler - Kapsamlı Liste
PRECIOUS_METALS = {
    # Türk Altın Enstrümanları - EN ÜST SIRADA
    "ALTIN_GRAM": "Altın (Gram/TRY)",
    "ALTIN_CEYREK": "Çeyrek Altın (TRY)",
    "ALTIN_YARIM": "Yarım Altın (TRY)",
    "ALTIN_TAM": "Tam Altın (TRY)",
    "ALTIN_ONS_TRY": "Altın (Ons/TRY)",
    "ALTIN_RESAT": "Reşat Altını (TRY)",
    "ALTIN_CUMHURIYET": "Cumhuriyet Altını (TRY)",
    "ALTIN_ATA": "Ata Altını (TRY)",
    "ALTIN_HAMIT": "Hamit Altını (TRY)",
    "ALTIN_IKIBUCUK": "İki Buçuk Altın (TRY)",
    "ALTIN_BESLI": "Beşli Altın (TRY)",
    "ALTIN_14AYAR": "14 Ayar Altın (TRY)",
    "ALTIN_18AYAR": "18 Ayar Altın (TRY)",
    "ALTIN_22AYAR_BILEZIK": "22 Ayar Bilezik (TRY)",
    
    # Ana Kıymetli Madenler
    "GC=F": "Altın (Gold)",
    "SI=F": "Gümüş (Silver)", 
    "PL=F": "Platin (Platinum)",
    "PA=F": "Paladyum (Palladium)",
    
    # Endüstriyel Metaller
    "HG=F": "Bakır (Copper)",
    "ALI=F": "Alüminyum (Aluminum)",
    "ZN=F": "Çinko (Zinc)",
    "NI=F": "Nikel (Nickel)",
    
    # BIST Altın Fonları
    "GLDTR.IS": "Altın TRY/Ons (BIST)",
    "ALTIN.IS": "İş Altın Fonu (BIST)",
    "GOLTR.IS": "QNB Finans Altın Fonu (BIST)",
    "AGLDX.IS": "Ak Altın Fonu (BIST)",
    "GLDA.IS": "Ata Altın Fonu (BIST)",
    
    # ETF'ler
    "GLD": "SPDR Gold Shares",
    "SLV": "iShares Silver Trust",
    "IAU": "iShares Gold Trust",
    "SGOL": "abrdn Physical Gold Shares",
    "PSLV": "Sprott Physical Silver Trust",
    "PHYS": "Sprott Physical Gold Trust",
    
    # Madencilik Şirketleri
    "GOLD": "Barrick Gold Corporation",
    "NEM": "Newmont Corporation",
    "AEM": "Agnico Eagle Mines",
    "KGC": "Kinross Gold Corporation",
    "AU": "AngloGold Ashanti Limited",
    "PAAS": "Pan American Silver Corp",
    "HL": "Hecla Mining Company",
    "AG": "First Majestic Silver Corp",
    "WPM": "Wheaton Precious Metals",
    "FNV": "Franco-Nevada Corporation",
    
    # Türk Madencilik Şirketleri
    "KOZAL.IS": "Koza Altın (BIST)",
    "KOZAA.IS": "Koza Anadolu Metal (BIST)",
    "TUCLK.IS": "Turkcell (BIST)",
    "EREGL.IS": "Ereğli Demir Çelik (BIST)",
    "KRDMD.IS": "Kardemir (BIST)",
    
    # Diğer Emtialar
    "CL=F": "Ham Petrol (Crude Oil)",
    "NG=F": "Doğal Gaz (Natural Gas)",
    "RB=F": "Benzin (Gasoline)",
    "HO=F": "Fuel Oil"
}

# Döviz Kurları - Kapsamlı Liste
FOREX_PAIRS = {
    # Majör Döviz Çiftleri
    "EURUSD=X": "EUR/USD",
    "GBPUSD=X": "GBP/USD", 
    "USDJPY=X": "USD/JPY",
    "USDCHF=X": "USD/CHF",
    "AUDUSD=X": "AUD/USD",
    "USDCAD=X": "USD/CAD",
    "NZDUSD=X": "NZD/USD",
    
    # Çapraz Döviz Çiftleri
    "EURGBP=X": "EUR/GBP",
    "EURJPY=X": "EUR/JPY",
    "GBPJPY=X": "GBP/JPY",
    "EURCHF=X": "EUR/CHF",
    "GBPCHF=X": "GBP/CHF",
    "EURAUD=X": "EUR/AUD",
    "GBPAUD=X": "GBP/AUD",
    "AUDJPY=X": "AUD/JPY",
    "CADJPY=X": "CAD/JPY",
    "CHFJPY=X": "CHF/JPY",
    "EURNZD=X": "EUR/NZD",
    "GBPNZD=X": "GBP/NZD",
    "AUDNZD=X": "AUD/NZD",
    "AUDCAD=X": "AUD/CAD",
    "CADCHF=X": "CAD/CHF",
    "NZDJPY=X": "NZD/JPY",
    
    # TRY Çiftleri
    "USDTRY=X": "USD/TRY",
    "EURTRY=X": "EUR/TRY",
    "GBPTRY=X": "GBP/TRY",
    "CHFTRY=X": "CHF/TRY",
    "JPYTRY=X": "JPY/TRY",
    "TRYUSD=X": "TRY/USD",
    "TRYEUR=X": "TRY/EUR",
    "TRYGBP=X": "TRY/GBP",
    
    # Emtia Paraları
    "USDRUB=X": "USD/RUB",
    "USDBRL=X": "USD/BRL",
    "USDMXN=X": "USD/MXN",
    "USDZAR=X": "USD/ZAR",
    "USDCNY=X": "USD/CNY",
    "USDINR=X": "USD/INR",
    "USDKRW=X": "USD/KRW",
    "USDSGD=X": "USD/SGD",
    "USDHKD=X": "USD/HKD",
    "USDTHB=X": "USD/THB",
    "USDPHP=X": "USD/PHP",
    "USDIDR=X": "USD/IDR",
    "USDMYR=X": "USD/MYR",
    
    # Orta Doğu ve Afrika
    "USDSAR=X": "USD/SAR",
    "USDAED=X": "USD/AED",
    "USDKWD=X": "USD/KWD",
    "USDQAR=X": "USD/QAR",
    "USDEGP=X": "USD/EGP",
    "USDNGN=X": "USD/NGN",
    
    # Avrupa
    "USDPLN=X": "USD/PLN",
    "USDHUF=X": "USD/HUF",
    "USDCZK=X": "USD/CZK",
    "USDSEK=X": "USD/SEK",
    "USDNOK=X": "USD/NOK",
    "USDDKK=X": "USD/DKK",
    "USDILS=X": "USD/ILS",
    
    # Altın ile Çiftler
    "XAUUSD=X": "Altın/USD",
    "XAUEUR=X": "Altın/EUR",
    "XAUJPY=X": "Altın/JPY",
    "XAUGBP=X": "Altın/GBP",
    "XAUAUD=X": "Altın/AUD",
    
    # Gümüş ile Çiftler
    "XAGUSD=X": "Gümüş/USD",
    "XAGEUR=X": "Gümüş/EUR"
}

# Kripto Para Birimleri - Popüler Kripto Paralar
CRYPTO_CURRENCIES = {
    # Ana Kripto Paralar
    "BTC-USD": "Bitcoin (BTC)",
    "ETH-USD": "Ethereum (ETH)",
    "BNB-USD": "Binance Coin (BNB)",
    "XRP-USD": "Ripple (XRP)",
    "ADA-USD": "Cardano (ADA)",
    "SOL-USD": "Solana (SOL)",
    "DOT-USD": "Polkadot (DOT)",
    "DOGE-USD": "Dogecoin (DOGE)",
    "AVAX-USD": "Avalanche (AVAX)",
    "MATIC-USD": "Polygon (MATIC)",
    
    # Diğer Popüler Kripto Paralar
    "LINK-USD": "Chainlink (LINK)",
    "LTC-USD": "Litecoin (LTC)",
    "BCH-USD": "Bitcoin Cash (BCH)",
    "UNI-USD": "Uniswap (UNI)",
    "ATOM-USD": "Cosmos (ATOM)",
    "ALGO-USD": "Algorand (ALGO)",
    "VET-USD": "VeChain (VET)",
    "ICP-USD": "Internet Computer (ICP)",
    "FIL-USD": "Filecoin (FIL)",
    "TRX-USD": "Tron (TRX)",
    "ETC-USD": "Ethereum Classic (ETC)",
    "XLM-USD": "Stellar (XLM)",
    "HBAR-USD": "Hedera (HBAR)",
    "SAND-USD": "The Sandbox (SAND)",
    "MANA-USD": "Decentraland (MANA)",
    "CRO-USD": "Cronos (CRO)",
    "SHIB-USD": "Shiba Inu (SHIB)",
    "NEAR-USD": "NEAR Protocol (NEAR)",
    "FTM-USD": "Fantom (FTM)",
    "AAVE-USD": "Aave (AAVE)",
    "GRT-USD": "The Graph (GRT)",
    "ENJ-USD": "Enjin Coin (ENJ)",
    "COMP-USD": "Compound (COMP)",
    "MKR-USD": "Maker (MKR)",
    "SNX-USD": "Synthetix (SNX)",
    "SUSHI-USD": "SushiSwap (SUSHI)",
    "YFI-USD": "yearn.finance (YFI)",
    "1INCH-USD": "1inch Network (1INCH)",
    "CRV-USD": "Curve DAO Token (CRV)",
    "BAT-USD": "Basic Attention Token (BAT)",
    "ZEC-USD": "Zcash (ZEC)",
    "DASH-USD": "Dash (DASH)",
    "XMR-USD": "Monero (XMR)",
    
    # Stable Coin'ler
    "USDT-USD": "Tether (USDT)",
    "USDC-USD": "USD Coin (USDC)",
    "BUSD-USD": "Binance USD (BUSD)",
    "DAI-USD": "Dai (DAI)",
    "TUSD-USD": "TrueUSD (TUSD)",
    "USDP-USD": "Pax Dollar (USDP)",
    
    # Kripto TRY Çiftleri
    "BTC-TRY": "Bitcoin/TRY",
    "ETH-TRY": "Ethereum/TRY",
    "XRP-TRY": "Ripple/TRY",
    "ADA-TRY": "Cardano/TRY",
    "DOGE-TRY": "Dogecoin/TRY"
}

# TEFAS Yatırım Fonları - Popüler Fonlar
@st.cache_data(ttl=300)  # 5 dakikalık önbellek (daha sık güncelleme)
def fetch_all_tefas_funds():
    """Azure Blob Storage'dan tüm TEFAS fonlarını döndür (tefas_funds.parquet ve tefas_data.parquet)"""
    try:
        tefas_funds = {}
        blob_storage = AzureBlobStorage()
        
        # 1. Önce tefas_funds.parquet dosyasından fon listesini al
        try:
            funds_content = blob_storage.download_file(TEFAS_FUNDS_FILE)
            if funds_content:
                funds_buffer = io.BytesIO(funds_content)
                funds_df = pd.read_parquet(funds_buffer)
                
                # Fon Kodu ve Fon Adı sütunlarını kontrol et
                if 'Fon Kodu' in funds_df.columns and 'Fon Adı' in funds_df.columns:
                    for _, row in funds_df.iterrows():
                        fund_code = str(row['Fon Kodu']).strip()
                        fund_name = str(row['Fon Adı']).strip()
                        tefas_funds[fund_code] = fund_name
                    print(f"✓ {len(tefas_funds)} fon tefas_funds.parquet'ten yüklendi")
        except Exception as e:
            print(f"tefas_funds.parquet okunamadı: {str(e)}")
        
        # 2. Eğer tefas_funds.parquet'te veri yoksa, tefas_data.parquet'ten al
        if not tefas_funds:
            try:
                data_content = blob_storage.download_file(TEFAS_DATA_FILE)
                if data_content:
                    data_buffer = io.BytesIO(data_content)
                    df = pd.read_parquet(data_buffer)
                    
                    # Sütun isimlerini kontrol et (hem eski hem yeni format)
                    fund_code_col = None
                    fund_name_col = None
                    
                    if 'Fon_Kodu' in df.columns:
                        fund_code_col = 'Fon_Kodu'
                        fund_name_col = 'Fon_Adi' if 'Fon_Adi' in df.columns else 'Fon_Adı'
                    elif 'Fon Kodu' in df.columns:
                        fund_code_col = 'Fon Kodu'
                        fund_name_col = 'Fon Adı'
                    
                    if fund_code_col and fund_name_col:
                        funds_unique = df[[fund_code_col, fund_name_col]].drop_duplicates(
                            subset=[fund_code_col], keep='last'
                        )
                        
                        for _, row in funds_unique.iterrows():
                            fund_code = str(row[fund_code_col]).strip()
                            fund_name = str(row[fund_name_col]).strip()
                            tefas_funds[fund_code] = fund_name
                        
                        print(f"✓ {len(tefas_funds)} fon tefas_data.parquet'ten yüklendi")
            except Exception as e:
                print(f"tefas_data.parquet okunamadı: {str(e)}")
        
        # 3. Popüler TEFAS fonları (fallback ve ek fonlar)
        popular_tefas_funds = {
            # Hisse Senedi Fonları
            "HPD": "Halk Portföy Değişken Fon",
            "HHY": "Halk Portföy Hisse Yoğun Değişken Fon",
            "GPD": "Gedik Portföy Değişken Fon",
            "ZPD": "Ziraat Portföy Değişken Fon",
            "IPD": "İş Portföy Değişken Fon",
            "APD": "Ak Portföy Değişken Fon",
            "YPD": "Yapı Kredi Portföy Değişken Fon",
            "TPD": "TSKB Portföy Değişken Fon",
            "OPD": "ODEABANK Portföy Değişken Fon",
            
            # Hisse Senedi (Agresif) Fonları
            "AHS": "Ak Portföy Hisse Senedi Fonu",
            "IHS": "İş Portföy Hisse Senedi Fonu",
            "GHS": "Gedik Portföy Hisse Senedi Fonu",
            "YHS": "Yapı Kredi Portföy Hisse Senedi Fonu",
            "HHS": "Halk Portföy Hisse Senedi Fonu",
            "ZHS": "Ziraat Portföy Hisse Senedi Fonu",
            
            # Borçlanma Araçları Fonları
            "ABF": "Ak Portföy Borçlanma Araçları Fonu",
            "IBF": "İş Portföy Borçlanma Araçları Fonu",
            "HBF": "Halk Portföy Borçlanma Araçları Fonu",
            "ZBF": "Ziraat Portföy Borçlanma Araçları Fonu",
            "YBF": "Yapı Kredi Portföy Borçlanma Araçları Fonu",
            "GBF": "Gedik Portföy Borçlanma Araçları Fonu",
            
            # Para Piyasası Fonları
            "APP": "Ak Portföy Para Piyasası Fonu",
            "IPP": "İş Portföy Para Piyasası Fonu",
            "HPP": "Halk Portföy Para Piyasası Fonu",
            "ZPP": "Ziraat Portföy Para Piyasası Fonu",
            "YPP": "Yapı Kredi Portföy Para Piyasası Fonu",
            "GPP": "Gedik Portföy Para Piyasası Fonu",
            
            # Altın Fonları
            "AAL": "Ak Portföy Altın Fonu",
            "IAL": "İş Portföy Altın Fonu",
            "HAL": "Halk Portföy Altın Fonu",
            "ZAL": "Ziraat Portföy Altın Fonu",
            "YAL": "Yapı Kredi Portföy Altın Fonu",
            "GAL": "Gedik Portföy Altın Fonu",
            
            # PPK (Bireysel Emeklilik) Fonları
            "PPK": "PPK Fonu",
            "PPKA": "PPK A Fonu",
            "PPKB": "PPK B Fonu", 
            "PPKC": "PPK C Fonu"
        }
        
        # Popüler fonları ana listeye ekle (Azure'deki fonları geçersiz kılmaz)
        for code, name in popular_tefas_funds.items():
            if code not in tefas_funds:
                tefas_funds[code] = name
        
        # Eğer hiç fon yoksa sadece popüler fonları döndür
        if not tefas_funds:
            tefas_funds = popular_tefas_funds
            print("⚠ Azure'da TEFAS verisi bulunamadı, popüler fonlar kullanılıyor")
        
        return tefas_funds
        
    except Exception as e:
        print(f"❌ TEFAS fonları yüklenirken hata: {str(e)}")
        # Hata durumunda minimal fon listesi döndür
        return {
            "HPD": "Halk Portföy Değişken Fon",
            "APD": "Ak Portföy Değişken Fon",
            "IPD": "İş Portföy Değişken Fon"
        }

# TEFAS fonları listesi - İlk başta yüklenmez, talep edildiğinde yüklenir
# Boş bir dict ile başla, kategorilerde TEFAS seçildiğinde fetch_all_tefas_funds() çağrılacak
TEFAS_FUNDS = {}

def get_tefas_funds_dynamic():
    """TEFAS fonlarını sadece gerektiğinde yükle - lazy loading"""
    global TEFAS_FUNDS
    
    # Eğer daha önce yüklenmemişse veya boşsa yükle
    if not TEFAS_FUNDS or len(TEFAS_FUNDS) == 0:
        try:
            TEFAS_FUNDS = fetch_all_tefas_funds()

            # Basit kontrol: PPK içeren kodları tespit et (UI bildirimleri kaldırıldı)
            ppk_funds = [code for code in TEFAS_FUNDS.keys() if "PPK" in code.upper()]
            # (İleride gerekiyorsa bu bilgi UI tarafında gösterülebilir)

        except Exception as e:
            # Hata durumunda minimal liste döndür
            TEFAS_FUNDS = {
                "HPD": "TEFAS HPD Fonu",
                "GPD": "TEFAS GPD Fonu", 
                "ZPD": "TEFAS ZPD Fonu",
                "IPD": "TEFAS IPD Fonu",
                "APD": "TEFAS APD Fonu"
            }
            # Hata durumunda minimal liste döndür
            TEFAS_FUNDS = {
                "HPD": "TEFAS HPD Fonu",
                "GPD": "TEFAS GPD Fonu", 
                "ZPD": "TEFAS ZPD Fonu",
                "IPD": "TEFAS IPD Fonu",
                "APD": "TEFAS APD Fonu"
            }
    
    return TEFAS_FUNDS

# Nakit Para Birimleri
CASH_CURRENCIES = {
    "CASH_TRY": "Türk Lirası (₺)",
    "CASH_USD": "Amerikan Doları ($)",
    "CASH_EUR": "Euro (€)",
    "CASH_GBP": "İngiliz Sterlini (£)",
    "CASH_JPY": "Japon Yeni (¥)",
    "CASH_CHF": "İsviçre Frangı (CHF)",
    "CASH_CAD": "Kanada Doları (CAD)",
    "CASH_AUD": "Avustralya Doları (AUD)",
    "CASH_SEK": "İsveç Kronu (SEK)",
    "CASH_NOK": "Norveç Kronu (NOK)",
    "CASH_DKK": "Danimarka Kronu (DKK)",
    "CASH_PLN": "Polonya Zlotu (PLN)",
    "CASH_CZK": "Çek Korunası (CZK)",
    "CASH_HUF": "Macar Forinti (HUF)",
    "CASH_RUB": "Rus Rublesi (RUB)",
    "CASH_CNY": "Çin Yuanı (CNY)",
    "CASH_KRW": "Güney Kore Wonu (KRW)",
    "CASH_SGD": "Singapur Doları (SGD)",
    "CASH_HKD": "Hong Kong Doları (HKD)",
    "CASH_INR": "Hindistan Rupisi (INR)",
    "CASH_BRL": "Brezilya Reali (BRL)",
    "CASH_MXN": "Meksika Pesosu (MXN)",
    "CASH_ZAR": "Güney Afrika Randı (ZAR)",
    "CASH_SAR": "Suudi Arabistan Riyali (SAR)",
    "CASH_AED": "BAE Dirhemi (AED)"
}

# Enstrüman Kategorileri
def get_instrument_categories():
    """Enstrüman kategorilerini dinamik olarak döndür - BIST ve NASDAQ listesi güncel tutulur"""
    # BIST listesini güncel tut
    current_bist_stocks = get_bist_stocks_dynamic()
    
    # NASDAQ listesini güncel tut
    current_nasdaq_stocks = get_nasdaq_stocks_smart()
    
    # TEFAS fonlarını sadece daha önce yüklenmişse kullan (lazy loading)
    tefas_snapshot = TEFAS_FUNDS if TEFAS_FUNDS else {}
    
    return {
        "BIST": {"name": "BIST Hisse Senetleri", "data": current_bist_stocks, "suffix": ".IS", "currency": "₺"},
        "NASDAQ": {"name": "NASDAQ Hisse Senetleri", "data": current_nasdaq_stocks, "suffix": "", "currency": "$"},
        "METALS": {"name": "Kıymetli Madenler", "data": PRECIOUS_METALS, "suffix": "", "currency": "$"},
        "FOREX": {"name": "Döviz Kurları", "data": FOREX_PAIRS, "suffix": "", "currency": ""},
        "CRYPTO": {"name": "Kripto Para Birimleri", "data": CRYPTO_CURRENCIES, "suffix": "", "currency": "$"},
        "TEFAS": {"name": "TEFAS Yatırım Fonları", "data": tefas_snapshot, "suffix": "", "currency": "₺", "lazy_loader": get_tefas_funds_dynamic},
        "CASH": {"name": "Nakit Para Birimleri", "data": CASH_CURRENCIES, "suffix": "", "currency": "Çeşitli"}
    }

def get_portfolio_allowed_categories():
    """Portföy için izin verilen kategorileri dinamik olarak döndür"""
    # BIST listesini güncel tut
    current_bist_stocks = get_bist_stocks_dynamic()
    
    # NASDAQ listesini güncel tut
    current_nasdaq_stocks = get_nasdaq_stocks_smart()
    
    # TEFAS fonlarını sadece ihtiyaç halinde yükle (lazy loading)
    tefas_snapshot = TEFAS_FUNDS if TEFAS_FUNDS else {}
    
    return {
        "METALS": {"name": "Kıymetli Madenler", "data": PRECIOUS_METALS, "suffix": "", "currency": "Çeşitli"},
        "BIST": {"name": "BIST Hisse Senetleri", "data": current_bist_stocks, "suffix": ".IS", "currency": "₺"},
        "NASDAQ": {"name": "NASDAQ Hisse Senetleri", "data": current_nasdaq_stocks, "suffix": "", "currency": "$"},
        "CRYPTO": {"name": "Kripto Para Birimleri", "data": CRYPTO_CURRENCIES, "suffix": "", "currency": "$"},
        "TEFAS": {"name": "TEFAS Yatırım Fonları", "data": tefas_snapshot, "suffix": "", "currency": "₺", "lazy_loader": get_tefas_funds_dynamic},
        "CASH": {"name": "Nakit Para Birimleri", "data": CASH_CURRENCIES, "suffix": "", "currency": "Çeşitli"}
    }

# Geriye uyumluluk için statik sürümler kaldırıldı - artık dinamik fonksiyonlar kullanılıyor
# INSTRUMENT_CATEGORIES ve PORTFOLIO_ALLOWED_CATEGORIES değişkenleri 
# get_instrument_categories() ve get_portfolio_allowed_categories() fonksiyonları ile değiştirildi

def get_turkish_gold_prices():
    """Türk altın piyasası fiyatlarını önce blob storage'dan, sonra API'den al"""
    
    # Önce blob storage'dan güncel fiyatları kontrol et
    blob_prices = turkish_gold_dm.get_prices()
    
    # Veriler güncel ise blob'dan döndür
    if blob_prices and turkish_gold_dm.is_data_fresh(max_age_hours=1):  # 1 saat güncel kabul et
        
        # Blob formatını API formatına çevir
        formatted_prices = {}
        for instrument, data in blob_prices.items():
            formatted_prices[instrument] = {
                'price': data.get('price', 0),
                'name': data.get('name', instrument)
            }
        return formatted_prices
    
    # Veriler eski ise API'den çek
    
    turkish_gold_data = {}
    
    # Sadece finans.truncgil.com API'sini kullan (en güncel ve güvenilir kaynak)
    try:
        api_url = "https://finans.truncgil.com/today.json"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'tr-TR,tr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        }
        
        response = requests.get(api_url, headers=headers, timeout=20)
        
        if response.status_code == 200:
                data = response.json()
                altın_keys = [key for key in data.keys() if 'altin' in key.lower()]

                # Fiyat formatını düzenle (virgülleri kaldır ve float'a çevir)
                def parse_price(price_str):
                    if isinstance(price_str, (int, float)):
                        return float(price_str)
                    elif isinstance(price_str, str):
                        # Türkiye formatı: 1.234,56 -> 1234.56
                        clean_price = price_str.replace('.', '').replace(',', '.')
                        return float(clean_price)
                    return 0.0

                # Gram altın
                if 'gram-altin' in data:
                    gram_data = data['gram-altin']
                    alış = parse_price(gram_data.get('Alış', gram_data.get('alis', '0')))
                    satış = parse_price(gram_data.get('Satış', gram_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_GRAM"] = {
                            "name": "Altın (Gram/TRY)",
                        "price": current_price,
                        "currency": "₺",
                        "buy": alış,
                        "sell": satış,
                        "source": "finans.truncgil.com"
                    }
            
            # Çeyrek altın
                if 'ceyrek-altin' in data:
                    ceyrek_data = data['ceyrek-altin']
                    alış = parse_price(ceyrek_data.get('Alış', ceyrek_data.get('alis', '0')))
                    satış = parse_price(ceyrek_data.get('Satış', ceyrek_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_CEYREK"] = {
                            "name": "Çeyrek Altın (TRY)",
                            "price": current_price,
                            "currency": "₺",
                        "buy": alış,
                        "sell": satış,
                        "source": "finans.truncgil.com"
                    }
            
            # Yarım altın
                if 'yarim-altin' in data:
                    yarim_data = data['yarim-altin']
                    alış = parse_price(yarim_data.get('Alış', yarim_data.get('alis', '0')))
                    satış = parse_price(yarim_data.get('Satış', yarim_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_YARIM"] = {
                            "name": "Yarım Altın (TRY)",
                            "price": current_price,
                            "currency": "₺",
                            "buy": alış,
                            "sell": satış,
                        "source": "finans.truncgil.com"
                    }
            
            # Tam altın
                if 'tam-altin' in data:
                    tam_data = data['tam-altin']
                    alış = parse_price(tam_data.get('Alış', tam_data.get('alis', '0')))
                    satış = parse_price(tam_data.get('Satış', tam_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_TAM"] = {
                            "name": "Tam Altın (TRY)",
                            "price": current_price,
                            "currency": "₺",
                            "buy": alış,
                            "sell": satış,
                            "source": "finans.truncgil.com"
                        }
            
                # Ons fiyatı hesapla (gram fiyatından)
                if "ALTIN_GRAM" in turkish_gold_data:
                    gram_price = turkish_gold_data["ALTIN_GRAM"]["price"]
                    ons_price = gram_price * 31.1035
                    turkish_gold_data["ALTIN_ONS_TRY"] = {
                        "name": "Altın (Ons/TRY)",
                        "price": ons_price,
                        "currency": "₺",
                        "source": "finans.truncgil.com"
                    }

                # Reşat Altını ve Cumhuriyet Altını kontrol et
                if 'resat-altin' in data:
                    resat_data = data['resat-altin']
                    alış = parse_price(resat_data.get('Alış', resat_data.get('alis', '0')))
                    satış = parse_price(resat_data.get('Satış', resat_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_RESAT"] = {
                            "name": "Reşat Altını (TRY)",
                            "price": current_price,
                            "currency": "₺",
                            "buy": alış,
                            "sell": satış,
                            "source": "finans.truncgil.com"
                        }

                if 'cumhuriyet-altini' in data:  # API'de cumhuriyet-altini anahtarı kullanılıyor
                    cumhuriyet_data = data['cumhuriyet-altini']
                    alış = parse_price(cumhuriyet_data.get('Alış', cumhuriyet_data.get('alis', '0')))
                    satış = parse_price(cumhuriyet_data.get('Satış', cumhuriyet_data.get('satis', '0')))
                    # ALIŞ fiyatını kullan (yatırımcının alacağı fiyat)
                    current_price = alış if alış > 0 else satış

                    if current_price > 0:
                        turkish_gold_data["ALTIN_CUMHURIYET"] = {
                            "name": "Cumhuriyet Altını (TRY)",
                        "price": current_price,
                        "currency": "₺",
                        "buy": alış,
                        "sell": satış,
                        "source": "finans.truncgil.com"
                    }
            
            # Ek altın türleri
                if 'ata-altin' in data:
                    ata_data = data['ata-altin']
                    alış = parse_price(ata_data.get('Alış', ata_data.get('alis', '0')))
                    satış = parse_price(ata_data.get('Satış', ata_data.get('satis', '0')))
                    current_price = alış if alış > 0 else satış
                    if current_price > 0:
                        turkish_gold_data["ALTIN_ATA"] = {
                            "name": "Ata Altını (TRY)", "price": current_price, "currency": "₺",
                            "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                        }

                if 'hamit-altin' in data:
                    hamit_data = data['hamit-altin']
                    alış = parse_price(hamit_data.get('Alış', hamit_data.get('alis', '0')))
                    satış = parse_price(hamit_data.get('Satış', hamit_data.get('satis', '0')))
                    current_price = alış if alış > 0 else satış
                    if current_price > 0:
                        turkish_gold_data["ALTIN_HAMIT"] = {
                            "name": "Hamit Altını (TRY)", "price": current_price, "currency": "₺",
                            "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                        }


        if 'besli-altin' in data:
            besli_data = data['besli-altin']
            alış = parse_price(besli_data.get('Alış', besli_data.get('alis', '0')))
            satış = parse_price(besli_data.get('Satış', besli_data.get('satis', '0')))
            current_price = alış if alış > 0 else satış
            if current_price > 0:
                turkish_gold_data["ALTIN_BESLI"] = {
                    "name": "Beşli Altın (TRY)", "price": current_price, "currency": "₺",
                    "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                    }

            if '14-ayar-altin' in data:
                ayar14_data = data['14-ayar-altin']
                alış = parse_price(ayar14_data.get('Alış', ayar14_data.get('alis', '0')))
                satış = parse_price(ayar14_data.get('Satış', ayar14_data.get('satis', '0')))
                current_price = alış if alış > 0 else satış
                if current_price > 0:
                    turkish_gold_data["ALTIN_14AYAR"] = {
                        "name": "14 Ayar Altın (TRY)", "price": current_price, "currency": "₺",
                        "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                    }
            
            if '18-ayar-altin' in data:
                ayar18_data = data['18-ayar-altin']
                alış = parse_price(ayar18_data.get('Alış', ayar18_data.get('alis', '0')))
                satış = parse_price(ayar18_data.get('Satış', ayar18_data.get('satis', '0')))
                current_price = alış if alış > 0 else satış
                if current_price > 0:
                    turkish_gold_data["ALTIN_18AYAR"] = {
                        "name": "18 Ayar Altın (TRY)", "price": current_price, "currency": "₺",
                        "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                    }
            
            if '22-ayar-bilezik' in data:
                bilezik_data = data['22-ayar-bilezik']
                alış = parse_price(bilezik_data.get('Alış', bilezik_data.get('alis', '0')))
                satış = parse_price(bilezik_data.get('Satış', bilezik_data.get('satis', '0')))
                current_price = alış if alış > 0 else satış
                if current_price > 0:
                    turkish_gold_data["ALTIN_22AYAR_BILEZIK"] = {
                        "name": "22 Ayar Bilezik (TRY)", "price": current_price, "currency": "₺",
                        "buy": alış, "sell": satış, "source": "finans.truncgil.com"
                    }
            
            if turkish_gold_data:
                # Başarılı API çağrısında blob storage'a günlük Parquet formatında kaydet
                try:
                    # Günlük Parquet formatında kaydet
                    turkish_gold_dm.save_daily_prices(turkish_gold_data)
                except Exception as e:
                    pass  # Parquet kaydetme hatası

                gram_price = turkish_gold_data.get('ALTIN_GRAM', {}).get('price', 0)
                return turkish_gold_data
            else:
                pass  # Veri alındı ama altın fiyatları bulunamadı

        else:
            pass  # HTTP hatası

    except requests.exceptions.Timeout:
        pass  # Zaman aşımı
    except requests.exceptions.ConnectionError:
        pass  # Bağlantı hatası
    except requests.exceptions.RequestException as e:
        pass  # İstek hatası
    except json.JSONDecodeError as e:
        pass  # JSON parse hatası
    except Exception as e:
        pass  # Beklenmeyen hata

    # Veri alınamazsa boş dict döndür
    return {}

def get_universal_data(instrument_category, selected_instruments, start_date=None, end_date=None):
    """Tüm finansal enstrümanlar için genel veri çekme fonksiyonu"""
    try:
        # Dinamik kategorileri al
        current_categories = get_instrument_categories()
        category_info = current_categories[instrument_category]
        if instrument_category == "TEFAS":
            instruments_data = get_tefas_funds_dynamic()
            category_info["data"] = instruments_data
        else:
            instruments_data = category_info["data"]
        suffix = category_info["suffix"]
        currency = category_info["currency"]
        
        df_list = []
        successful_count = 0
        
        # Türk altın fiyatlarını al (kıymetli madenler kategorisi için)
        turkish_gold_data = {}
        if instrument_category == "METALS":
            turkish_gold_data = get_turkish_gold_prices()
        
        # TEFAS fonları için özel hazırlık
        tefas_warning_shown = False
        if instrument_category == "TEFAS" and not tefas_warning_shown:
            st.info("📊 TEFAS fonları için veri çekiliyor. Bu işlem biraz zaman alabilir...")
            tefas_warning_shown = True
        
        # Tarih aralığı ayarla - tarihe göre farklı davranış
        if start_date is None or end_date is None:
            period = "1d"
            use_period = True
            is_summary_view = True  # Özet görünüm için
        else:
            # Tarih aralığı verildiyse her zaman detaylı görünüm kullan
            use_period = False
            is_summary_view = False  # Detaylı tarih aralığı - tüm tarihleri getir
        
        for i, instrument in enumerate(selected_instruments):
            try:
                # Türk altın fiyatları için özel işlem
                if instrument in turkish_gold_data:
                    gold_info = turkish_gold_data[instrument]
                    
                    if is_summary_view:
                        # Özet görünüm için sadece son fiyat
                        row = {
                            'Kod': instrument,
                            'Adı': gold_info["name"],
                            'Son Fiyat': float(gold_info["price"]),
                            'Değişim': 0.0,  # Gerçek zamanlı değişim hesaplaması için geliştirilebilir
                            'Değişim %': 0.0,
                            'En Yüksek': float(gold_info["price"]),
                            'En Düşük': float(gold_info["price"]),
                            'Açılış': float(gold_info["price"]),
                            'Hacim': 0,
                            'Para Birimi': gold_info["currency"],
                            'Kategori': category_info["name"],
                            'Güncelleme Zamanı': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        df_list.append(row)
                        successful_count += 1
                    else:
                        # Detaylı görünüm için tarihsel hesaplama
                        try:
                            # USD/TRY kurunu tarihsel olarak al (session ile)
                            usdtry = yf.Ticker("USDTRY=X", session=YF_SESSION)
                            usdtry_hist = usdtry.history(start=start_date, end=end_date)
                            
                            # Altın fiyatını USD'den tarihsel olarak al (session ile)
                            gold_usd = yf.Ticker("GC=F", session=YF_SESSION)
                            gold_hist = gold_usd.history(start=start_date, end=end_date)
                            
                            if not usdtry_hist.empty and not gold_hist.empty:
                                # Tarihleri eşleştir - timezone aware dates kullan
                                try:
                                    # Timezone bilgisi varsa kullan
                                    tz = usdtry_hist.index.tz if usdtry_hist.index.tz else None
                                    all_dates = pd.date_range(start=start_date, end=end_date, tz=tz)
                                except:
                                    # Timezone sorunu varsa timezone olmadan dene
                                    all_dates = pd.date_range(start=start_date, end=end_date)
                                    usdtry_hist.index = usdtry_hist.index.tz_localize(None)
                                    gold_hist.index = gold_hist.index.tz_localize(None)
                                
                                usdtry_hist = usdtry_hist.reindex(all_dates, method='ffill')
                                gold_hist = gold_hist.reindex(all_dates, method='ffill')
                                
                                for date in all_dates:
                                    usd_try_rate = usdtry_hist.loc[date, 'Close']
                                    gold_usd_price = gold_hist.loc[date, 'Close']
                                    
                                    # Ons başına TRY fiyatı
                                    gold_try_ons = gold_usd_price * usd_try_rate
                                    
                                    # Gram başına TRY fiyatı (1 ons = 31.1035 gram)
                                    gold_try_gram = gold_try_ons / 31.1035
                                    
                                    if instrument == "ALTIN_GRAM":
                                        calculated_price = gold_try_gram
                                    elif instrument == "ALTIN_CEYREK":
                                        calculated_price = gold_try_gram * 1.75  # Çeyrek altın (1.75 gram)
                                    elif instrument == "ALTIN_TAM":
                                        calculated_price = gold_try_gram * 7.216  # Tam altın (7.216 gram)
                                    elif instrument == "ALTIN_ONS_TRY":
                                        calculated_price = gold_try_ons
                                    
                                    data_row = {
                                        'Kod': instrument,
                                        'Adı': gold_info["name"],
                                        'Tarih': date.strftime('%Y-%m-%d'),
                                        'Açılış': float(calculated_price),
                                        'En Yüksek': float(calculated_price),
                                        'En Düşük': float(calculated_price),
                                        'Kapanış': float(calculated_price),
                                        'Hacim': 0,
                                        'Kategori': category_info["name"],
                                        'Para Birimi': "₺"
                                    }
                                    df_list.append(data_row)
                                
                            else:
                                st.warning(f"⚠️ {instrument} için hesaplama verisi bulunamadı")
                        except Exception as e:
                            st.warning(f"⚠️ {instrument} hesaplanırken hata: {str(e)}")
                            continue
                    
                else:
                    # TEFAS fonları için özel işlem
                    if instrument_category == "TEFAS":
                        try:
                            # TEFAS API'sinden fon verisi al
                            from datetime import datetime, timedelta
                            
                            # Tarih aralığını hazırla
                            if is_summary_view:
                                # Özet görünüm için son 30 gün
                                end_date_str = datetime.now().strftime('%d-%m-%Y')
                                start_date_str = (datetime.now() - timedelta(days=30)).strftime('%d-%m-%Y')
                            else:
                                # Detaylı görünüm için belirtilen tarih aralığı
                                start_date_str = start_date.strftime('%d-%m-%Y')
                                end_date_str = end_date.strftime('%d-%m-%Y')
                            
                            fund_data = None
                            fund_price = None
                            
                            # Farklı fon tiplerini dene
                            for fund_type in [1, 2, 3, 4, 5]:
                                try:
                                    fund_data = fetch_tefas_data(
                                        fund_type_code=fund_type,
                                        tab_code=2,  # Fiyat bilgisi
                                        start_date=start_date_str,
                                        end_date=end_date_str
                                    )
                                    
                                    if fund_data is not None and not fund_data.empty:
                                        fund_row = fund_data[fund_data['FONKODU'] == instrument]
                                        if not fund_row.empty:
                                            fund_price = round(float(fund_row['BIRIMPAYDEGERI'].iloc[-1]), 6)
                                            break
                                except Exception as e:
                                    continue
                            
                            if fund_price is not None:
                                fund_name = current_instruments.get(instrument, instrument)
                                
                                if is_summary_view:
                                    # Özet görünüm için
                                    row = {
                                        'Kod': instrument,
                                        'Adı': fund_name,
                                        'Son Fiyat': fund_price,
                                        'Değişim': 0.0,  # TEFAS değişim bilgisi ek işlem gerektirir
                                        'Değişim %': 0.0,
                                        'En Yüksek': fund_price,
                                        'En Düşük': fund_price,
                                        'Açılış': fund_price,
                                        'Hacim': 0,
                                        'Para Birimi': "₺",
                                        'Kategori': category_info["name"],
                                        'Güncelleme Zamanı': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    }
                                    df_list.append(row)
                                    successful_count += 1
                                
                                else:
                                    # Detaylı görünüm için - her gün için aynı fiyatı kullan
                                    current_date = start_date
                                    while current_date <= end_date:
                                        data_row = {
                                            'Kod': instrument,
                                            'Adı': fund_name,
                                            'Tarih': current_date.strftime('%Y-%m-%d'),
                                            'Açılış': fund_price,
                                            'En Yüksek': fund_price,
                                            'En Düşük': fund_price,
                                            'Kapanış': fund_price,
                                            'Hacim': 0,
                                            'Kategori': category_info["name"],
                                            'Para Birimi': "₺"
                                        }
                                        df_list.append(data_row)
                                        current_date += timedelta(days=1)
                                    
                                    successful_count += 1
                            else:
                                st.warning(f"⚠️ {instrument} TEFAS fonu için veri bulunamadı")
                        
                        except Exception as e:
                            st.warning(f"⚠️ {instrument} TEFAS fonu işlenirken hata: {str(e)}")
                            continue
                    
                    else:
                        # Normal Yahoo Finance işlemi (session ile)
                        ticker_symbol = f"{instrument}{suffix}"
                        ticker = yf.Ticker(ticker_symbol, session=YF_SESSION)
                        info = ticker.info
                        
                        # Tarih aralığına göre veri çek
                        if use_period:
                            hist = ticker.history(period=period)
                        else:
                            hist = ticker.history(start=start_date, end=end_date)
                        
                        if not hist.empty:
                            if is_summary_view:
                                # Özet görünüm için sadece son fiyatları al
                                last_price = hist['Close'].iloc[-1]
                                open_price = hist['Open'].iloc[-1]
                                high_price = hist['High'].iloc[-1]
                                low_price = hist['Low'].iloc[-1]
                                volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns else 0
                                
                                change = last_price - open_price
                                change_percent = (change / open_price) * 100 if open_price > 0 else 0
                                
                                # Para birimi formatlaması
                                if instrument_category == "FOREX":
                                    currency_symbol = ""
                                    price_format = "{:.4f}"
                                elif instrument_category == "METALS":
                                    currency_symbol = "$" if not instrument.endswith(".IS") else "₺"
                                    price_format = "{:.2f}"
                                elif instrument_category == "BIST":
                                    currency_symbol = "₺"
                                    price_format = "{:.2f}"
                                else:  # NASDAQ
                                    currency_symbol = "$"
                                    price_format = "{:.2f}"
                                
                                row = {
                                    'Kod': instrument,
                                    'Adı': instruments_data.get(instrument, info.get('longName', instrument)),
                                    'Son Fiyat': float(last_price),
                                    'Değişim': float(change),
                                    'Değişim %': float(change_percent),
                                    'En Yüksek': float(high_price),
                                    'En Düşük': float(low_price),
                                    'Açılış': float(open_price),
                                    'Hacim': float(volume),
                                    'Para Birimi': currency_symbol,
                                    'Kategori': category_info["name"],
                                    'Güncelleme Zamanı': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                df_list.append(row)
                                successful_count += 1
                            else:
                                # Detaylı görünüm için tüm tarih aralığındaki verileri al
                                for date, row in hist.iterrows():
                                    # Para birimi formatlaması
                                    if instrument_category == "FOREX":
                                        currency_symbol = ""
                                    elif instrument_category == "METALS":
                                        currency_symbol = "$" if not instrument.endswith(".IS") else "₺"
                                    elif instrument_category == "BIST":
                                        currency_symbol = "₺"
                                    else:  # NASDAQ
                                        currency_symbol = "$"
                                    
                                    data_row = {
                                        'Kod': instrument,
                                        'Adı': instruments_data.get(instrument, info.get('longName', instrument)),
                                        'Tarih': date.strftime('%Y-%m-%d'),
                                        'Açılış': float(row['Open']),
                                        'En Yüksek': float(row['High']),
                                        'En Düşük': float(row['Low']),
                                        'Kapanış': float(row['Close']),
                                        'Hacim': float(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) else 0,
                                        'Kategori': category_info["name"],
                                        'Para Birimi': currency_symbol
                                    }
                                    df_list.append(data_row)

                                successful_count += 1
                        else:
                            pass
                    
            except Exception as e:
                st.warning(f"⚠️ {instrument} için hata: {str(e)}")
                continue
        
        if df_list:
            df = pd.DataFrame(df_list)
            return df
        else:
            return None
            
    except Exception as e:
        st.error(f"❌ Veri çekilirken genel hata: {str(e)}")
        return None

def get_bist_data_from_yahoo(start_date=None, end_date=None):
    """Yahoo Finance'den BIST verilerini çek - Ana yöntem"""
    try:
        bist_stocks = list(BIST_STOCKS.keys())
        df_list = []
        successful_count = 0
        
        # Tarih aralığı ayarla - eğer verilmemişse son 1 günü kullan
        if start_date is None or end_date is None:
            period = "1d"
            use_period = True
        else:
            use_period = False
        
        for i, stock in enumerate(bist_stocks):
            try:
                ticker = yf.Ticker(f"{stock}.IS", session=YF_SESSION)
                info = ticker.info
                
                # Tarih aralığına göre veri çek
                if use_period:
                    hist = ticker.history(period=period)
                else:
                    hist = ticker.history(start=start_date, end=end_date)
                
                if not hist.empty:
                    last_price = hist['Close'].iloc[-1]
                    open_price = hist['Open'].iloc[-1]
                    high_price = hist['High'].iloc[-1]
                    low_price = hist['Low'].iloc[-1]
                    volume = hist['Volume'].iloc[-1]
                    
                    change = last_price - open_price
                    change_percent = (change / open_price) * 100 if open_price > 0 else 0
                    
                    row = {
                        'Hisse Kodu': stock,
                        'Hisse Adı': get_stock_display_name(stock),
                        'Son Fiyat': float(last_price),
                        'Değişim': float(change),
                        'Değişim %': float(change_percent),
                        'En Yüksek': float(high_price),
                        'En Düşük': float(low_price),
                        'Açılış': float(open_price),
                        'Hacim': float(volume),
                        'Para Birimi': '₺',
                        'Güncelleme Zamanı': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    df_list.append(row)
                    successful_count += 1
                    st.success(f"✅ {stock} verisi alındı")
                else:
                    st.warning(f"⚠️ {stock} için veri yok")
                    
            except Exception as e:
                st.warning(f"⚠️ {stock} için hata: {str(e)}")
                continue
            
        if df_list:
            df = pd.DataFrame(df_list)
            return df
        else:
            return None
            
    except Exception as e:
        st.error(f"❌ Yahoo Finance'den veri çekilirken genel hata: {str(e)}")
        return None

def get_specific_instrument_data(instrument_category, instruments_list, start_date, end_date):
    """Seçilen enstrümanlar için belirtilen tarih aralığında detaylı veri çek"""
    from datetime import datetime, timedelta
    
    try:
        # Dinamik kategorileri al
        current_categories = get_instrument_categories()
        category_info = current_categories[instrument_category]
        if instrument_category == "TEFAS":
            instruments_data = get_tefas_funds_dynamic()
            category_info["data"] = instruments_data
        else:
            instruments_data = category_info["data"]
        suffix = category_info["suffix"]
        
        df_list = []
        
        # Türk altın fiyatları için tarihsel hesaplama
        turkish_gold_instruments = TURKISH_GOLD_INSTRUMENTS
        
        # İşlemler arasına delay ekle (Rate limiting için)
        import time as time_module
        
        for idx, instrument in enumerate(instruments_list):
            # Her enstrümandan sonra kısa bir bekleme (ilki hariç)
            if idx > 0:
                time_module.sleep(0.5)  # 500ms bekleme
            
            try:
                # Türk altın fiyatları için özel işlem - Yahoo Finance'tan hesapla (blob okuma yok)
                if instrument in turkish_gold_instruments:
                    try:
                        # Bugünün tarihi mi kontrol et
                        today = datetime.now().date()
                        
                        # İstenen tarih aralığındaki her gün için işlem yap
                        current_date = start_date
                        found_count = 0
                        
                        while current_date <= end_date:
                            # Bugün için mi hesaplıyoruz?
                            is_today = (current_date == today)
                            
                            # O gün için fiyatları hesapla
                            gold_prices = calculate_turkish_gold_prices(current_date, is_today=is_today)
                            
                            if gold_prices and instrument in gold_prices:
                                price = gold_prices[instrument]
                                
                                # İnstrüman adını belirle
                                instrument_names = {
                                    "ALTIN_GRAM": "Gram Altın",
                                    "ALTIN_CEYREK": "Çeyrek Altın",
                                    "ALTIN_YARIM": "Yarım Altın",
                                    "ALTIN_TAM": "Tam Altın",
                                    "ALTIN_ONS_TRY": "Ons Altın (TRY)",
                                    "ALTIN_RESAT": "Reşat Altını",
                                    "ALTIN_CUMHURIYET": "Cumhuriyet Altını",
                                    "ALTIN_ATA": "Ata Altını",
                                    "ALTIN_HAMIT": "Hamit Altını",
                                    "ALTIN_IKIBUCUK": "İkibuçuk Altın",
                                    "ALTIN_BESLI": "Beşli Altın",
                                    "ALTIN_14AYAR": "14 Ayar Altın",
                                    "ALTIN_18AYAR": "18 Ayar Altın",
                                    "ALTIN_22AYAR_BILEZIK": "22 Ayar Bilezik"
                                }
                                
                                data_row = {
                                    'Kod': instrument,
                                    'Adı': instrument_names.get(instrument, instrument),
                                    'Tarih': current_date.strftime('%Y-%m-%d'),
                                    'Açılış': float(price),
                                    'En Yüksek': float(price),
                                    'En Düşük': float(price),
                                    'Kapanış': float(price),
                                    'Hacim': 0,
                                    'Kategori': category_info["name"],
                                    'Para Birimi': "₺"
                                }
                                df_list.append(data_row)
                                found_count += 1
                            
                            # Bir sonraki güne geç
                            current_date += timedelta(days=1)
                            
                            # Rate limiting (bugün hariç)
                            if not is_today and current_date <= end_date:
                                time_module.sleep(0.3)
                        
                        if found_count > 0:
                            st.success(f"✅ {instrument} verisi alındı ({found_count} gün)")
                        else:
                            st.warning(f"⚠️ {instrument} için {start_date} - {end_date} tarih aralığında veri hesaplanamadı")
                        
                    except Exception as e:
                        st.error(f"❌ {instrument} için fiyat hesaplama hatası: {str(e)}")
                        continue

                # TEFAS fonları için özel işlem
                elif instrument_category == "TEFAS":
                    try:
                        # Azure Blob Storage'dan TEFAS verisi al
                        from datetime import datetime, timedelta
                        
                        # Azure'dan tüm fon verilerini bir kerede çek
                        try:
                            content = tefas_dm.blob_storage.download_file(tefas_dm.data_file)
                            if content:
                                parquet_buffer = io.BytesIO(content)
                                df = pd.read_parquet(parquet_buffer)
                                
                                # Sütun isimlerini normalize et (hem alt çizgi hem boşluk destekli)
                                column_mapping = {}
                                for col in df.columns:
                                    if 'fon' in col.lower() and 'kod' in col.lower():
                                        column_mapping[col] = 'Fon_Kodu'
                                    elif 'fiyat' in col.lower() or 'price' in col.lower():
                                        column_mapping[col] = 'Fiyat'
                                    elif 'tarih' in col.lower() or 'date' in col.lower():
                                        column_mapping[col] = 'Tarih'
                                
                                if column_mapping:
                                    df = df.rename(columns=column_mapping)
                                
                                # Bu fon için tüm verileri filtrele (case-insensitive ve trim ile)
                                if 'Fon_Kodu' in df.columns:
                                    # Fon kodlarını normalize et
                                    df['Fon_Kodu_Clean'] = df['Fon_Kodu'].astype(str).str.strip().str.upper()
                                    instrument_clean = str(instrument).strip().upper()
                                    
                                    fund_df = df[df['Fon_Kodu_Clean'] == instrument_clean].copy()
                                    
                                    # Eğer bulunamazsa, fon adında arama yap
                                    if fund_df.empty and 'Fon_Adi' in df.columns:
                                        st.warning(f"🔍 '{instrument}' kodu bulunamadı, fon adında aranıyor...")
                                        # Fon adında "PPK" veya instrument geçen kayıtları bul
                                        df['Fon_Adi_Clean'] = df['Fon_Adi'].astype(str).str.upper()
                                        fund_df = df[df['Fon_Adi_Clean'].str.contains(instrument_clean, na=False)].copy()
                                        
                                        if not fund_df.empty:
                                            actual_code = fund_df.iloc[0]['Fon_Kodu']
                                            st.info(f"✅ '{instrument}' ile eşleşen fon bulundu: Kod='{actual_code}'")
                                    
                                    if not fund_df.empty:
                                        # Tarih sütununu datetime'a çevir
                                        if 'Tarih' in fund_df.columns:
                                            fund_df['Tarih'] = pd.to_datetime(fund_df['Tarih'])
                                            # Tarihe göre sırala (en eskiden en yeniye)
                                            fund_df = fund_df.sort_values('Tarih')
                                        
                                        # Sadece Azure'da gerçekten var olan tarihleri kullan
                                        found_dates = 0
                                        skipped_dates = 0
                                        
                                        # İstenen tarih aralığındaki her gün için kontrol et
                                        current_date = start_date
                                        while current_date <= end_date:
                                            # Bu tarihe ait veriyi bul
                                            date_data = fund_df[fund_df['Tarih'].dt.date == current_date]
                                            
                                            if not date_data.empty and 'Fiyat' in date_data.columns:
                                                # O tarihe ait gerçek fiyatı al (eğer birden fazla satır varsa en son olanı)
                                                daily_price = float(date_data.iloc[-1]['Fiyat'])
                                                
                                                data_row = {
                                                    'Kod': instrument,
                                                    'Adı': instruments_data.get(instrument, instrument),
                                                    'Tarih': current_date.strftime('%Y-%m-%d'),
                                                    'Açılış': daily_price,
                                                    'En Yüksek': daily_price,
                                                    'En Düşük': daily_price,
                                                    'Kapanış': daily_price,
                                                    'Hacim': 0,
                                                    'Kategori': category_info["name"],
                                                    'Para Birimi': "₺"
                                                }
                                                df_list.append(data_row)
                                                found_dates += 1
                                            else:
                                                # O tarihte veri yoksa atla (boş bırak)
                                                skipped_dates += 1
                                            
                                            current_date += timedelta(days=1)
                                        
                                        if found_dates > 0:
                                            st.success(f"✅ {instrument} verisi alındı")
                                        else:
                                            st.warning(f"⚠️ {instrument} için {start_date} - {end_date} aralığında hiç veri bulunamadı")
                                    else:
                                        st.warning(f"⚠️ {instrument} fonu için Azure'da veri bulunamadı")
                                else:
                                    st.warning(f"⚠️ Azure TEFAS dosyasında 'Fon_Kodu' sütunu bulunamadı. Sütunlar: {list(df.columns)}")
                            else:
                                st.warning(f"⚠️ Azure'dan TEFAS verisi indirilemedi")
                        except Exception as e:
                            st.error(f"❌ Azure'dan {instrument} verisi alınırken hata: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())
                    
                    except Exception as e:
                        st.warning(f"⚠️ {instrument} TEFAS fonu işlenirken hata: {str(e)}")
                        continue
                
                else:
                    # Normal Yahoo Finance işlemi (TEFAS hariç)
                    ticker_symbol = f"{instrument}{suffix}"
                    
                    # Retry mekanizması ile veri çek
                    max_retries = 3
                    retry_count = 0
                    hist = None
                    
                    while retry_count < max_retries and (hist is None or hist.empty):
                        try:
                            ticker = yf.Ticker(ticker_symbol, session=YF_SESSION)
                            hist = ticker.history(start=start_date, end=end_date)
                            
                            if not hist.empty:
                                break
                            else:
                                retry_count += 1
                                if retry_count < max_retries:
                                    time.sleep(1)  # 1 saniye bekle
                        except Exception as e:
                            retry_count += 1
                            if retry_count < max_retries:
                                st.warning(f"⚠️ {instrument} için deneme {retry_count}/{max_retries}, tekrar deneniyor...")
                                time.sleep(2)  # Rate limit için daha uzun bekle
                            else:
                                st.error(f"❌ {instrument} için {max_retries} denemeden sonra veri alınamadı: {str(e)}")
                                hist = None

                    if hist is not None and not hist.empty:
                        for date, row in hist.iterrows():
                            data_row = {
                                'Kod': instrument,
                                'Adı': instruments_data.get(instrument, instrument),
                                'Tarih': date.strftime('%Y-%m-%d'),
                                'Açılış': float(row['Open']),
                                'En Yüksek': float(row['High']),
                                'En Düşük': float(row['Low']),
                                'Kapanış': float(row['Close']),
                                'Hacim': float(row['Volume']) if 'Volume' in row and pd.notna(row['Volume']) else 0,
                                'Kategori': category_info["name"],
                                'Para Birimi': category_info["currency"]
                            }
                            df_list.append(data_row)

                        st.success(f"✅ {instrument} verisi alındı ({len(hist)} gün)")
                    else:
                        st.warning(f"⚠️ {instrument} için belirtilen tarih aralığında veri alınamadı (Yahoo Finance API sorunu olabilir)")

            except Exception as e:
                st.warning(f"⚠️ {instrument} için hata: {str(e)}")
                continue
        
        if df_list:
            df = pd.DataFrame(df_list)
            return df
        else:
            return None
            
    except Exception as e:
        st.error(f"❌ Detaylı veri çekilirken hata: {str(e)}")
        return None

def get_specific_stock_data(stocks_list, start_date, end_date):
    """Seçilen hisseler için belirtilen tarih aralığında detaylı veri çek"""
    try:
        df_list = []
        
        # İşlemler arasına delay ekle (Rate limiting için)
        import time as time_module
        
        for idx, stock in enumerate(stocks_list):
            # Her hisseden sonra kısa bir bekleme (ilki hariç)
            if idx > 0:
                time_module.sleep(0.5)  # 500ms bekleme
            
            try:
                # Retry mekanizması ile veri çek
                max_retries = 3
                retry_count = 0
                hist = None
                
                while retry_count < max_retries and (hist is None or hist.empty):
                    try:
                        ticker = yf.Ticker(f"{stock}.IS", session=YF_SESSION)
                        hist = ticker.history(start=start_date, end=end_date)
                        
                        if not hist.empty:
                            break
                        else:
                            retry_count += 1
                            if retry_count < max_retries:
                                time.sleep(1)  # 1 saniye bekle
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            st.warning(f"⚠️ {stock} için deneme {retry_count}/{max_retries}, tekrar deneniyor...")
                            time.sleep(2)  # Rate limit için daha uzun bekle
                        else:
                            st.error(f"❌ {stock} için {max_retries} denemeden sonra veri alınamadı: {str(e)}")
                            hist = None
                
                if hist is not None and not hist.empty:
                    for date, row in hist.iterrows():
                        data_row = {
                            'Hisse Kodu': stock,
                            'Hisse Adı': get_stock_display_name(stock),
                            'Tarih': date.strftime('%Y-%m-%d'),
                            'Açılış': float(row['Open']),
                            'En Yüksek': float(row['High']),
                            'En Düşük': float(row['Low']),
                            'Kapanış': float(row['Close']),
                            'Hacim': float(row['Volume']),
                            'Para Birimi': '₺'
                        }
                        df_list.append(data_row)
                    
                    st.success(f"✅ {stock} verisi alındı ({len(hist)} gün)")
                else:
                    st.warning(f"⚠️ {stock} için belirtilen tarih aralığında veri alınamadı (Yahoo Finance API sorunu olabilir)")
                    
            except Exception as e:
                st.warning(f"⚠️ {stock} için hata: {str(e)}")
                continue
        
        if df_list:
            df = pd.DataFrame(df_list)
            return df
        else:
            return None
            
    except Exception as e:
        st.error(f"❌ Detaylı veri çekilirken hata: {str(e)}")
# ================ ANA UYGULAMA AKIŞI ================

def test_azure_connection():
    """Azure Blob Storage bağlantısını test et"""
    if blob_storage.blob_service_client:
        try:
            # Container'ı test et
            if blob_storage.file_exists("test.txt"):
                return True
            else:
                # Test dosyası oluştur
                blob_storage.upload_file("test.txt", b"Azure connection test")
                return True
        except Exception as e:
            return False
    else:
        return False

    test_azure_connection()

# Session state kontrolü - TEST MODU BYPASS
if 'logged_in' not in st.session_state:
    if TEST_MODE:
        st.session_state['logged_in'] = True  # Test modunda otomatik giriş
        st.session_state['user_email'] = 'erdalural@gmail.com'
        st.session_state['user_name'] = 'Erdal Ural (Test Kullanıcısı)'
    else:
        st.session_state['logged_in'] = False

# Beni Hatırla (Remember Me) session state başlatma
if 'remembered_email' not in st.session_state:
    st.session_state['remembered_email'] = ""
if 'remembered_password' not in st.session_state:
    st.session_state['remembered_password'] = ""

# Load remembered credentials from storage at app startup
_remembered_email, _remembered_password = load_remembered_credentials()
if _remembered_email and _remembered_password:
    st.session_state['remembered_email'] = _remembered_email
    st.session_state['remembered_password'] = _remembered_password

# Load persisted job settings (scheduler) from blob and apply defaults
try:
    _saved_job_settings = load_job_settings()
except Exception:
    _saved_job_settings = {}

# BIST scheduler session state initialization
if 'bist_scheduler_active' not in st.session_state:
    bist_setting = _saved_job_settings.get('bist', {}) if _saved_job_settings else {}
    st.session_state['bist_scheduler_active'] = bool(bist_setting.get('active', False))
if 'bist_update_period' not in st.session_state:
    st.session_state['bist_update_period'] = bist_setting.get('period', 'günlük')
if 'bist_update_time' not in st.session_state:
    # Try to parse saved time string
    try:
        t = bist_setting.get('time')
        st.session_state['bist_update_time'] = safe_parse_time(t, datetime_time(9, 0))
    except Exception:
        st.session_state['bist_update_time'] = datetime_time(9, 0)
if 'bist_update_logs' not in st.session_state:
    st.session_state['bist_update_logs'] = []

# NASDAQ scheduler session state initialization
if 'nasdaq_scheduler_active' not in st.session_state:
    nasdaq_setting = _saved_job_settings.get('nasdaq', {}) if _saved_job_settings else {}
    st.session_state['nasdaq_scheduler_active'] = bool(nasdaq_setting.get('active', False))
if 'nasdaq_update_period' not in st.session_state:
    st.session_state['nasdaq_update_period'] = nasdaq_setting.get('period', 'günlük')
if 'nasdaq_update_time' not in st.session_state:
    try:
        t = nasdaq_setting.get('time')
        st.session_state['nasdaq_update_time'] = safe_parse_time(t, datetime_time(9, 0))
    except Exception:
        st.session_state['nasdaq_update_time'] = datetime_time(9, 0)
    except Exception:
        st.session_state['nasdaq_update_time'] = datetime_time(9, 0)
if 'nasdaq_update_logs' not in st.session_state:
    st.session_state['nasdaq_update_logs'] = []

# TEFAS scheduler session state initialization
if 'tefas_scheduler_active' not in st.session_state:
    tefas_setting = _saved_job_settings.get('tefas', {}) if _saved_job_settings else {}
    st.session_state['tefas_scheduler_active'] = bool(tefas_setting.get('active', False))
if 'tefas_update_period' not in st.session_state:
    st.session_state['tefas_update_period'] = tefas_setting.get('period', 'günlük')
if 'tefas_update_time' not in st.session_state:
    try:
        t = tefas_setting.get('time')
        st.session_state['tefas_update_time'] = safe_parse_time(t, datetime_time(9, 0))
    except Exception:
        st.session_state['tefas_update_time'] = datetime_time(9, 0)
    except Exception:
        st.session_state['tefas_update_time'] = datetime_time(9, 0)
if 'tefas_update_logs' not in st.session_state:
    st.session_state['tefas_update_logs'] = []

# Turkish Gold scheduler session state initialization
if 'turkish_gold_scheduler_active' not in st.session_state:
    tg_setting = _saved_job_settings.get('turkish_gold', {}) if _saved_job_settings else {}
    st.session_state['turkish_gold_scheduler_active'] = bool(tg_setting.get('active', False))
if 'turkish_gold_update_period' not in st.session_state:
    st.session_state['turkish_gold_update_period'] = tg_setting.get('period', 'günlük')
if 'turkish_gold_update_time' not in st.session_state:
    try:
        t = tg_setting.get('time')
        st.session_state['turkish_gold_update_time'] = safe_parse_time(t, datetime_time(9, 0))
    except Exception:
        st.session_state['turkish_gold_update_time'] = datetime_time(9, 0)
    except Exception:
        st.session_state['turkish_gold_update_time'] = datetime_time(9, 0)
if 'turkish_gold_update_logs' not in st.session_state:
    st.session_state['turkish_gold_update_logs'] = []

# Giriş durumuna göre sayfa göster
if not st.session_state['logged_in']:
    show_login_page()
else:
    show_main_app()

# BIST, NASDAQ, TEFAS ve Turkish Gold periyodik güncelleme scheduler'larını başlat
# Başlat: Scheduler'ları her zaman başlat (uygulama process'i çalıştığı sürece çalışsın)
try:
    init_bist_scheduler()
    init_nasdaq_scheduler()
    init_tefas_scheduler()
    init_turkish_gold_scheduler()
except Exception as e:
    print(f"Scheduler init hata: {str(e)}")

