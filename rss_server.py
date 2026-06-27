from __future__ import annotations

import concurrent.futures
import email.utils
import hashlib
import html
import json
import math
import os
import re
import socketserver
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import ssl
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler
from pathlib import Path

try:
    from supabase import Client, create_client
except Exception:  # supabase-py non installato: il radar resta leggibile, storage disattivo.
    Client = None
    create_client = None


PORT = 8768
ROOT = Path(__file__).resolve().parent
SOURCES_FILE = ROOT / "sources.json"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_KEY and create_client)
_SUPABASE_CLIENT = None
# Segnale breaking Livello 1: stessa storia coperta da almeno N fonti RSS distinte nello stesso fetch.
# Nota onesta: misura velocita/corroborazione di pubblicazione cross-fonte, non engagement social.
# TODO Livello 2: con storage/snapshot, richiedere anche 3+ fonti entro una finestra dal first_seen (es. <30 min).
BREAKING_SOURCE_THRESHOLD = 3
# Ricalibrazione score: curve continue, range piu ampio, niente saturazione precoce.
# Onesta: spalma meglio i segnali RSS gia disponibili, non aggiunge engagement social.
# Con soli RSS il segnale reale resta la pubblicazione/corroborazione cross-fonte.
MAX_EXPECTED_SOURCES_FOR_SCORE = 12
FRESHNESS_HALF_LIFE_MINUTES = 240
SCORE_WEIGHTS = {
    "base": 12,
    "cross_source": 30,
    "freshness": 24,
    "authority": 20,
    "keywords": 14,
    "locality": 8,
    "breaking": 10,
}
PICCO_SCORE_THRESHOLD = 88
BREAKOUT_SCORE_THRESHOLD = 72
OSSERVA_SCORE_THRESHOLD = 50

FEEDS = [
    {
        "name": "ANSA",
        "url": "https://www.ansa.it/sito/notizie/topnews/topnews_rss.xml",
        "sourceUrl": "https://www.ansa.it/",
        "category": "Italia",
    },
    {
        "name": "ANSA Cronaca",
        "url": "https://www.ansa.it/sito/notizie/cronaca/cronaca_rss.xml",
        "sourceUrl": "https://www.ansa.it/sito/notizie/cronaca/",
        "category": "Italia",
    },
    {
        "name": "ANSA Sicilia",
        "url": "https://www.ansa.it/sicilia/notizie/sicilia_rss.xml",
        "sourceUrl": "https://www.ansa.it/sicilia/",
        "category": "Sicilia",
    },
    {
        "name": "ANSA Calabria",
        "url": "https://www.ansa.it/calabria/notizie/calabria_rss.xml",
        "sourceUrl": "https://www.ansa.it/calabria/",
        "category": "Calabria",
    },
    {
        "name": "AGI Cronaca",
        "url": "https://www.agi.it/cronaca/rss.xml",
        "sourceUrl": "https://www.agi.it/cronaca/",
        "category": "Italia",
    },
    {
        "name": "AGI Politica",
        "url": "https://www.agi.it/politica/rss.xml",
        "sourceUrl": "https://www.agi.it/politica/",
        "category": "Politica",
    },
    {
        "name": "AGI Economia",
        "url": "https://www.agi.it/economia/rss.xml",
        "sourceUrl": "https://www.agi.it/economia/",
        "category": "Finanza",
    },
    {
        "name": "AGI Estero",
        "url": "https://www.agi.it/estero/rss.xml",
        "sourceUrl": "https://www.agi.it/estero/",
        "category": "Italia",
    },
    {
        "name": "AGI Innovazione",
        "url": "https://www.agi.it/innovazione/rss.xml",
        "sourceUrl": "https://www.agi.it/innovazione/",
        "category": "Tecnologia",
    },
    {
        "name": "AGI Salute",
        "url": "https://www.agi.it/salute/rss.xml",
        "sourceUrl": "https://www.agi.it/salute/",
        "category": "Italia",
    },
    {
        "name": "Corriere",
        "url": "https://xml2.corriereobjects.it/rss/homepage.xml",
        "sourceUrl": "https://www.corriere.it/",
        "category": "Italia",
    },
    {
        "name": "Corriere Cronache",
        "url": "https://xml2.corriereobjects.it/rss/cronache.xml",
        "sourceUrl": "https://www.corriere.it/cronache/",
        "category": "Italia",
    },
    {
        "name": "Repubblica",
        "url": "https://www.repubblica.it/rss/homepage/rss2.0.xml",
        "sourceUrl": "https://www.repubblica.it/",
        "category": "Italia",
    },
    {
        "name": "Repubblica Cronaca",
        "url": "https://www.repubblica.it/rss/cronaca/rss2.0.xml",
        "sourceUrl": "https://www.repubblica.it/cronaca/",
        "category": "Italia",
    },
    {
        "name": "Fanpage",
        "url": "https://www.fanpage.it/feed/",
        "sourceUrl": "https://www.fanpage.it/",
        "category": "Social",
    },
    {
        "name": "Il Fatto Quotidiano",
        "url": "https://www.ilfattoquotidiano.it/feed/",
        "sourceUrl": "https://www.ilfattoquotidiano.it/",
        "category": "Politica",
    },
    {
        "name": "Sky TG24",
        "url": "https://tg24.sky.it/rss/tg24.xml",
        "sourceUrl": "https://tg24.sky.it/",
        "category": "Italia",
    },
    {
        "name": "TGCom24",
        "url": "https://www.tgcom24.mediaset.it/rss/homepage.xml",
        "sourceUrl": "https://www.tgcom24.mediaset.it/",
        "category": "Italia",
    },
    {
        "name": "Adnkronos Ultima Ora",
        "url": "https://www.adnkronos.com/RSS_Ultimora.xml",
        "sourceUrl": "https://www.adnkronos.com/",
        "category": "Italia",
    },
    {
        "name": "Adnkronos Politica",
        "url": "https://www.adnkronos.com/RSS_Politica.xml",
        "sourceUrl": "https://www.adnkronos.com/politica/",
        "category": "Politica",
    },
    {
        "name": "Open",
        "url": "https://www.open.online/feed/",
        "sourceUrl": "https://www.open.online/",
        "category": "Italia",
    },
    {
        "name": "Today",
        "url": "https://www.today.it/rss",
        "sourceUrl": "https://www.today.it/",
        "category": "Italia",
    },
    {
        "name": "TPI",
        "url": "https://www.tpi.it/feed/",
        "sourceUrl": "https://www.tpi.it/",
        "category": "Politica",
    },
    {
        "name": "Wired Italia",
        "url": "https://www.wired.it/feed/rss",
        "sourceUrl": "https://www.wired.it/",
        "category": "Tecnologia",
    },
    {
        "name": "Il Sole 24 Ore",
        "url": "https://www.ilsole24ore.com/rss/italia.xml",
        "sourceUrl": "https://www.ilsole24ore.com/",
        "category": "Finanza",
    },
    {
        "name": "Tempostretto",
        "url": "https://www.tempostretto.it/feed",
        "sourceUrl": "https://www.tempostretto.it/",
        "category": "Sicilia",
    },
    {
        "name": "StrettoWeb",
        "url": "https://www.strettoweb.com/feed/",
        "sourceUrl": "https://www.strettoweb.com/",
        "category": "Sicilia",
    },
    {
        "name": "Live Sicilia",
        "url": "https://livesicilia.it/feed/",
        "sourceUrl": "https://livesicilia.it/",
        "category": "Sicilia",
    },
    {
        "name": "MeridioNews",
        "url": "https://meridionews.it/feed/",
        "sourceUrl": "https://meridionews.it/",
        "category": "Sicilia",
    },
    {
        "name": "BlogSicilia",
        "url": "https://www.blogsicilia.it/feed/",
        "sourceUrl": "https://www.blogsicilia.it/",
        "category": "Sicilia",
    },
    {
        "name": "SiciliaNews24",
        "url": "https://www.sicilianews24.it/feed/",
        "sourceUrl": "https://www.sicilianews24.it/",
        "category": "Sicilia",
    },
    {
        "name": "PalermoToday",
        "url": "https://www.palermotoday.it/rss",
        "sourceUrl": "https://www.palermotoday.it/",
        "category": "Sicilia",
    },
    {
        "name": "CataniaToday",
        "url": "https://www.cataniatoday.it/rss",
        "sourceUrl": "https://www.cataniatoday.it/",
        "category": "Sicilia",
    },
    {
        "name": "MessinaToday",
        "url": "https://www.messinatoday.it/rss",
        "sourceUrl": "https://www.messinatoday.it/",
        "category": "Sicilia",
    },
    {
        "name": "ReggioToday",
        "url": "https://www.reggiotoday.it/rss",
        "sourceUrl": "https://www.reggiotoday.it/",
        "category": "Calabria",
    },
    {
        "name": "CatanzaroInforma",
        "url": "https://www.catanzaroinforma.it/feed/",
        "sourceUrl": "https://www.catanzaroinforma.it/",
        "category": "Calabria",
    },
    {
        "name": "CrotoneNews",
        "url": "https://www.crotonenews.com/feed/",
        "sourceUrl": "https://www.crotonenews.com/",
        "category": "Calabria",
    },
    {
        "name": "Il Crotonese",
        "url": "https://www.ilcrotonese.it/feed/",
        "sourceUrl": "https://www.ilcrotonese.it/",
        "category": "Calabria",
    },
    {
        "name": "Zoom24",
        "url": "https://www.zoom24.it/feed/",
        "sourceUrl": "https://www.zoom24.it/",
        "category": "Calabria",
    },
    {
        "name": "Calabria Diretta News",
        "url": "https://www.calabriadirettanews.com/feed/",
        "sourceUrl": "https://www.calabriadirettanews.com/",
        "category": "Calabria",
    },
    {
        "name": "CN24",
        "url": "https://www.cn24tv.it/feed/",
        "sourceUrl": "https://www.cn24tv.it/",
        "category": "Calabria",
    },
    {
        "name": "CityNow",
        "url": "https://www.citynow.it/feed/",
        "sourceUrl": "https://www.citynow.it/",
        "category": "Calabria",
    },

    {
        "name": "Askanews",
        "url": "https://www.askanews.it/feed/",
        "sourceUrl": "https://www.askanews.it/",
        "category": "Italia",
    },
    {
        "name": "Italpress",
        "url": "https://www.italpress.com/feed/",
        "sourceUrl": "https://www.italpress.com/",
        "category": "Italia",
    },
    {
        "name": "RaiNews",
        "url": "https://www.rainews.it/rss/tutti",
        "sourceUrl": "https://www.rainews.it/",
        "category": "Italia",
    },
    {
        "name": "La Stampa",
        "url": "https://www.lastampa.it/rss/copertina.xml",
        "sourceUrl": "https://www.lastampa.it/",
        "category": "Italia",
    },
    {
        "name": "Il Messaggero",
        "url": "https://www.ilmessaggero.it/rss/home.xml",
        "sourceUrl": "https://www.ilmessaggero.it/",
        "category": "Italia",
    },
    {
        "name": "Il Giornale",
        "url": "https://www.ilgiornale.it/feed.xml",
        "sourceUrl": "https://www.ilgiornale.it/",
        "category": "Italia",
    },
    {
        "name": "Libero",
        "url": "https://www.liberoquotidiano.it/rss.xml",
        "sourceUrl": "https://www.liberoquotidiano.it/",
        "category": "Italia",
    },
    {
        "name": "Il Manifesto",
        "url": "https://ilmanifesto.it/feed",
        "sourceUrl": "https://ilmanifesto.it/",
        "category": "Italia",
    },
    {
        "name": "Quotidiano.net",
        "url": "https://www.quotidiano.net/rss",
        "sourceUrl": "https://www.quotidiano.net/",
        "category": "Italia",
    },
    {
        "name": "Il Mattino",
        "url": "https://www.ilmattino.it/rss/home.xml",
        "sourceUrl": "https://www.ilmattino.it/",
        "category": "Italia",
    },
    {
        "name": "Affaritaliani",
        "url": "https://www.affaritaliani.it/rss/",
        "sourceUrl": "https://www.affaritaliani.it/",
        "category": "Politica",
    },
    {
        "name": "Linkiesta",
        "url": "https://www.linkiesta.it/feed/",
        "sourceUrl": "https://www.linkiesta.it/",
        "category": "Politica",
    },
    {
        "name": "Formiche",
        "url": "https://formiche.net/feed/",
        "sourceUrl": "https://formiche.net/",
        "category": "Politica",
    },
    {
        "name": "Blitz Quotidiano",
        "url": "https://www.blitzquotidiano.it/feed/",
        "sourceUrl": "https://www.blitzquotidiano.it/",
        "category": "Italia",
    },
    {
        "name": "Notizie.it",
        "url": "https://www.notizie.it/feed/",
        "sourceUrl": "https://www.notizie.it/",
        "category": "Italia",
    },
    {
        "name": "Wall Street Italia",
        "url": "https://www.wallstreetitalia.com/feed/",
        "sourceUrl": "https://www.wallstreetitalia.com/",
        "category": "Finanza",
    },
    {
        "name": "FinanzaOnline",
        "url": "https://www.finanzaonline.com/feed",
        "sourceUrl": "https://www.finanzaonline.com/",
        "category": "Finanza",
    },
    {
        "name": "InvestireOggi",
        "url": "https://www.investireoggi.it/feed/",
        "sourceUrl": "https://www.investireoggi.it/",
        "category": "Finanza",
    },
    {
        "name": "Forbes Italia",
        "url": "https://forbes.it/feed/",
        "sourceUrl": "https://forbes.it/",
        "category": "Finanza",
    },
    {
        "name": "Gazzetta dello Sport",
        "url": "http://www.gazzetta.it/rss/home.xml",
        "sourceUrl": "https://www.gazzetta.it/",
        "category": "Sport",
    },
    {
        "name": "Corriere dello Sport",
        "url": "http://www.corrieredellosport.it/rss/rss.shtml",
        "sourceUrl": "https://www.corrieredellosport.it/",
        "category": "Sport",
    },
    {
        "name": "Tuttosport",
        "url": "https://www.tuttosport.com/rss/rss.xml",
        "sourceUrl": "https://www.tuttosport.com/",
        "category": "Sport",
    },
    {
        "name": "SportMediaset",
        "url": "https://www.sportmediaset.mediaset.it/rss/",
        "sourceUrl": "https://www.sportmediaset.mediaset.it/",
        "category": "Sport",
    },
    {
        "name": "OA Sport",
        "url": "https://www.oasport.it/feed/",
        "sourceUrl": "https://www.oasport.it/",
        "category": "Sport",
    },
    {
        "name": "TuttoMercatoWeb",
        "url": "https://www.tuttomercatoweb.com/rss/",
        "sourceUrl": "https://www.tuttomercatoweb.com/",
        "category": "Sport",
    },
    {
        "name": "FantaMaster",
        "url": "https://www.fantamaster.it/feed/",
        "sourceUrl": "https://www.fantamaster.it/",
        "category": "Sport",
    },
    {
        "name": "HDblog",
        "url": "https://www.hdblog.it/feed/",
        "sourceUrl": "https://www.hdblog.it/",
        "category": "Tecnologia",
    },
    {
        "name": "SmartWorld",
        "url": "https://www.smartworld.it/feed",
        "sourceUrl": "https://www.smartworld.it/",
        "category": "Tecnologia",
    },
    {
        "name": "Tom's Hardware",
        "url": "https://www.tomshw.it/feed/",
        "sourceUrl": "https://www.tomshw.it/",
        "category": "Tecnologia",
    },
    {
        "name": "Punto Informatico",
        "url": "https://www.punto-informatico.it/feed/",
        "sourceUrl": "https://www.punto-informatico.it/",
        "category": "Tecnologia",
    },
    {
        "name": "Focus",
        "url": "https://www.focus.it/rss",
        "sourceUrl": "https://www.focus.it/",
        "category": "Tecnologia",
    },
]

TERRITORIES = [
    ("Messina", "Sicilia", ["messina", "milazzo", "barcellona pozzo di gotto", "taormina", "giardini naxos"]),
    ("Palermo", "Sicilia", ["palermo", "bagheria", "monreale"]),
    ("Catania", "Sicilia", ["catania", "etna", "acireale", "adrano", "paterno"]),
    ("Reggio Calabria", "Calabria", ["reggio calabria", "reggino", "villa san giovanni", "gioia tauro", "locri"]),
    ("Cosenza", "Calabria", ["cosenza", "cosentino", "rende", "corigliano", "rossano", "sibari"]),
    ("Catanzaro", "Calabria", ["catanzaro", "lametino", "lamezia", "soverato"]),
    ("Crotone", "Calabria", ["crotone", "crotonese", "isola capo rizzuto"]),
    ("Vibo Valentia", "Calabria", ["vibo valentia", "vibonese", "tropea", "costa degli dei"]),
    ("Sicilia", "Sicilia", ["sicilia", "siciliano", "siciliana"]),
    ("Calabria", "Calabria", ["calabria", "calabrese", "calabresi"]),
]

TOPIC_KEYWORDS = [
    "sanita",
    "maltempo",
    "allerta",
    "incendio",
    "trasporti",
    "porto",
    "ponte",
    "stretto",
    "scuola",
    "rifiuti",
    "acqua",
    "cronaca",
    "politica",
    "turismo",
    "ambiente",
    "lavoro",
    "incident",
    "inchiesta",
    "mafia",
    "ndrangheta",
    "ai",
    "intelligenza artificiale",
]


TERM_STOPWORDS = {
    "alla", "allo", "alle", "agli", "all", "dalla", "dallo", "dalle", "dagli", "dell", "della", "dello", "delle", "degli",
    "nella", "nello", "nelle", "negli", "sulla", "sullo", "sulle", "sugli", "con", "per", "tra", "fra", "sul", "del", "dei",
    "dal", "nel", "gli", "che", "chi", "cui", "non", "sono", "come", "dopo", "prima", "oggi", "ieri", "domani", "anche",
    "ancora", "nuovo", "nuova", "nuovi", "nuove", "tutto", "tutti", "tutte", "solo", "stata", "stato", "stati", "essere",
    "notizie", "news", "ultime", "ultima", "ora", "video", "foto", "diretta", "aggiornamento", "aggiornamenti", "articolo",
    "italia", "italiano", "italiana", "sicilia", "siciliano", "siciliana", "calabria", "calabrese", "calabresi",
    "palermo", "catania", "messina", "reggio", "calabria", "ansa", "giornale", "sicilia", "cronaca",
    "politica", "tecnologia", "sport", "finanza", "economia", "cultura", "sicurezza", "social", "italia",
    "anni", "anno", "mesi", "mese", "giorni", "giorno", "nuovo", "nuova", "nuovi", "nuove", "prima", "dopo",
    "esteri", "oltre", "milioni", "mila", "euro", "caso", "casi", "parte", "verso", "contro",
    "ecco", "attualit?", "attualita", "tutte", "tutti", "dice", "fare", "fatto", "fatti",
}

TERM_SYNONYMS = {
    "intelligenza artificiale": "AI",
    "artificiale": "AI",
    "scioperi": "sciopero",
    "trasporti": "trasporti",
    "maltempo": "maltempo",
    "ponte": "ponte stretto",
    "stretto": "stretto",
}


class RadarHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/rss":
            limit = int(urllib.parse.parse_qs(parsed.query).get("limit", ["80"])[0])
            payload = load_rss_payload(limit=limit)
            self.send_json(payload)
            return

        if parsed.path == "/api/sources":
            self.send_json(load_sources_payload())
            return

        if parsed.path == "/":
            self.path = "/newshub-ai-radar.html"
        return super().do_GET()

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length else "{}"
            data = json.loads(raw or "{}")
            if parsed.path == "/api/sources":
                self.send_json(add_source(data))
                return
            if parsed.path == "/api/sources/toggle":
                self.send_json(toggle_source(data))
                return
            self.send_json({"ok": False, "error": "Endpoint non trovato"}, status=404)
        except Exception as exc:  # noqa: BLE001 - local diagnostics for UI.
            self.send_json({"ok": False, "error": str(exc)[:240]}, status=400)

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        print(fmt % args)


def load_sources_payload() -> dict:
    sources = read_sources()
    return {
        "ok": True,
        "sources": sources,
        "total": len(sources),
        "active": sum(1 for source in sources if source.get("active", True)),
    }


def read_sources() -> list[dict]:
    if SOURCES_FILE.exists():
        try:
            payload = json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
            sources = payload.get("sources", payload if isinstance(payload, list) else [])
            if isinstance(sources, list):
                return [normalize_source(source) for source in sources if isinstance(source, dict)]
        except Exception as exc:  # noqa: BLE001
            print(f"sources.json non leggibile: {exc}")
    return [normalize_source(source) for source in FEEDS]


def write_sources(sources: list[dict]) -> None:
    payload = {"version": 1, "updatedAt": datetime.now(timezone.utc).isoformat(), "sources": sources}
    tmp = SOURCES_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SOURCES_FILE)


def normalize_source(source: dict) -> dict:
    name = clean_text(source.get("name") or source.get("title") or domain_from_url(source.get("url", "")) or "Fonte RSS")
    category = clean_text(source.get("category") or "Italia")
    area = clean_text(source.get("area") or category)
    url = clean_text(source.get("url") or "")
    normalized = {
        "name": name,
        "url": url,
        "sourceUrl": clean_text(source.get("sourceUrl") or site_root(url)),
        "category": category,
        "area": area,
        "authority": int(source.get("authority") or source_authority(name)),
        "active": bool(source.get("active", True)),
    }
    for key in ("testOk", "testItems", "testTitle", "testError", "testedAt", "lastStatus"):
        if key in source:
            normalized[key] = source[key]
    return normalized


def active_feeds() -> list[dict]:
    return [source for source in read_sources() if source.get("active", True) and source.get("url")]


def add_source(data: dict) -> dict:
    url = clean_text(data.get("url", ""))
    if not url.startswith(("http://", "https://")):
        return {"ok": False, "error": "Inserisci un URL completo che inizi con http:// o https://"}
    sources = read_sources()
    if any(source.get("url") == url for source in sources):
        return {"ok": False, "error": "Questa fonte e' gia presente."}
    validation = validate_feed_url(url)
    if not validation.get("ok"):
        return validation
    source = normalize_source({
        "name": clean_text(data.get("name") or validation.get("title") or domain_from_url(url) or "Nuova fonte"),
        "url": url,
        "sourceUrl": site_root(url),
        "category": clean_text(data.get("category") or "Italia"),
        "area": clean_text(data.get("area") or data.get("category") or "Italia"),
        "authority": int(data.get("authority") or 74),
        "active": True,
        "testOk": True,
        "testItems": validation.get("items", 0),
        "testTitle": validation.get("title", ""),
        "testedAt": datetime.now(timezone.utc).isoformat(),
        "lastStatus": "ok",
    })
    sources.append(source)
    write_sources(sources)
    return {"ok": True, "source": source, "message": "Fonte aggiunta e attivata."}


def toggle_source(data: dict) -> dict:
    url = clean_text(data.get("url", ""))
    active = bool(data.get("active"))
    sources = read_sources()
    changed = False
    for source in sources:
        if source.get("url") == url:
            source["active"] = active
            changed = True
            break
    if not changed:
        return {"ok": False, "error": "Fonte non trovata."}
    write_sources(sources)
    return {"ok": True, "url": url, "active": active}


def validate_feed_url(url: str) -> dict:
    try:
        xml_text = fetch(url)
        root = ET.fromstring(xml_text.lstrip())
        items = parse_feed(xml_text)
        if not items:
            return {"ok": False, "error": "L'URL risponde, ma non contiene articoli RSS/Atom."}
        title = ""
        channel_title = root.find("./channel/title")
        if channel_title is not None and channel_title.text:
            title = clean_text(channel_title.text)
        if not title and root.tag.endswith("feed"):
            atom_title = root.find("{http://www.w3.org/2005/Atom}title")
            if atom_title is not None and atom_title.text:
                title = clean_text(atom_title.text)
        return {"ok": True, "title": title, "items": len(items)}
    except ET.ParseError:
        return {"ok": False, "error": "Non e' un feed valido: la risposta non e' XML RSS/Atom."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"Feed non raggiungibile o non valido: {str(exc)[:160]}"}


def domain_from_url(url: str) -> str:
    try:
        return urllib.parse.urlsplit(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def site_root(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme and parsed.netloc:
            return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))
    except Exception:
        pass
    return url



def supabase_client() -> Client | None:
    global _SUPABASE_CLIENT
    if not SUPABASE_ENABLED:
        return None
    if _SUPABASE_CLIENT is None:
        _SUPABASE_CLIENT = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _SUPABASE_CLIENT


def persist_to_supabase(stories: list[dict]) -> dict:
    """Persistenza Postgres/Supabase.

    Usa la service key: lato server scavalca RLS in scrittura. La logica di clustering,
    scoring e ID stabili resta quella gia calcolata nel radar.
    """
    if not SUPABASE_ENABLED:
        reason = "env mancanti" if SUPABASE_URL == "" or SUPABASE_SERVICE_KEY == "" else "supabase-py non installato"
        return {"enabled": False, "ok": False, "reason": reason}
    client = supabase_client()
    if client is None:
        return {"enabled": False, "ok": False, "reason": "client non disponibile"}
    try:
        now = datetime.now(timezone.utc).isoformat()
        articles = []
        clusters = []
        article_cluster_rows = []
        snapshots = []
        scores = []
        seen_articles = set()

        for story in stories:
            cluster_id = story.get("clusterId") or hashlib.sha1(f"cluster::{story.get('title','')}".encode("utf-8", errors="ignore")).hexdigest()[:16]
            cluster_articles = story.get("clusterArticles") or [story]
            clusters.append({
                "id": cluster_id,
                "canonical_title": story.get("title", ""),
                "last_seen_at": now,
                "source_count": int(story.get("sourceCount") or len(story.get("sources", [])) or 1),
                "article_count": int(story.get("articleCount") or len(cluster_articles)),
                "breaking": bool(story.get("breaking")),
                "score": int(story.get("score") or 0),
                "sources": story.get("sources", []),
            })
            snapshots.append({
                "cluster_id": cluster_id,
                "observed_at": now,
                "source_count": int(story.get("sourceCount") or 1),
                "article_count": int(story.get("articleCount") or len(cluster_articles)),
                "score": int(story.get("score") or 0),
                "breaking": bool(story.get("breaking")),
            })
            scores.append({
                "cluster_id": cluster_id,
                "observed_at": now,
                "score": int(story.get("score") or 0),
                "score_abs": int(story.get("scoreAbs") or story.get("score") or 0),
                "velocity": int(story.get("velocity") or 0),
                "acceleration": int(story.get("acceleration") or 0),
                "overlap": int(story.get("overlap") or 0),
                "source_count": int(story.get("sourceCount") or 1),
                "components": story.get("scoreComponents") or {},
            })
            for article in cluster_articles:
                article_id = str(article.get("id") or "")
                if not article_id or article_id in seen_articles:
                    continue
                seen_articles.add(article_id)
                articles.append(article_row(article, now))
                article_cluster_rows.append({"article_id": article_id, "cluster_id": cluster_id})

        chunked_upsert(client, "articles", articles, on_conflict="id")
        chunked_upsert(client, "clusters", clusters, on_conflict="id")
        chunked_upsert(client, "article_cluster", article_cluster_rows, on_conflict="article_id,cluster_id")
        chunked_insert(client, "cluster_snapshots", snapshots)
        chunked_insert(client, "viral_scores", scores)
        return {"enabled": True, "ok": True, "articles": len(articles), "clusters": len(clusters), "observedAt": now}
    except Exception as exc:  # noqa: BLE001
        return {"enabled": True, "ok": False, "error": str(exc)[:240]}


def article_row(story: dict, fetched_at: str) -> dict:
    published_ts = int(story.get("publishedTs") or 0)
    published_at = datetime.fromtimestamp(published_ts, tz=timezone.utc).isoformat() if published_ts else None
    tags = story.get("tags") if isinstance(story.get("tags"), list) else []
    content_basis = f"{story.get('title','')}\n{story.get('summary','')}\n{story.get('url','')}"
    return {
        "id": str(story.get("id")),
        "source_name": story.get("source", ""),
        "title": story.get("title", ""),
        "url": story.get("url", ""),
        "summary": story.get("summary", ""),
        "published_at": published_at,
        "fetched_at": fetched_at,
        "content_hash": hashlib.sha1(content_basis.encode("utf-8", errors="ignore")).hexdigest(),
        "tags": tags,
        "area": story.get("area", ""),
        "category": story.get("category", ""),
        "lang": "it",
        "authority": int(story.get("authority") or 0),
    }


def chunked_upsert(client: Client, table: str, rows: list[dict], on_conflict: str, size: int = 500) -> None:
    for chunk in chunks(rows, size):
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()


def chunked_insert(client: Client, table: str, rows: list[dict], size: int = 500) -> None:
    for chunk in chunks(rows, size):
        client.table(table).insert(chunk).execute()


def chunks(rows: list[dict], size: int) -> list[list[dict]]:
    return [rows[index:index + size] for index in range(0, len(rows), size) if rows[index:index + size]]


def load_rss_payload(limit: int) -> dict:
    stories = []
    errors = []
    feeds = active_feeds()

    if not feeds:
        return {
            "updatedAt": datetime.now().strftime("%H:%M"),
            "totalFeeds": 0,
            "okFeeds": 0,
            "errors": [],
            "stories": [],
            "hotTerms": [],
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(feeds))) as executor:
        futures = [executor.submit(load_feed_stories, feed) for feed in feeds]
        for future in concurrent.futures.as_completed(futures):
            feed_stories, feed_error = future.result()
            stories.extend(feed_stories)
            if feed_error:
                errors.append(feed_error)

    stories = cluster_stories(stories)
    storage_status = persist_to_supabase(stories)
    stories.sort(key=lambda item: (item.get("breaking", False), item["score"], item["publishedTs"]), reverse=True)
    visible_stories = stories[:limit]

    return {
        "updatedAt": datetime.now().strftime("%H:%M"),
        "totalFeeds": len(feeds),
        "okFeeds": len(feeds) - len(errors),
        "errors": sorted(errors, key=lambda item: item["feed"]),
        "stories": visible_stories,
        "breakingCount": sum(1 for story in visible_stories if story.get("breaking")),
        "breakingThreshold": BREAKING_SOURCE_THRESHOLD,
        "hotTerms": extract_hot_terms(visible_stories),
        "storage": storage_status,
    }


def load_feed_stories(feed: dict) -> tuple[list[dict], dict | None]:
    feed_stories = []
    try:
        xml_text = fetch(feed["url"])
        items = parse_feed(xml_text)
        for item in items[:12]:
            story = story_from_item(item, feed)
            if story:
                feed_stories.append(story)
        return feed_stories, None
    except Exception as exc:  # noqa: BLE001 - visible diagnostics for local tool.
        return [], {"feed": feed["name"], "error": str(exc)[:180]}


def fetch(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NewsHubAI-RSS/0.1",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        },
    )
    context = ssl._create_unverified_context()
    with urllib.request.urlopen(request, timeout=9, context=context) as response:
        data = response.read(1_500_000)
    return data.decode("utf-8", errors="replace")


def extract_hot_terms(stories: list[dict], limit: int = 10) -> list[dict]:
    # Livello 1: frequenza istantanea nel corpus corrente.
    # TODO Livello 2: quando saranno disponibili storage/snapshot, confrontare
    # finestra attuale vs baseline recente e calcolare termini "in crescita" con delta %.
    counts: dict[str, int] = {}
    for story in stories:
        text = f"{story.get('title', '')} {' '.join(story.get('tags', []))}"
        for term in candidate_terms(text):
            counts[term] = counts.get(term, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (item[1], len(item[0])), reverse=True)
    return [{"term": term, "count": count} for term, count in ranked[:limit] if count >= 2 or len(ranked) < 6]


def candidate_terms(text: str) -> list[str]:
    cleaned = re.sub(r"https?://\S+", " ", html.unescape(text or "").lower())
    cleaned = re.sub(r"[^\w\s???????-]", " ", cleaned, flags=re.UNICODE)
    words = [word.strip("-_") for word in cleaned.split()]
    terms: list[str] = []
    for word in words:
        if not is_good_term(word):
            continue
        terms.append(TERM_SYNONYMS.get(word, word))
    for left, right in zip(words, words[1:]):
        if is_good_term(left) and is_good_term(right):
            phrase = f"{left} {right}"
            if phrase in {"ponte stretto", "reggio calabria", "protezione civile", "guardia finanza", "polizia stato", "carabinieri nas"}:
                terms.append(TERM_SYNONYMS.get(phrase, phrase))
    # Dedup per singola notizia: conta presenza, non ripetizione nello stesso titolo.
    return sorted(set(terms))


def is_good_term(word: str) -> bool:
    if not word or len(word) < 4:
        return False
    if word.isdigit():
        return False
    translation = str.maketrans({
        "\u00e0": "a", "\u00e8": "e", "\u00e9": "e",
        "\u00ec": "i", "\u00f2": "o", "\u00f9": "u",
    })
    normalized = word.translate(translation)
    if word in TERM_STOPWORDS or normalized in TERM_STOPWORDS:
        return False
    if normalized.startswith("attualit"):
        return False
    if re.match(r"^\d", word):
        return False
    return True


def parse_feed(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text.lstrip())
    items = []

    rss_items = root.findall(".//item")
    if rss_items:
        for item in rss_items:
            items.append(
                {
                    "title": node_text(item, "title"),
                    "link": node_text(item, "link"),
                    "stableRef": node_text(item, "guid"),
                    "summary": node_text(item, "description"),
                    "published": node_text(item, "pubDate"),
                    "categories": [clean_text(child.text or "") for child in item.findall("category")],
                }
            )
        return items

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    for entry in root.findall(".//atom:entry", ns):
        link = ""
        for link_node in entry.findall("atom:link", ns):
            if link_node.get("href"):
                link = link_node.get("href", "")
                break
        items.append(
            {
                "title": node_text(entry, "atom:title", ns),
                "link": link,
                "stableRef": node_text(entry, "atom:id", ns),
                "summary": node_text(entry, "atom:summary", ns) or node_text(entry, "atom:content", ns),
                "published": node_text(entry, "atom:published", ns) or node_text(entry, "atom:updated", ns),
                "categories": [child.get("term", "") for child in entry.findall("atom:category", ns)],
            }
        )
    return items


def story_from_item(item: dict, feed: dict) -> dict | None:
    title = clean_text(item.get("title", ""))
    link = clean_text(item.get("link", ""))
    if not title or not link.startswith("http"):
        return None

    summary = clean_text(item.get("summary", "")) or "Sommario non disponibile dalla fonte RSS."
    published = parse_date(item.get("published", ""))
    age_minutes = max(0, int((datetime.now(timezone.utc) - published).total_seconds() // 60))
    age = format_age(age_minutes)

    text = f"{title} {summary} {' '.join(item.get('categories', []))}".lower()
    area, category = classify_area(text, feed["category"], feed["name"])
    tags = build_tags(text, area, category, item.get("categories", []))
    authority = int(feed.get("authority") or source_authority(feed["name"]))
    components = score_components(
        source_count=1,
        age_minutes=age_minutes,
        avg_authority=authority,
        keyword_count=keyword_hits(text),
        category=category,
        breaking=False,
    )
    score = components["score"]
    velocity = velocity_from_components(score, components, age_minutes)
    acceleration = acceleration_from_components(score, components, len(tags))
    overlap = overlap_from_sources(1)

    return {
        "id": stable_article_id(item.get("stableRef", ""), link, title, feed["name"]),
        "title": title,
        "source": feed["name"],
        "sourceUrl": feed["sourceUrl"],
        "category": category,
        "area": area,
        "age": age,
        "score": score,
        "scoreAbs": score,
        "scoreComponents": components,
        "velocity": velocity,
        "acceleration": acceleration,
        "authority": authority,
        "overlap": overlap,
        "sentiment": sentiment(text),
        "signal": signal_from(score, acceleration),
        "saved": False,
        "read": False,
        "summary": summary[:260],
        "why": why_reasons(text, category, area, age_minutes),
        "tags": tags[:4],
        "history": history_from_score(score),
        "volatility": 1.1 + (keyword_hits(text) * 0.12),
        "url": link,
        "publishedTs": int(published.timestamp()),
        "breaking": False,
        "sourceCount": 1,
        "sources": [feed["name"]],
        "clusterId": "",
        "corroboratingLinks": [],
    }


def stable_article_id(stable_ref: str, url: str, title: str, source: str) -> str:
    stable_ref = clean_text(stable_ref or "")
    canonical = canonicalize_url(url)
    if stable_ref:
        basis = stable_ref
    elif canonical:
        basis = canonical
    else:
        basis = normalize_for_id(title)
    namespaced_basis = f"{normalize_for_id(source)}::{basis}"
    return hashlib.sha1(namespaced_basis.encode("utf-8", errors="ignore")).hexdigest()[:16]


def canonicalize_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit((url or "").strip())
        if not parsed.scheme or not parsed.netloc:
            return ""
        query_pairs = []
        for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
            low = key.lower()
            if low.startswith("utm_") or low in {"fbclid", "gclid", "cmpid", "ref", "referrer"}:
                continue
            query_pairs.append((key, value))
        query = urllib.parse.urlencode(query_pairs, doseq=True)
        return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), query, ""))
    except Exception:
        return ""


def normalize_for_id(value: str) -> str:
    return re.sub(r"\s+", " ", clean_text(value).lower()).strip()


def classify_area(text: str, fallback_category: str, source_name: str = "") -> tuple[str, str]:
    local_calabria_sources = {
        "ANSA Calabria", "ReggioToday", "CatanzaroInforma", "CrotoneNews",
        "Il Crotonese", "Zoom24", "Calabria Diretta News", "CN24", "CityNow",
    }
    local_sicilia_sources = {
        "ANSA Sicilia", "Tempostretto", "StrettoWeb", "Live Sicilia",
        "MeridioNews", "BlogSicilia", "SiciliaNews24", "PalermoToday",
        "CataniaToday", "MessinaToday",
    }

    messina_person_only = is_messina_person_only(text, source_name)

    if has_keyword(text, "reggio calabria") or has_keyword(text, "reggino"):
        return "Reggio Calabria", "Calabria"
    if has_keyword(text, "messina") and not messina_person_only:
        return "Messina", "Sicilia"

    # "Reggio" ? ambiguo a livello nazionale (pu? essere Reggio Emilia),
    # ma su fonti calabresi/locali indica normalmente Reggio Calabria.
    if source_name in local_calabria_sources and any(
        has_keyword(text, value) for value in ("reggio", "reggina", "reggine", "amaranto")
    ):
        return "Reggio Calabria", "Calabria"

    # "Stretto" ? geografico solo in espressioni specifiche, non come parola isolata.
    # Senza una sponda chiara non attribuiamo la notizia a Messina.
    if any(
        phrase in text
        for phrase in (
            "stretto di messina",
            "ponte sullo stretto",
            "ponte dello stretto",
            "area dello stretto",
            "sponde dello stretto",
        )
    ):
        if fallback_category in ("Sicilia", "Calabria"):
            return "Area dello Stretto", fallback_category
        if source_name in local_sicilia_sources:
            return "Area dello Stretto", "Sicilia"
        if source_name in local_calabria_sources:
            return "Area dello Stretto", "Calabria"
        return "Area dello Stretto", "Italia"

    for area, category, keywords in TERRITORIES:
        if area == "Messina" and messina_person_only:
            continue
        if any(has_keyword(text, keyword) for keyword in keywords):
            return area, category
    return fallback_category, fallback_category


def has_keyword(text: str, keyword: str) -> bool:
    pattern = r"(?<!\w)" + re.escape(keyword.lower()) + r"(?!\w)"
    return re.search(pattern, text) is not None


def is_messina_person_only(text: str, source_name: str = "") -> bool:
    # "Messina" può essere un cognome molto frequente nelle news nazionali/sportive:
    # non deve trasformare automaticamente la notizia in cronaca della città.
    if source_name in {"MessinaToday", "Tempostretto"}:
        return False
    person_patterns = (
        "ettore messina",
        "matteo messina denaro",
        "messina denaro",
    )
    geo_patterns = (
        "messina,",
        "messina:",
        "citta di messina",
        "città di messina",
        "comune di messina",
        "provincia di messina",
        "porto di messina",
        "stretto di messina",
        "a messina",
        "da messina",
        "di messina",
    )
    return any(pattern in text for pattern in person_patterns) and not any(
        pattern in text for pattern in geo_patterns
    )


def build_tags(text: str, area: str, category: str, categories: list[str]) -> list[str]:
    tags = []
    for value in [area, category]:
        if value and value not in tags:
            tags.append(value)
    for keyword in TOPIC_KEYWORDS:
        if keyword in text and keyword not in tags:
            tags.append(keyword)
    for category_name in categories:
        cleaned = clean_text(category_name)
        if cleaned and len(cleaned) <= 24 and cleaned not in tags:
            tags.append(cleaned)
    return tags or [category]


def score_components(
    source_count: int,
    age_minutes: int,
    avg_authority: float,
    keyword_count: int,
    category: str,
    breaking: bool = False,
) -> dict:
    source_count = max(1, int(source_count or 1))
    cross_norm = min(1.0, math.log1p(source_count) / math.log1p(MAX_EXPECTED_SOURCES_FOR_SCORE))
    freshness_norm = math.exp(-max(0, age_minutes) / FRESHNESS_HALF_LIFE_MINUTES)
    authority_norm = clamp_float((avg_authority - 60) / 35, 0, 1)
    keyword_norm = min(1.0, math.log1p(max(0, keyword_count)) / math.log1p(5))
    locality_norm = 1.0 if category in ("Sicilia", "Calabria") else 0.35 if category in ("Italia", "Politica") else 0.2
    raw = (
        SCORE_WEIGHTS["base"]
        + SCORE_WEIGHTS["cross_source"] * cross_norm
        + SCORE_WEIGHTS["freshness"] * freshness_norm
        + SCORE_WEIGHTS["authority"] * authority_norm
        + SCORE_WEIGHTS["keywords"] * keyword_norm
        + SCORE_WEIGHTS["locality"] * locality_norm
        + (SCORE_WEIGHTS["breaking"] if breaking else 0)
    )
    score = clamp(raw, 0, 99)
    return {
        "score": score,
        "crossSource": round(cross_norm * 100, 1),
        "freshness": round(freshness_norm * 100, 1),
        "authority": round(authority_norm * 100, 1),
        "keywords": round(keyword_norm * 100, 1),
        "locality": round(locality_norm * 100, 1),
        "raw": round(raw, 2),
    }


def overlap_from_sources(source_count: int) -> int:
    # Fonti incrociate: scala logaritmica, cosi 2/5/12 fonti restano distinguibili.
    return clamp((math.log1p(max(1, source_count)) / math.log1p(20)) * 100, 0, 100)


def velocity_from_components(score: int, components: dict, age_minutes: int) -> int:
    return clamp(score * 0.62 + components.get("freshness", 0) * 0.28 + components.get("crossSource", 0) * 0.10 - min(age_minutes / 90, 12))


def acceleration_from_components(score: int, components: dict, tag_count: int) -> int:
    return clamp(score * 0.56 + components.get("freshness", 0) * 0.24 + components.get("crossSource", 0) * 0.16 + min(tag_count, 5) * 1.5)


def clamp_float(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def viral_score(age_minutes: int, text: str, category: str) -> int:
    # Compatibilita interna: score a fonte singola, non piu formula principale di cluster.
    return score_components(1, age_minutes, 74, keyword_hits(text), category)["score"]


def keyword_hits(text: str) -> int:
    return sum(1 for keyword in TOPIC_KEYWORDS if keyword in text)


def source_authority(source: str) -> int:
    if "ANSA" in source:
        return 94
    if source.startswith("AGI"):
        return 92
    if source in {"Corriere", "Corriere Cronache", "Repubblica", "Repubblica Cronaca", "Sky TG24", "TGCom24"}:
        return 90
    if source in {"Adnkronos Ultima Ora", "Adnkronos Politica", "Il Sole 24 Ore"}:
        return 89
    if source in {"Fanpage", "Il Fatto Quotidiano", "Open", "Today", "TPI", "Wired Italia"}:
        return 84
    if source in {"Tempostretto", "StrettoWeb", "Live Sicilia", "MeridioNews", "BlogSicilia", "SiciliaNews24"}:
        return 82
    if source in {"PalermoToday", "CataniaToday", "MessinaToday", "ReggioToday"}:
        return 80
    if source in {"CatanzaroInforma", "CrotoneNews", "Il Crotonese", "Zoom24", "Calabria Diretta News", "CN24", "CityNow"}:
        return 86
    return 72


def why_reasons(text: str, category: str, area: str, age_minutes: int) -> list[str]:
    reasons = []
    if age_minutes <= 90:
        reasons.append("Notizia fresca: pubblicata o aggiornata da poco nel feed RSS.")
    if category in ("Sicilia", "Calabria"):
        reasons.append(f"Impatto territoriale forte su {area}: utile per monitoraggio locale.")
    if keyword_hits(text) >= 2:
        reasons.append("Contiene piu keyword sensibili che possono accelerare la condivisione.")
    if not reasons:
        reasons.append("Trend stabile: da osservare se viene ripreso da altre fonti.")
    return reasons[:3]


def sentiment(text: str) -> str:
    tense_words = ["morto", "incidente", "allerta", "emergenza", "mafia", "ndrangheta", "arrest", "rifiuti", "maltempo", "scontro"]
    positive_words = ["turismo", "successo", "festival", "cultura", "crescita", "record", "premio"]
    if any(word in text for word in tense_words):
        return "teso"
    if any(word in text for word in positive_words):
        return "positivo"
    return "neutro"


def signal_from(score: int, acceleration: int) -> str:
    if score >= PICCO_SCORE_THRESHOLD:
        return "Picco"
    if score >= BREAKOUT_SCORE_THRESHOLD or acceleration >= 78:
        return "Breakout"
    if score >= OSSERVA_SCORE_THRESHOLD:
        return "Osserva"
    return "Stabile"


def history_from_score(score: int) -> list[int]:
    start = max(18, score - 30)
    return [clamp(start + int((score - start) * step / 6)) for step in range(7)]


def cluster_stories(stories: list[dict]) -> list[dict]:
    clusters: dict[str, list[dict]] = {}
    for story in stories:
        key = story_cluster_key(story.get("title", ""))
        clusters.setdefault(key, []).append(story)

    merged = []
    for key, items in clusters.items():
        # Rappresentante: la versione piu forte/fresca, arricchita con fonti distinte.
        representative = max(items, key=lambda item: (item.get("score", 0), item.get("publishedTs", 0))).copy()
        sources = sorted({item.get("source", "") for item in items if item.get("source")})
        source_count = len(sources)
        article_count = len(items)
        breaking = source_count >= BREAKING_SOURCE_THRESHOLD
        representative["clusterId"] = hashlib.sha1(f"cluster::{key}".encode("utf-8", errors="ignore")).hexdigest()[:16]
        representative["sourceCount"] = source_count
        representative["articleCount"] = article_count
        representative["sources"] = sources
        representative["breaking"] = breaking
        representative["breakingId"] = representative["clusterId"]
        representative["clusterArticles"] = items
        representative["corroboratingLinks"] = [
            {"source": item.get("source", ""), "title": item.get("title", ""), "url": item.get("url", "")}
            for item in sorted(items, key=lambda item: item.get("publishedTs", 0), reverse=True)[:6]
        ]
        newest_ts = max(item.get("publishedTs", 0) for item in items)
        age_minutes = max(0, int((datetime.now(timezone.utc).timestamp() - newest_ts) // 60)) if newest_ts else 9999
        avg_authority = sum(float(item.get("authority", 72)) for item in items) / max(1, len(items))
        keyword_count = max(keyword_hits(f"{item.get('title', '')} {' '.join(item.get('tags', []))}".lower()) for item in items)
        components = score_components(source_count, age_minutes, avg_authority, keyword_count, representative.get("category", "Italia"), breaking)
        representative["score"] = components["score"]
        representative["scoreAbs"] = components["score"]
        representative["scoreComponents"] = components
        representative["authority"] = clamp(avg_authority)
        representative["overlap"] = overlap_from_sources(source_count)
        representative["velocity"] = velocity_from_components(representative["score"], components, age_minutes)
        representative["acceleration"] = acceleration_from_components(representative["score"], components, len(representative.get("tags", [])))
        representative["history"] = history_from_score(representative["score"])
        if breaking:
            representative["signal"] = "BREAKING"
            reasons = representative.get("why", [])
            representative["why"] = [
                f"BREAKING reale: stessa storia ripresa da {source_count} fonti RSS distinte nel fetch corrente.",
                "Proxy di pubblicazione cross-fonte: non misura condivisioni social.",
            ] + reasons
        else:
            representative["signal"] = signal_from(representative["score"], representative["acceleration"])
        merged.append(representative)
    return merged


def story_cluster_key(title: str) -> str:
    normalized = re.sub(r"[^\w\s???????-]", " ", clean_text(title).lower(), flags=re.UNICODE)
    words = []
    for word in normalized.split():
        word = word.strip("-_")
        if len(word) < 4 or word in TERM_STOPWORDS or word.isdigit():
            continue
        words.append(word)
    if not words:
        return re.sub(r"\W+", " ", clean_text(title).lower()).strip()[:90]
    # Conserva l'ordine: abbastanza severo per non fondere storie diverse, ma robusto a punteggiatura/sottotitoli.
    return " ".join(words[:10])


def node_text(node: ET.Element, path: str, ns: dict | None = None) -> str:
    found = node.find(path, ns or {})
    return clean_text(found.text if found is not None and found.text else "")


def clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_date(value: str) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)


def format_age(minutes: int) -> str:
    if minutes < 1:
        return "ora"
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} h"
    return f"{hours // 24} g"


def clamp(value: int | float, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(round(value))))


if __name__ == "__main__":
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", PORT), RadarHandler) as httpd:
        print(f"NewsHub AI RSS attivo: http://127.0.0.1:{PORT}/newshub-ai-radar.html")
        print("Lascia questa finestra aperta mentre usi il radar.")
        httpd.serve_forever()
