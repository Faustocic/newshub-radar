from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

ENGINE_CANDIDATES = [
    ROOT / "rss_server.py",
    ROOT / "outputs" / "rss_server.py",
    ROOT / "outputs" / "fix-newshub-feed-geografia" / "rss_server.py",
    ROOT / "app-locale" / "rss_server.py",
]


def find_engine() -> Path:
    for candidate in ENGINE_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "rss_server.py non trovato. Percorsi cercati: "
        + ", ".join(str(path.relative_to(ROOT)) for path in ENGINE_CANDIDATES)
    )


def load_engine(path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location("newshub_rss_server", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Impossibile importare il motore: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    engine_path = find_engine()
    engine = load_engine(engine_path)

    if not hasattr(engine, "load_rss_payload"):
        raise AttributeError(f"{engine_path} non espone load_rss_payload(limit)")

    limit = int(os.environ.get("RADAR_LIMIT", "500"))
    payload = engine.load_rss_payload(limit=limit)

    summary = {
        "engine": str(engine_path.relative_to(ROOT)),
        "stories": len(payload.get("stories", [])),
        "totalFeeds": payload.get("totalFeeds"),
        "okFeeds": payload.get("okFeeds"),
        "breakingCount": payload.get("breakingCount"),
        "storage": payload.get("storage"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    storage = payload.get("storage") or {}
    if storage.get("enabled") and not storage.get("ok"):
        raise RuntimeError(f"Supabase storage error: {storage.get('error')}")
    if not storage.get("enabled"):
        print("ATTENZIONE: storage Supabase non attivo. Verifica secret e dipendenze.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
