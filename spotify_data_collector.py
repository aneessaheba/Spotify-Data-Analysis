"""
Spotify Data Collector 
- Authenticates via SpotifyOAuth (env vars required)
- Extracts: saved tracks, user playlists, first playlist's tracks,
  audio features (for saved tracks), and recently played tracks
- Persists each dataset to timestamped JSON files in ./spotify_data
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Optional libs you might use later for transforms
# import pandas as pd

# --------------------------
# Configuration & Constants
# --------------------------
SCOPES = (
    "user-read-private user-read-email user-library-read user-top-read "
    "user-read-recently-played playlist-read-private playlist-read-collaborative"
)
DATA_DIR = Path("spotify_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

AUDIO_FEATURES_BATCH = 100  # API max per call
PLAYLIST_ITEMS_BATCH = 100  # API max per call
SAVED_TRACKS_BATCH = 50     # API default max per call
PLAYLISTS_BATCH = 50        # API default max per call

# --------------------------
# Logging
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("spotify-etl")


# --------------------------
# Utility helpers
# --------------------------
def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def dump_json(payload: Any, prefix: str) -> Path:
    """Write JSON to ./spotify_data/<prefix>_<ts>.json and return the path."""
    fp = DATA_DIR / f"{prefix}_{timestamp()}.json"
    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
    log.info("Saved -> %s", fp)
    return fp


def chunk(seq: List[str], size: int) -> Iterable[List[str]]:
    """Yield successive chunks of length <= size."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# --------------------------
# Core Client
# --------------------------
class SpotifyCollector:
    def __init__(self, scopes: str = SCOPES) -> None:
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scopes))
        me = self.sp.current_user()
        log.info("Authenticated as %s (%s)", me.get("display_name"), me.get("id"))

    # Generic paginator for endpoints that return {'items': [...], 'next': url}
    def _gather_items(self, fn, **kwargs) -> List[Dict[str, Any]]:
        page = fn(**kwargs)
        items = list(page.get("items", []))
        while page.get("next"):
            page = self.sp.next(page)
            items.extend(page.get("items", []))
        return items

    # 1) Saved tracks (library)
    def collect_saved_tracks(self) -> List[Dict[str, Any]]:
        log.info("Fetching saved tracks ...")
        raw = self._gather_items(
            self.sp.current_user_saved_tracks, limit=SAVED_TRACKS_BATCH
        )
        processed: List[Dict[str, Any]] = []
        for row in raw:
            track = row.get("track") or {}
            if not track:
                continue
            processed.append(
                {
                    "added_at": row.get("added_at"),
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "artist_ids": [a.get("id") for a in track.get("artists", [])],
                    "artist_names": [a.get("name") for a in track.get("artists", [])],
                    "album_id": (track.get("album") or {}).get("id"),
                    "album_name": (track.get("album") or {}).get("name"),
                    "duration_ms": track.get("duration_ms"),
                    "popularity": track.get("popularity"),
                    "external_url": (track.get("external_urls") or {}).get("spotify"),
                    "preview_url": track.get("preview_url"),
                    "is_local": track.get("is_local", False),
                }
            )
        dump_json(processed, "saved_tracks")
        log.info("Saved tracks count: %d", len(processed))
        return processed

    # 2) User playlists (metadata only)
    def collect_playlists(self) -> List[Dict[str, Any]]:
        log.info("Fetching user playlists ...")
        raw = self._gather_items(self.sp.current_user_playlists, limit=PLAYLISTS_BATCH)
        processed: List[Dict[str, Any]] = []
        for pl in raw:
            processed.append(
                {
                    "playlist_id": pl.get("id"),
                    "playlist_name": pl.get("name"),
                    "owner_id": (pl.get("owner") or {}).get("id"),
                    "owner_name": (pl.get("owner") or {}).get("display_name"),
                    "description": pl.get("description"),
                    "public": pl.get("public"),
                    "collaborative": pl.get("collaborative"),
                    "track_count": (pl.get("tracks") or {}).get("total"),
                    "snapshot_id": pl.get("snapshot_id"),
                    "external_url": (pl.get("external_urls") or {}).get("spotify"),
                }
            )
        dump_json(processed, "user_playlists")
        log.info("Playlists count: %d", len(processed))
        return processed

    # 3) Tracks from a specific playlist (here: first playlist)
    def collect_first_playlist_tracks(
        self, playlists: List[Dict[str, Any]]
    ) -> Optional[List[Dict[str, Any]]]:
        if not playlists:
            log.warning("No playlists found; skipping playlist item extraction.")
            return None

        first = playlists[0]
        pl_id = first["playlist_id"]
        pl_name = first["playlist_name"]
        log.info("Fetching tracks for playlist: '%s' (%s)", pl_name, pl_id)

        # Paginate playlist items (max 100 per page)
        page = self.sp.playlist_items(pl_id, limit=PLAYLIST_ITEMS_BATCH)
        items = list(page.get("items", []))
        while page.get("next"):
            page = self.sp.next(page)
            items.extend(page.get("items", []))

        processed: List[Dict[str, Any]] = []
        for it in items:
            track = (it or {}).get("track") or {}
            if not track:
                continue
            processed.append(
                {
                    "playlist_id": pl_id,
                    "added_at": it.get("added_at"),
                    "added_by_id": (it.get("added_by") or {}).get("id"),
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "artist_ids": [a.get("id") for a in track.get("artists", [])],
                    "artist_names": [a.get("name") for a in track.get("artists", [])],
                    "album_id": (track.get("album") or {}).get("id"),
                    "album_name": (track.get("album") or {}).get("name"),
                    "duration_ms": track.get("duration_ms"),
                    "popularity": track.get("popularity"),
                    "external_url": (track.get("external_urls") or {}).get("spotify"),
                    "preview_url": track.get("preview_url"),
                    "is_local": track.get("is_local", False),
                }
            )

        dump_json(processed, f"playlist_{pl_id}_tracks")
        log.info("Tracks in first playlist: %d", len(processed))
        return processed

    # 4) Audio features for a set of track IDs (e.g., saved tracks)
    def collect_audio_features_for_tracks(
        self, track_ids: List[str], prefix: str = "saved_tracks_audio_features"
    ) -> List[Dict[str, Any]]:
        valid_ids = [tid for tid in track_ids if tid]
        if not valid_ids:
            log.warning("No valid track IDs for audio features.")
            return []

        log.info("Fetching audio features for %d tracks ...", len(valid_ids))
        all_feats: List[Dict[str, Any]] = []
        for group in chunk(valid_ids, AUDIO_FEATURES_BATCH):
            feats = self.sp.audio_features(group) or []
            # Filter out None entries
            all_feats.extend([f for f in feats if f])

        dump_json(all_feats, prefix)
        log.info("Audio features rows: %d", len(all_feats))
        return all_feats

    # 5) Recently played (last 50)
    def collect_recently_played(self) -> List[Dict[str, Any]]:
        log.info("Fetching recently played tracks (last 50) ...")
        payload = self.sp.current_user_recently_played(limit=50) or {}
        items = payload.get("items", [])
        processed: List[Dict[str, Any]] = []
        for row in items:
            track = (row or {}).get("track") or {}
            processed.append(
                {
                    "played_at": row.get("played_at"),
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "artist_ids": [a.get("id") for a in track.get("artists", [])],
                    "artist_names": [a.get("name") for a in track.get("artists", [])],
                    "album_id": (track.get("album") or {}).get("id"),
                    "album_name": (track.get("album") or {}).get("name"),
                    "context_type": (row.get("context") or {}).get("type"),
                    "context_uri": (row.get("context") or {}).get("uri"),
                }
            )
        dump_json(processed, "recently_played")
        log.info("Recently played count: %d", len(processed))
        return processed


# --------------------------
# Entry point
# --------------------------
def main() -> None:
    # Ensure environment variables exist before attempting auth
    for var in ("SPOTIPY_CLIENT_ID", "SPOTIPY_CLIENT_SECRET", "SPOTIPY_REDIRECT_URI"):
        if not os.getenv(var):
            raise RuntimeError(
                f"Missing environment variable: {var}. "
                "Set it before running this script."
            )

    try:
        client = SpotifyCollector(scopes=SCOPES)
    except Exception as exc:
        log.error("Authentication failed: %s", exc)
        return

    # 1) Library (saved tracks)
    saved_tracks = client.collect_saved_tracks()

    # 2) Playlists
    playlists = client.collect_playlists()

    # 3) First playlist tracks (if any)
    client.collect_first_playlist_tracks(playlists)

    # 4) Audio features for saved tracks (only non-local with valid IDs)
    saved_track_ids = [
        t["track_id"] for t in saved_tracks if t.get("track_id") and not t.get("is_local")
    ]
    client.collect_audio_features_for_tracks(saved_track_ids)

    # 5) Recently played
    client.collect_recently_played()

    log.info("--- Extraction complete ---")
    log.info("All datasets saved in: %s", DATA_DIR.resolve())


if __name__ == "__main__":
    main()
