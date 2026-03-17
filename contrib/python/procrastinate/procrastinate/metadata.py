from __future__ import annotations

import importlib.metadata as importlib_metadata
from collections.abc import Mapping


def extract_metadata() -> Mapping[str, str]:
    metadata = importlib_metadata.metadata("procrastinate")

    return {
        "author": metadata.get("Author", ""),
        "email": metadata.get("Author-email", ""),
        "license": metadata.get("License", ""),
        "url": metadata.get("Home-page", ""),
        "version": metadata.get("Version", ""),
    }
