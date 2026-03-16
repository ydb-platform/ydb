from __future__ import annotations

import importlib.metadata as metadata
from functools import lru_cache


@lru_cache
def collect_package_info() -> dict[str, str]:
    """Retrieve the package information for all installed packages.

    Returns:
        A dicts with the package name and version.
    """
    try:
        distributions = list(metadata.distributions())
        try:
            metas = [dist.metadata for dist in distributions]
            pairs = [(meta['Name'], meta.get('Version', 'UNKNOWN')) for meta in metas if meta.get('Name')]
        except Exception:  # pragma: no cover
            # Just in case `dist.metadata['Name']` stops working but `dist.name` still works,
            # not that this is expected.
            # Currently this is about 2x slower because `dist.name` and `dist.version` each call `dist.metadata`,
            # which reads and parses a file and is not cached.
            pairs = [(dist.name, dist.version) for dist in distributions]
    except Exception:  # pragma: no cover
        # Don't crash for this.
        pairs = []

    return dict(sorted(pairs))
