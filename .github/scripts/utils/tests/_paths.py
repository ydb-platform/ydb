"""Product import paths for utils unit tests.

Keep tests out of analytics/dashboard/metrics packages.
"""

from __future__ import annotations

import sys
from pathlib import Path

TESTS_ROOT = Path(__file__).resolve().parent
UTILS = TESTS_ROOT.parent
ANALYTICS = UTILS / "analytics"
DASHBOARD = UTILS / "dashboard"
TEST_METRICS = DASHBOARD / "test_metrics"
METRICS = UTILS / "metrics"


def add_product_paths(*dirs: Path) -> None:
    for directory in dirs:
        path = str(directory)
        if path not in sys.path:
            sys.path.insert(0, path)
