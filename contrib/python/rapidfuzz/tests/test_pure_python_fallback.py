# simple test to ensure the pure Python version is not missing any symbols
from __future__ import annotations

import os

os.environ["RAPIDFUZZ_IMPLEMENTATION"] = "python"
import rapidfuzz  # noqa: F401
