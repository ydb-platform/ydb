# simple test to ensure the C++ version is not missing any symbols
from __future__ import annotations

import os

os.environ["RAPIDFUZZ_IMPLEMENTATION"] = "cpp"
import rapidfuzz  # noqa: F401
