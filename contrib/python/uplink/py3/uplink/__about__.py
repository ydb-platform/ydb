"""
This module is the single source of truth for any package metadata
that is used both in distribution (i.e., setup.py) and within the
codebase.
"""

import importlib.metadata

__version__ = importlib.metadata.version("uplink")
