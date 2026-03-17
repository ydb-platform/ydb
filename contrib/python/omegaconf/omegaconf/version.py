import sys  # pragma: no cover

__version__ = "2.4.0.dev3"

msg = """OmegaConf 2.4 and above is compatible with Python 3.8 and newer.
You have the following options:
1. Upgrade to Python 3.8 or newer.
   This is highly recommended. new features will not be added to OmegaConf 2.3.
2. Continue using OmegaConf 2.3:
    You can pip install 'OmegaConf<2.4' to do that.
"""

if sys.version_info < (3, 8):
    raise ImportError(msg)  # pragma: no cover
