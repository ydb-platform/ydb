# coding=utf-8

__author__ = """Cristi V."""
__email__ = "cristi@cvjd.me"

try:
    from importlib.metadata import version
except ImportError:  # Python < 3.8
    from importlib_metadata import version

__version__ = version(__name__)
