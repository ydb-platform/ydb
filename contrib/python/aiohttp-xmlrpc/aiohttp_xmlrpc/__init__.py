import sys


try:
    __import__("__pypy__")
    IS_PYPY = True
except ImportError:
    IS_PYPY = False

author_info = [
    ("Dmitry Orlov", "me@mosquito.su"),
]

version_info = (1, 5, 0)
description = "aiohttp XML-RPC server handler and client"

__version__ = ".".join(map(str, version_info))
__author__ = ", ".join("{0} <{1}>".format(*author) for author in author_info)
__pyversion__ = ".".join(map(str, sys.version_info))
__license__ = "MIT"
