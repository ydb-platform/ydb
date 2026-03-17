# -*- coding: utf-8 -*-

from .flask_restful import RestfulPlugin

try:
    from importlib.metadata import version

    __version__ = version("apispec-flask-restful")
except ImportError:
    __version__ = "unknown"
