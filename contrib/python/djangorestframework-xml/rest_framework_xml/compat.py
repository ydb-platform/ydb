"""
The `compat` module provides support for backwards compatibility with older
versions of django/python, and compatibility wrappers around optional packages.
"""
# flake8: noqa


try:
    import defusedxml.ElementTree as etree
except ImportError:
    etree = None
