# -*- coding: utf-8 -*-

"""
environ.compat
~~~~~~~~~~~~~~~
This module handles import compatibility issues between Python 2 and
Python 3.
"""

import sys
import pkgutil

# -------
# Pythons
# -------

# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = (_ver[0] == 2)

#: Python 3.x?
is_py3 = (_ver[0] == 3)

if is_py2:
    import urlparse as urlparselib
    from urllib import quote, unquote_plus

    basestring = basestring

elif is_py3:
    import urllib.parse as urlparselib
    quote = urlparselib.quote
    unquote_plus = urlparselib.unquote_plus

    basestring = str

urlparse = urlparselib.urlparse
urlunparse = urlparselib.urlunparse
ParseResult = urlparselib.ParseResult
parse_qs = urlparselib.parse_qs

if pkgutil.find_loader('simplejson'):
    import simplejson as json
else:
    import json

if pkgutil.find_loader('django'):
    from django import VERSION as DJANGO_VERSION
    from django.core.exceptions import ImproperlyConfigured
else:
    DJANGO_VERSION = None

    class ImproperlyConfigured(Exception):
        pass

# back compatibility with django postgresql package
if DJANGO_VERSION is not None and DJANGO_VERSION < (2, 0):
    DJANGO_POSTGRES = 'django.db.backends.postgresql_psycopg2'
else:
    # https://docs.djangoproject.com/en/2.0/releases/2.0/#id1
    DJANGO_POSTGRES = 'django.db.backends.postgresql'

# back compatibility with redis_cache package
if pkgutil.find_loader('redis_cache'):
    REDIS_DRIVER = 'redis_cache.RedisCache'
else:
    REDIS_DRIVER = 'django_redis.cache.RedisCache'
