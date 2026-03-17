from django.template.defaultfilters import slugify

from .settings import get_cache_backend

# Stripped down version of caching functions from django-dbtemplates
# https://github.com/jezdez/django-dbtemplates/blob/develop/dbtemplates/utils/cache.py
cache_backend = get_cache_backend()


def get_cache_key(name):
    """
    Prefixes and slugify the key name
    """
    return 'post_office:template:%s' % (slugify(name))


def set(name, content):
    return cache_backend.set(get_cache_key(name), content)


def get(name):
    return cache_backend.get(get_cache_key(name))


def delete(name):
    return cache_backend.delete(get_cache_key(name))
