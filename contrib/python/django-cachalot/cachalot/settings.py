from itertools import chain

from django.apps import apps
from django.conf import settings
from django.utils.module_loading import import_string


SUPPORTED_DATABASE_ENGINES = {
    'django.db.backends.sqlite3',
    'django.db.backends.postgresql',
    'django.db.backends.mysql',

    # GeoDjango
    'django.contrib.gis.db.backends.spatialite',
    'django.contrib.gis.db.backends.postgis',
    'django.contrib.gis.db.backends.mysql',

    # django-transaction-hooks
    'transaction_hooks.backends.sqlite3',
    'transaction_hooks.backends.postgis',
    'transaction_hooks.backends.mysql',

    # django-prometheus wrapped engines
    'django_prometheus.db.backends.sqlite3',
    'django_prometheus.db.backends.postgresql',
    'django_prometheus.db.backends.mysql',
}

SUPPORTED_CACHE_BACKENDS = {
    'django.core.cache.backends.dummy.DummyCache',
    'django.core.cache.backends.locmem.LocMemCache',
    'django.core.cache.backends.filebased.FileBasedCache',
    'django.core.cache.backends.redis.RedisCache',
    'django_redis.cache.RedisCache',
    'django.core.cache.backends.memcached.MemcachedCache',
    'django.core.cache.backends.memcached.PyLibMCCache',
    'django.core.cache.backends.memcached.PyMemcacheCache',
}

SUPPORTED_ONLY = 'supported_only'
ITERABLES = {tuple, list, frozenset, set}


class Settings(object):
    patched = False
    converters = {}

    CACHALOT_ENABLED = True
    CACHALOT_CACHE = 'default'
    CACHALOT_DATABASES = 'supported_only'
    CACHALOT_USE_UNSUPPORTED_DATABASE = False
    CACHALOT_ADDITIONAL_SUPPORTED_DATABASES = {}
    CACHALOT_TIMEOUT = None
    CACHALOT_CACHE_RANDOM = False
    CACHALOT_CACHE_ITERATORS = True
    CACHALOT_INVALIDATE_RAW = True
    CACHALOT_ONLY_CACHABLE_TABLES = ()
    CACHALOT_ONLY_CACHABLE_APPS = ()
    CACHALOT_UNCACHABLE_TABLES = ('django_migrations',)
    CACHALOT_UNCACHABLE_APPS = ()
    CACHALOT_ADDITIONAL_TABLES = ()
    CACHALOT_QUERY_KEYGEN = 'cachalot.utils.get_query_cache_key'
    CACHALOT_TABLE_KEYGEN = 'cachalot.utils.get_table_cache_key'
    CACHALOT_FINAL_SQL_CHECK = False

    @classmethod
    def add_converter(cls, setting):
        def inner(func):
            cls.converters[setting] = func

        return inner

    @classmethod
    def get_names(cls):
        return {name for name in cls.__dict__
                if name[:2] != '__' and name.isupper()}

    def load(self):
        for name in self.get_names():
            value = getattr(settings, name, getattr(self.__class__, name))
            converter = self.converters.get(name)
            if converter is not None:
                value = converter(value)
            setattr(self, name, value)

        if not self.patched:
            from .monkey_patch import patch
            patch()
            self.patched = True

    def unload(self):
        if self.patched:
            from .monkey_patch import unpatch
            unpatch()
            self.patched = False

    def reload(self):
        self.unload()
        self.load()


@Settings.add_converter('CACHALOT_DATABASES')
def convert(value):
    if value == SUPPORTED_ONLY:
        use_unsupported = getattr(settings, 'CACHALOT_USE_UNSUPPORTED_DATABASE', False)
        additional_supported = getattr(settings, 'CACHALOT_ADDITIONAL_SUPPORTED_DATABASES', set())

        if use_unsupported:
            # All databases are enabled
            value = set(settings.DATABASES.keys())
        else:
            # Include databases in SUPPORTED_DATABASE_ENGINES or ADDITIONAL_SUPPORTED_DATABASES
            value = {alias for alias, setting in settings.DATABASES.items()
                     if setting['ENGINE'] in SUPPORTED_DATABASE_ENGINES
                     or setting['ENGINE'] in additional_supported}
    if value.__class__ in ITERABLES:
        return frozenset(value)
    return value


def convert_tables(value, setting_app_name):
    dj_apps = getattr(settings, setting_app_name, ())
    if dj_apps:
        dj_apps = tuple(model._meta.db_table for model in chain.from_iterable(
            apps.all_models[_app].values() for _app in dj_apps
        ))  # Use [] lookup to make sure app is loaded (via INSTALLED_APP's order)
        return frozenset(tuple(value) + dj_apps)
    return frozenset(value)


@Settings.add_converter('CACHALOT_ONLY_CACHABLE_TABLES')
def convert(value):
    return convert_tables(value, 'CACHALOT_ONLY_CACHABLE_APPS')


@Settings.add_converter('CACHALOT_UNCACHABLE_TABLES')
def convert(value):
    return convert_tables(value, 'CACHALOT_UNCACHABLE_APPS')


@Settings.add_converter('CACHALOT_ADDITIONAL_TABLES')
def convert(value):
    return list(value)


@Settings.add_converter('CACHALOT_QUERY_KEYGEN')
def convert(value):
    return import_string(value)


@Settings.add_converter('CACHALOT_TABLE_KEYGEN')
def convert(value):
    return import_string(value)


cachalot_settings = Settings()
