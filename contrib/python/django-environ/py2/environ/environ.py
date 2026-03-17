"""
Django-environ allows you to utilize 12factor inspired environment
variables to configure your Django application.
"""
import ast
import logging
import os
import re
import sys
import warnings
from .compat import (
    json, urlparselib, urlparse, urlunparse, ParseResult, parse_qs,
    unquote_plus, DJANGO_POSTGRES, REDIS_DRIVER, ImproperlyConfigured, basestring)

logger = logging.getLogger(__name__)


VERSION = '0.4.5'
__author__ = 'joke2k'
__version__ = tuple(VERSION.split('.'))


def _cast(value):
    # Safely evaluate an expression node or a string containing a Python
    # literal or container display.
    # https://docs.python.org/3/library/ast.html#ast.literal_eval
    try:
        return ast.literal_eval(value)
    except ValueError:
        return value

# return int if possible
def _cast_int(v):
    return int(v) if hasattr(v, 'isdigit') and v.isdigit() else v

def _cast_urlstr(v):
    return unquote_plus(v) if isinstance(v, str) else v


class NoValue(object):

    def __repr__(self):
        return '<{0}>'.format(self.__class__.__name__)


class Env(object):

    """Provide scheme-based lookups of environment variables so that each
    caller doesn't have to pass in `cast` and `default` parameters.

    Usage:::

        env = Env(MAIL_ENABLED=bool, SMTP_LOGIN=(str, 'DEFAULT'))
        if env('MAIL_ENABLED'):
            ...
    """

    ENVIRON = os.environ
    NOTSET = NoValue()
    BOOLEAN_TRUE_STRINGS = ('true', 'on', 'ok', 'y', 'yes', '1')
    URL_CLASS = ParseResult
    DEFAULT_DATABASE_ENV = 'DATABASE_URL'
    DB_SCHEMES = {
        'postgres': DJANGO_POSTGRES,
        'postgresql': DJANGO_POSTGRES,
        'psql': DJANGO_POSTGRES,
        'pgsql': DJANGO_POSTGRES,
        'postgis': 'django.contrib.gis.db.backends.postgis',
        'mysql': 'django.db.backends.mysql',
        'mysql2': 'django.db.backends.mysql',
        'mysql-connector': 'mysql.connector.django',
        'mysqlgis': 'django.contrib.gis.db.backends.mysql',
        'mssql': 'sql_server.pyodbc',
        'oracle': 'django.db.backends.oracle',
        'pyodbc': 'sql_server.pyodbc',
        'redshift': 'django_redshift_backend',
        'spatialite': 'django.contrib.gis.db.backends.spatialite',
        'sqlite': 'django.db.backends.sqlite3',
        'ldap': 'ldapdb.backends.ldap',
    }
    _DB_BASE_OPTIONS = ['CONN_MAX_AGE', 'ATOMIC_REQUESTS', 'AUTOCOMMIT']

    DEFAULT_CACHE_ENV = 'CACHE_URL'
    CACHE_SCHEMES = {
        'dbcache': 'django.core.cache.backends.db.DatabaseCache',
        'dummycache': 'django.core.cache.backends.dummy.DummyCache',
        'filecache': 'django.core.cache.backends.filebased.FileBasedCache',
        'locmemcache': 'django.core.cache.backends.locmem.LocMemCache',
        'memcache': 'django.core.cache.backends.memcached.MemcachedCache',
        'pymemcache': 'django.core.cache.backends.memcached.PyLibMCCache',
        'rediscache': REDIS_DRIVER,
        'redis': REDIS_DRIVER,
    }
    _CACHE_BASE_OPTIONS = ['TIMEOUT', 'KEY_PREFIX', 'VERSION', 'KEY_FUNCTION', 'BINARY']

    DEFAULT_EMAIL_ENV = 'EMAIL_URL'
    EMAIL_SCHEMES = {
        'smtp': 'django.core.mail.backends.smtp.EmailBackend',
        'smtps': 'django.core.mail.backends.smtp.EmailBackend',
        'smtp+tls': 'django.core.mail.backends.smtp.EmailBackend',
        'smtp+ssl': 'django.core.mail.backends.smtp.EmailBackend',
        'consolemail': 'django.core.mail.backends.console.EmailBackend',
        'filemail': 'django.core.mail.backends.filebased.EmailBackend',
        'memorymail': 'django.core.mail.backends.locmem.EmailBackend',
        'dummymail': 'django.core.mail.backends.dummy.EmailBackend'
    }
    _EMAIL_BASE_OPTIONS = ['EMAIL_USE_TLS', 'EMAIL_USE_SSL']

    DEFAULT_SEARCH_ENV = 'SEARCH_URL'
    SEARCH_SCHEMES = {
        "elasticsearch": "haystack.backends.elasticsearch_backend.ElasticsearchSearchEngine",
        "elasticsearch2": "haystack.backends.elasticsearch2_backend.Elasticsearch2SearchEngine",
        "solr": "haystack.backends.solr_backend.SolrEngine",
        "whoosh": "haystack.backends.whoosh_backend.WhooshEngine",
        "xapian": "haystack.backends.xapian_backend.XapianEngine",
        "simple": "haystack.backends.simple_backend.SimpleEngine",
    }

    def __init__(self, **scheme):
        self.scheme = scheme

    def __call__(self, var, cast=None, default=NOTSET, parse_default=False):
        return self.get_value(var, cast=cast, default=default, parse_default=parse_default)

    def __contains__(self, var):
        return var in self.ENVIRON

    # Shortcuts

    def str(self, var, default=NOTSET, multiline=False):
        """
        :rtype: str
        """
        value = self.get_value(var, default=default)
        if multiline:
            return value.replace('\\n', '\n')
        return value

    def unicode(self, var, default=NOTSET):
        """Helper for python2
        :rtype: unicode
        """
        return self.get_value(var, cast=str, default=default)

    def bytes(self, var, default=NOTSET, encoding='utf8'):
        """
        :rtype: bytes
        """
        return self.get_value(var, cast=str).encode(encoding)

    def bool(self, var, default=NOTSET):
        """
        :rtype: bool
        """
        return self.get_value(var, cast=bool, default=default)

    def int(self, var, default=NOTSET):
        """
        :rtype: int
        """
        return self.get_value(var, cast=int, default=default)

    def float(self, var, default=NOTSET):
        """
        :rtype: float
        """
        return self.get_value(var, cast=float, default=default)

    def json(self, var, default=NOTSET):
        """
        :returns: Json parsed
        """
        return self.get_value(var, cast=json.loads, default=default)

    def list(self, var, cast=None, default=NOTSET):
        """
        :rtype: list
        """
        return self.get_value(var, cast=list if not cast else [cast], default=default)

    def tuple(self, var, cast=None, default=NOTSET):
        """
        :rtype: tuple
        """
        return self.get_value(var, cast=tuple if not cast else (cast,), default=default)

    def dict(self, var, cast=dict, default=NOTSET):
        """
        :rtype: dict
        """
        return self.get_value(var, cast=cast, default=default)

    def url(self, var, default=NOTSET):
        """
        :rtype: urlparse.ParseResult
        """
        return self.get_value(var, cast=urlparse, default=default, parse_default=True)

    def db_url(self, var=DEFAULT_DATABASE_ENV, default=NOTSET, engine=None):
        """Returns a config dictionary, defaulting to DATABASE_URL.

        :rtype: dict
        """
        return self.db_url_config(self.get_value(var, default=default), engine=engine)
    db = db_url

    def cache_url(self, var=DEFAULT_CACHE_ENV, default=NOTSET, backend=None):
        """Returns a config dictionary, defaulting to CACHE_URL.

        :rtype: dict
        """
        return self.cache_url_config(self.url(var, default=default), backend=backend)
    cache = cache_url

    def email_url(self, var=DEFAULT_EMAIL_ENV, default=NOTSET, backend=None):
        """Returns a config dictionary, defaulting to EMAIL_URL.

        :rtype: dict
        """
        return self.email_url_config(self.url(var, default=default), backend=backend)
    email = email_url

    def search_url(self, var=DEFAULT_SEARCH_ENV, default=NOTSET, engine=None):
        """Returns a config dictionary, defaulting to SEARCH_URL.

        :rtype: dict
        """
        return self.search_url_config(self.url(var, default=default), engine=engine)

    def path(self, var, default=NOTSET, **kwargs):
        """
        :rtype: Path
        """
        return Path(self.get_value(var, default=default), **kwargs)

    def get_value(self, var, cast=None, default=NOTSET, parse_default=False):
        """Return value for given environment variable.

        :param var: Name of variable.
        :param cast: Type to cast return value as.
        :param default: If var not present in environ, return this instead.
        :param parse_default: force to parse default..

        :returns: Value from environment or default (if set)
        """

        logger.debug("get '{0}' casted as '{1}' with default '{2}'".format(
            var, cast, default
        ))

        if var in self.scheme:
            var_info = self.scheme[var]

            try:
                has_default = len(var_info) == 2
            except TypeError:
                has_default = False

            if has_default:
                if not cast:
                    cast = var_info[0]

                if default is self.NOTSET:
                    try:
                        default = var_info[1]
                    except IndexError:
                        pass
            else:
                if not cast:
                    cast = var_info

        try:
            value = self.ENVIRON[var]
        except KeyError:
            if default is self.NOTSET:
                error_msg = "Set the {0} environment variable".format(var)
                raise ImproperlyConfigured(error_msg)

            value = default

        # Resolve any proxied values
        if hasattr(value, 'startswith') and value.startswith('$'):
            value = value.lstrip('$')
            value = self.get_value(value, cast=cast, default=default)

        if cast is None and default is not None and not isinstance(default, NoValue):
            cast = type(default)

        if value != default or (parse_default and value):
            value = self.parse_value(value, cast)

        return value

    # Class and static methods

    @classmethod
    def parse_value(cls, value, cast):
        """Parse and cast provided value

        :param value: Stringed value.
        :param cast: Type to cast return value as.

        :returns: Casted value
        """
        if cast is None:
            return value
        elif cast is bool:
            try:
                value = int(value) != 0
            except ValueError:
                value = value.lower() in cls.BOOLEAN_TRUE_STRINGS
        elif isinstance(cast, list):
            value = list(map(cast[0], [x for x in value.split(',') if x]))
        elif isinstance(cast, tuple):
            val = value.strip('(').strip(')').split(',')
            value = tuple(map(cast[0], [x for x in val if x]))
        elif isinstance(cast, dict):
            key_cast = cast.get('key', str)
            value_cast = cast.get('value', str)
            value_cast_by_key = cast.get('cast', dict())
            value = dict(map(
                lambda kv: (
                    key_cast(kv[0]),
                    cls.parse_value(kv[1], value_cast_by_key.get(kv[0], value_cast))
                ),
                [val.split('=') for val in value.split(';') if val]
            ))
        elif cast is dict:
            value = dict([val.split('=') for val in value.split(',') if val])
        elif cast is list:
            value = [x for x in value.split(',') if x]
        elif cast is tuple:
            val = value.strip('(').strip(')').split(',')
            value = tuple([x for x in val if x])
        elif cast is float:
            # clean string
            float_str = re.sub(r'[^\d,\.]', '', value)
            # split for avoid thousand separator and different locale comma/dot symbol
            parts = re.split(r'[,\.]', float_str)
            if len(parts) == 1:
                float_str = parts[0]
            else:
                float_str = "{0}.{1}".format(''.join(parts[0:-1]), parts[-1])
            value = float(float_str)
        else:
            value = cast(value)
        return value

    @classmethod
    def db_url_config(cls, url, engine=None):
        """Pulled from DJ-Database-URL, parse an arbitrary Database URL.
        Support currently exists for PostgreSQL, PostGIS, MySQL, Oracle and SQLite.

        SQLite connects to file based databases. The same URL format is used, omitting the hostname,
        and using the "file" portion as the filename of the database.
        This has the effect of four slashes being present for an absolute file path:

        >>> from environ import Env
        >>> Env.db_url_config('sqlite:////full/path/to/your/file.sqlite')
        {'ENGINE': 'django.db.backends.sqlite3', 'HOST': '', 'NAME': '/full/path/to/your/file.sqlite', 'PASSWORD': '', 'PORT': '', 'USER': ''}
        >>> Env.db_url_config('postgres://uf07k1i6d8ia0v:wegauwhgeuioweg@ec2-107-21-253-135.compute-1.amazonaws.com:5431/d8r82722r2kuvn')
        {'ENGINE': 'django.db.backends.postgresql', 'HOST': 'ec2-107-21-253-135.compute-1.amazonaws.com', 'NAME': 'd8r82722r2kuvn', 'PASSWORD': 'wegauwhgeuioweg', 'PORT': 5431, 'USER': 'uf07k1i6d8ia0v'}

        """
        if not isinstance(url, cls.URL_CLASS):
            if url == 'sqlite://:memory:':
                # this is a special case, because if we pass this URL into
                # urlparse, urlparse will choke trying to interpret "memory"
                # as a port number
                return {
                    'ENGINE': cls.DB_SCHEMES['sqlite'],
                    'NAME': ':memory:'
                }
                # note: no other settings are required for sqlite
            url = urlparse(url)

        config = {}

        # Remove query strings.
        path = url.path[1:]
        path = unquote_plus(path.split('?', 2)[0])

        if url.scheme == 'sqlite':
            if path == '':
                # if we are using sqlite and we have no path, then assume we
                # want an in-memory database (this is the behaviour of  sqlalchemy)
                path = ':memory:'
            if url.netloc:
                warnings.warn(
                    'SQLite URL contains host component %r, it will be ignored' % url.netloc, stacklevel=3)
        if url.scheme == 'ldap':
            path = '{scheme}://{hostname}'.format(scheme=url.scheme, hostname=url.hostname)
            if url.port:
                path += ':{port}'.format(port=url.port)

        # Update with environment configuration.
        config.update({
            'NAME': path or '',
            'USER': _cast_urlstr(url.username) or '',
            'PASSWORD': _cast_urlstr(url.password) or '',
            'HOST': url.hostname or '',
            'PORT': _cast_int(url.port) or '',
        })

        if url.scheme == 'postgres' and path.startswith('/'):
            config['HOST'], config['NAME'] = path.rsplit('/', 1)

        if url.scheme == 'oracle' and path == '':
            config['NAME'] = config['HOST']
            config['HOST'] = ''

        if url.scheme == 'oracle':
            # Django oracle/base.py strips port and fails on non-string value
            if not config['PORT']:
                del(config['PORT'])
            else:
                config['PORT'] = str(config['PORT'])

        if url.query:
            config_options = {}
            for k, v in parse_qs(url.query).items():
                if k.upper() in cls._DB_BASE_OPTIONS:
                    config.update({k.upper(): _cast(v[0])})
                else:
                    config_options.update({k: _cast_int(v[0])})
            config['OPTIONS'] = config_options

        if engine:
            config['ENGINE'] = engine
        else:
            config['ENGINE'] = url.scheme

        if config['ENGINE'] in Env.DB_SCHEMES:
            config['ENGINE'] = Env.DB_SCHEMES[config['ENGINE']]

        if not config.get('ENGINE', False):
            warnings.warn("Engine not recognized from url: {0}".format(config))
            return {}

        return config

    @classmethod
    def cache_url_config(cls, url, backend=None):
        """Pulled from DJ-Cache-URL, parse an arbitrary Cache URL.

        :param url:
        :param backend:
        :return:
        """
        url = urlparse(url) if not isinstance(url, cls.URL_CLASS) else url

        location = url.netloc.split(',')
        if len(location) == 1:
            location = location[0]

        config = {
            'BACKEND': cls.CACHE_SCHEMES[url.scheme],
            'LOCATION': location,
        }

        # Add the drive to LOCATION
        if url.scheme == 'filecache':
            config.update({
                'LOCATION': url.netloc + url.path,
            })

        if url.path and url.scheme in ['memcache', 'pymemcache']:
            config.update({
                'LOCATION': 'unix:' + url.path,
            })
        elif url.scheme.startswith('redis'):
            if url.hostname:
                scheme = url.scheme.replace('cache', '')
            else:
                scheme = 'unix'
            locations = [scheme + '://' + loc + url.path for loc in url.netloc.split(',')]
            config['LOCATION'] = locations[0] if len(locations) == 1 else locations

        if url.query:
            config_options = {}
            for k, v in parse_qs(url.query).items():
                opt = {k.upper(): _cast(v[0])}
                if k.upper() in cls._CACHE_BASE_OPTIONS:
                    config.update(opt)
                else:
                    config_options.update(opt)
            config['OPTIONS'] = config_options

        if backend:
            config['BACKEND'] = backend

        return config

    @classmethod
    def email_url_config(cls, url, backend=None):
        """Parses an email URL."""

        config = {}

        url = urlparse(url) if not isinstance(url, cls.URL_CLASS) else url

        # Remove query strings
        path = url.path[1:]
        path = unquote_plus(path.split('?', 2)[0])

        # Update with environment configuration
        config.update({
            'EMAIL_FILE_PATH': path,
            'EMAIL_HOST_USER': _cast_urlstr(url.username),
            'EMAIL_HOST_PASSWORD': _cast_urlstr(url.password),
            'EMAIL_HOST': url.hostname,
            'EMAIL_PORT': _cast_int(url.port),
        })

        if backend:
            config['EMAIL_BACKEND'] = backend
        elif url.scheme not in cls.EMAIL_SCHEMES:
            raise ImproperlyConfigured('Invalid email schema %s' % url.scheme)
        elif url.scheme in cls.EMAIL_SCHEMES:
            config['EMAIL_BACKEND'] = cls.EMAIL_SCHEMES[url.scheme]

        if url.scheme in ('smtps', 'smtp+tls'):
            config['EMAIL_USE_TLS'] = True
        elif url.scheme == 'smtp+ssl':
            config['EMAIL_USE_SSL'] = True

        if url.query:
            config_options = {}
            for k, v in parse_qs(url.query).items():
                opt = {k.upper(): _cast_int(v[0])}
                if k.upper() in cls._EMAIL_BASE_OPTIONS:
                    config.update(opt)
                else:
                    config_options.update(opt)
            config['OPTIONS'] = config_options

        return config

    @classmethod
    def search_url_config(cls, url, engine=None):
        config = {}

        url = urlparse(url) if not isinstance(url, cls.URL_CLASS) else url

        # Remove query strings.
        path = url.path[1:]
        path = unquote_plus(path.split('?', 2)[0])

        if url.scheme not in cls.SEARCH_SCHEMES:
            raise ImproperlyConfigured('Invalid search schema %s' % url.scheme)
        config["ENGINE"] = cls.SEARCH_SCHEMES[url.scheme]

        # check commons params
        params = {}
        if url.query:
            params = parse_qs(url.query)
            if 'EXCLUDED_INDEXES' in params.keys():
                config['EXCLUDED_INDEXES'] = params['EXCLUDED_INDEXES'][0].split(',')
            if 'INCLUDE_SPELLING' in params.keys():
                config['INCLUDE_SPELLING'] = cls.parse_value(params['INCLUDE_SPELLING'][0], bool)
            if 'BATCH_SIZE' in params.keys():
                config['BATCH_SIZE'] = cls.parse_value(params['BATCH_SIZE'][0], int)

        if url.scheme == 'simple':
            return config
        elif url.scheme in ['solr', 'elasticsearch', 'elasticsearch2']:
            if 'KWARGS' in params.keys():
                config['KWARGS'] = params['KWARGS'][0]

        # remove trailing slash
        if path.endswith("/"):
            path = path[:-1]

        if url.scheme == 'solr':
            config['URL'] = urlunparse(('http',) + url[1:2] + (path,) + ('', '', ''))
            if 'TIMEOUT' in params.keys():
                config['TIMEOUT'] = cls.parse_value(params['TIMEOUT'][0], int)
            return config

        if url.scheme in ['elasticsearch', 'elasticsearch2']:

            split = path.rsplit("/", 1)

            if len(split) > 1:
                path = "/".join(split[:-1])
                index = split[-1]
            else:
                path = ""
                index = split[0]

            config['URL'] = urlunparse(('http',) + url[1:2] + (path,) + ('', '', ''))
            if 'TIMEOUT' in params.keys():
                config['TIMEOUT'] = cls.parse_value(params['TIMEOUT'][0], int)
            config['INDEX_NAME'] = index
            return config

        config['PATH'] = '/' + path

        if url.scheme == 'whoosh':
            if 'STORAGE' in params.keys():
                config['STORAGE'] = params['STORAGE'][0]
            if 'POST_LIMIT' in params.keys():
                config['POST_LIMIT'] = cls.parse_value(params['POST_LIMIT'][0], int)
        elif url.scheme == 'xapian':
            if 'FLAGS' in params.keys():
                config['FLAGS'] = params['FLAGS'][0]

        if engine:
            config['ENGINE'] = engine

        return config

    @classmethod
    def read_env(cls, env_file=None, **overrides):
        """Read a .env file into os.environ.

        If not given a path to a dotenv path, does filthy magic stack backtracking
        to find manage.py and then find the dotenv.

        http://www.wellfireinteractive.com/blog/easier-12-factor-django/

        https://gist.github.com/bennylope/2999704
        """
        if env_file is None:
            frame = sys._getframe()
            env_file = os.path.join(os.path.dirname(frame.f_back.f_code.co_filename), '.env')
            if not os.path.exists(env_file):
                warnings.warn(
                    "%s doesn't exist - if you're not configuring your "
                    "environment separately, create one." % env_file)
                return

        try:
            with open(env_file) if isinstance(env_file, basestring) else env_file as f:
                content = f.read()
        except IOError:
            warnings.warn(
                "Error reading %s - if you're not configuring your "
                "environment separately, check this." % env_file)
            return

        logger.debug('Read environment variables from: {0}'.format(env_file))

        for line in content.splitlines():
            m1 = re.match(r'\A(?:export )?([A-Za-z_0-9]+)=(.*)\Z', line)
            if m1:
                key, val = m1.group(1), m1.group(2)
                m2 = re.match(r"\A'(.*)'\Z", val)
                if m2:
                    val = m2.group(1)
                m3 = re.match(r'\A"(.*)"\Z', val)
                if m3:
                    val = re.sub(r'\\(.)', r'\1', m3.group(1))
                cls.ENVIRON.setdefault(key, str(val))

        # set defaults
        for key, value in overrides.items():
            cls.ENVIRON.setdefault(key, value)


class Path(object):

    """Inspired to Django Two-scoops, handling File Paths in Settings.

        >>> from environ import Path
        >>> root = Path('/home')
        >>> root, root(), root('dev')
        (<Path:/home>, '/home', '/home/dev')
        >>> root == Path('/home')
        True
        >>> root in Path('/'), root not in Path('/other/path')
        (True, True)
        >>> root('dev', 'not_existing_dir', required=True)
        Traceback (most recent call last):
        environ.environ.ImproperlyConfigured: Create required path: /home/not_existing_dir
        >>> public = root.path('public')
        >>> public, public.root, public('styles')
        (<Path:/home/public>, '/home/public', '/home/public/styles')
        >>> assets, scripts = public.path('assets'), public.path('assets', 'scripts')
        >>> assets.root, scripts.root
        ('/home/public/assets', '/home/public/assets/scripts')
        >>> assets + 'styles', str(assets + 'styles'), ~assets
        (<Path:/home/public/assets/styles>, '/home/public/assets/styles', <Path:/home/public>)

    """

    def path(self, *paths, **kwargs):
        """Create new Path based on self.root and provided paths.

        :param paths: List of sub paths
        :param kwargs: required=False
        :rtype: Path
        """
        return self.__class__(self.__root__, *paths, **kwargs)

    def file(self, name, *args, **kwargs):
        """Open a file.

        :param name: Filename appended to self.root
        :param args: passed to open()
        :param kwargs: passed to open()

        :rtype: file
        """
        return open(self(name), *args, **kwargs)

    @property
    def root(self):
        """Current directory for this Path"""
        return self.__root__

    def __init__(self, start='', *paths, **kwargs):

        super(Path, self).__init__()

        if kwargs.get('is_file', False):
            start = os.path.dirname(start)

        self.__root__ = self._absolute_join(start, *paths, **kwargs)

    def __call__(self, *paths, **kwargs):
        """Retrieve the absolute path, with appended paths

        :param paths: List of sub path of self.root
        :param kwargs: required=False
        """
        return self._absolute_join(self.__root__, *paths, **kwargs)

    def __eq__(self, other):
        return self.__root__ == other.__root__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        return Path(self.__root__, other if not isinstance(other, Path) else other.__root__)

    def __sub__(self, other):
        if isinstance(other, int):
            return self.path('../' * other)
        elif isinstance(other, basestring):
            if self.__root__.endswith(other):
                return Path(self.__root__.rstrip(other))
        raise TypeError(
            "unsupported operand type(s) for -: '{self}' and '{other}' "
            "unless value of {self} ends with value of {other}".format(
                self=type(self), other=type(other)
            )
        )

    def __invert__(self):
        return self.path('..')

    def __contains__(self, item):
        base_path = self.__root__
        if len(base_path) > 1:
            base_path = os.path.join(base_path, '')
        return item.__root__.startswith(base_path)

    def __repr__(self):
        return "<Path:{0}>".format(self.__root__)

    def __str__(self):
        return self.__root__

    def __unicode__(self):
        return self.__str__()

    def __getitem__(self, *args, **kwargs):
        return self.__str__().__getitem__(*args, **kwargs)
    
    def __fspath__(self):
        return self.__str__()

    def rfind(self, *args, **kwargs):
        return self.__str__().rfind(*args, **kwargs)

    def find(self, *args, **kwargs):
        return self.__str__().find(*args, **kwargs)

    @staticmethod
    def _absolute_join(base, *paths, **kwargs):
        absolute_path = os.path.abspath(os.path.join(base, *paths))
        if kwargs.get('required', False) and not os.path.exists(absolute_path):
            raise ImproperlyConfigured(
                "Create required path: {0}".format(absolute_path))
        return absolute_path


def register_scheme(scheme):
    for method in dir(urlparselib):
        if method.startswith('uses_'):
            getattr(urlparselib, method).append(scheme)


def register_schemes(schemes):
    for scheme in schemes:
        register_scheme(scheme)


# Register database and cache schemes in URLs.
register_schemes(Env.DB_SCHEMES.keys())
register_schemes(Env.CACHE_SCHEMES.keys())
register_schemes(Env.SEARCH_SCHEMES.keys())
register_schemes(Env.EMAIL_SCHEMES.keys())
