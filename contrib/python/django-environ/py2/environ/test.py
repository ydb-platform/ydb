from __future__ import print_function
import os
import sys
import unittest
import warnings

import yatest.common

from environ.compat import (
    json, DJANGO_POSTGRES, ImproperlyConfigured, REDIS_DRIVER, quote
)

from environ import Env, Path


class BaseTests(unittest.TestCase):

    URL = 'http://www.google.com/'
    POSTGRES = 'postgres://uf07k1:wegauwhg@ec2-107-21-253-135.compute-1.amazonaws.com:5431/d8r82722'
    MYSQL = 'mysql://bea6eb0:69772142@us-cdbr-east.cleardb.com/heroku_97681?reconnect=true'
    MYSQLGIS = 'mysqlgis://user:password@127.0.0.1/some_database'
    SQLITE = 'sqlite:////full/path/to/your/database/file.sqlite'
    ORACLE_TNS = 'oracle://user:password@sid/'
    ORACLE = 'oracle://user:password@host:1521/sid'
    CUSTOM_BACKEND = 'custom.backend://user:password@example.com:5430/database'
    REDSHIFT = 'redshift://user:password@examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev'
    MEMCACHE = 'memcache://127.0.0.1:11211'
    REDIS = 'rediscache://127.0.0.1:6379/1?client_class=django_redis.client.DefaultClient&password=secret'
    EMAIL = 'smtps://user@domain.com:password@smtp.example.com:587'
    JSON = dict(one='bar', two=2, three=33.44)
    DICT = dict(foo='bar', test='on')
    PATH = '/home/dev'
    EXPORTED = 'exported var'

    @classmethod
    def generateData(cls):
        return dict(STR_VAR='bar',
                    MULTILINE_STR_VAR='foo\\nbar',
                    INT_VAR='42',
                    FLOAT_VAR='33.3',
                    FLOAT_COMMA_VAR='33,3',
                    FLOAT_STRANGE_VAR1='123,420,333.3',
                    FLOAT_STRANGE_VAR2='123.420.333,3',
                    BOOL_TRUE_VAR='1',
                    BOOL_TRUE_VAR2='True',
                    BOOL_FALSE_VAR='0',
                    BOOL_FALSE_VAR2='False',
                    PROXIED_VAR='$STR_VAR',
                    INT_LIST='42,33',
                    INT_TUPLE='(42,33)',
                    STR_LIST_WITH_SPACES=' foo,  bar',
                    EMPTY_LIST='',
                    DICT_VAR='foo=bar,test=on',
                    DATABASE_URL=cls.POSTGRES,
                    DATABASE_MYSQL_URL=cls.MYSQL,
                    DATABASE_MYSQL_GIS_URL=cls.MYSQLGIS,
                    DATABASE_SQLITE_URL=cls.SQLITE,
                    DATABASE_ORACLE_URL=cls.ORACLE,
                    DATABASE_ORACLE_TNS_URL=cls.ORACLE_TNS,
                    DATABASE_REDSHIFT_URL=cls.REDSHIFT,
                    DATABASE_CUSTOM_BACKEND_URL=cls.CUSTOM_BACKEND,
                    CACHE_URL=cls.MEMCACHE,
                    CACHE_REDIS=cls.REDIS,
                    EMAIL_URL=cls.EMAIL,
                    URL_VAR=cls.URL,
                    JSON_VAR=json.dumps(cls.JSON),
                    PATH_VAR=cls.PATH,
                    EXPORTED_VAR=cls.EXPORTED)

    def setUp(self):
        self._old_environ = os.environ
        os.environ = Env.ENVIRON = self.generateData()
        self.env = Env()

    def tearDown(self):
        os.environ = self._old_environ

    def assertTypeAndValue(self, type_, expected, actual):
        self.assertEqual(type_, type(actual))
        self.assertEqual(expected, actual)


class EnvTests(BaseTests):

    def test_not_present_with_default(self):
        self.assertEqual(3, self.env('not_present', default=3))

    def test_not_present_without_default(self):
        self.assertRaises(ImproperlyConfigured, self.env, 'not_present')

    def test_contains(self):
        self.assertTrue('STR_VAR' in self.env)
        self.assertTrue('EMPTY_LIST' in self.env)
        self.assertFalse('I_AM_NOT_A_VAR' in self.env)

    def test_str(self):
        self.assertTypeAndValue(str, 'bar', self.env('STR_VAR'))
        self.assertTypeAndValue(str, 'bar', self.env.str('STR_VAR'))
        self.assertTypeAndValue(str, 'foo\\nbar', self.env.str('MULTILINE_STR_VAR'))
        self.assertTypeAndValue(str, 'foo\nbar', self.env.str('MULTILINE_STR_VAR', multiline=True))

    def test_bytes(self):
        self.assertTypeAndValue(bytes, b'bar', self.env.bytes('STR_VAR'))

    def test_int(self):
        self.assertTypeAndValue(int, 42, self.env('INT_VAR', cast=int))
        self.assertTypeAndValue(int, 42, self.env.int('INT_VAR'))

    def test_int_with_none_default(self):
        self.assertTrue(self.env('NOT_PRESENT_VAR', cast=int, default=None) is None)

    def test_float(self):
        self.assertTypeAndValue(float, 33.3, self.env('FLOAT_VAR', cast=float))
        self.assertTypeAndValue(float, 33.3, self.env.float('FLOAT_VAR'))

        self.assertTypeAndValue(float, 33.3, self.env('FLOAT_COMMA_VAR', cast=float))
        self.assertTypeAndValue(float, 123420333.3, self.env('FLOAT_STRANGE_VAR1', cast=float))
        self.assertTypeAndValue(float, 123420333.3, self.env('FLOAT_STRANGE_VAR2', cast=float))

    def test_bool_true(self):
        self.assertTypeAndValue(bool, True, self.env('BOOL_TRUE_VAR', cast=bool))
        self.assertTypeAndValue(bool, True, self.env('BOOL_TRUE_VAR2', cast=bool))
        self.assertTypeAndValue(bool, True, self.env.bool('BOOL_TRUE_VAR'))

    def test_bool_false(self):
        self.assertTypeAndValue(bool, False, self.env('BOOL_FALSE_VAR', cast=bool))
        self.assertTypeAndValue(bool, False, self.env('BOOL_FALSE_VAR2', cast=bool))
        self.assertTypeAndValue(bool, False, self.env.bool('BOOL_FALSE_VAR'))

    def test_proxied_value(self):
        self.assertEqual('bar', self.env('PROXIED_VAR'))

    def test_int_list(self):
        self.assertTypeAndValue(list, [42, 33], self.env('INT_LIST', cast=[int]))
        self.assertTypeAndValue(list, [42, 33], self.env.list('INT_LIST', int))

    def test_int_tuple(self):
        self.assertTypeAndValue(tuple, (42, 33), self.env('INT_LIST', cast=(int,)))
        self.assertTypeAndValue(tuple, (42, 33), self.env.tuple('INT_LIST', int))
        self.assertTypeAndValue(tuple, ('42', '33'), self.env.tuple('INT_LIST'))

    def test_str_list_with_spaces(self):
        self.assertTypeAndValue(list, [' foo', '  bar'],
                                self.env('STR_LIST_WITH_SPACES', cast=[str]))
        self.assertTypeAndValue(list, [' foo', '  bar'],
                                self.env.list('STR_LIST_WITH_SPACES'))

    def test_empty_list(self):
        self.assertTypeAndValue(list, [], self.env('EMPTY_LIST', cast=[int]))

    def test_dict_value(self):
        self.assertTypeAndValue(dict, self.DICT, self.env.dict('DICT_VAR'))

    def test_dict_parsing(self):

        self.assertEqual({'a': '1'}, self.env.parse_value('a=1', dict))
        self.assertEqual({'a': 1}, self.env.parse_value('a=1', dict(value=int)))
        self.assertEqual({'a': ['1', '2', '3']}, self.env.parse_value('a=1,2,3', dict(value=[str])))
        self.assertEqual({'a': [1, 2, 3]}, self.env.parse_value('a=1,2,3', dict(value=[int])))
        self.assertEqual({'a': 1, 'b': [1.1, 2.2], 'c': 3},
                         self.env.parse_value('a=1;b=1.1,2.2;c=3', dict(value=int, cast=dict(b=[float]))))

        self.assertEqual({'a': "uname", 'c': "http://www.google.com", 'b': True},
                         self.env.parse_value('a=uname;c=http://www.google.com;b=True', dict(value=str, cast=dict(b=bool))))

    def test_url_value(self):
        url = self.env.url('URL_VAR')
        self.assertEqual(url.__class__, self.env.URL_CLASS)
        self.assertEqual(url.geturl(), self.URL)
        self.assertEqual(None, self.env.url('OTHER_URL', default=None))

    def test_url_encoded_parts(self):
        password_with_unquoted_characters = "#password"
        encoded_url = "mysql://user:%s@127.0.0.1:3306/dbname" % quote(password_with_unquoted_characters)
        parsed_url = self.env.db_url_config(encoded_url)
        self.assertEqual(parsed_url['PASSWORD'], password_with_unquoted_characters)

    def test_db_url_value(self):
        pg_config = self.env.db()
        self.assertEqual(pg_config['ENGINE'], DJANGO_POSTGRES)
        self.assertEqual(pg_config['NAME'], 'd8r82722')
        self.assertEqual(pg_config['HOST'], 'ec2-107-21-253-135.compute-1.amazonaws.com')
        self.assertEqual(pg_config['USER'], 'uf07k1')
        self.assertEqual(pg_config['PASSWORD'], 'wegauwhg')
        self.assertEqual(pg_config['PORT'], 5431)

        mysql_config = self.env.db('DATABASE_MYSQL_URL')
        self.assertEqual(mysql_config['ENGINE'], 'django.db.backends.mysql')
        self.assertEqual(mysql_config['NAME'], 'heroku_97681')
        self.assertEqual(mysql_config['HOST'], 'us-cdbr-east.cleardb.com')
        self.assertEqual(mysql_config['USER'], 'bea6eb0')
        self.assertEqual(mysql_config['PASSWORD'], '69772142')
        self.assertEqual(mysql_config['PORT'], '')

        mysql_gis_config = self.env.db('DATABASE_MYSQL_GIS_URL')
        self.assertEqual(mysql_gis_config['ENGINE'], 'django.contrib.gis.db.backends.mysql')
        self.assertEqual(mysql_gis_config['NAME'], 'some_database')
        self.assertEqual(mysql_gis_config['HOST'], '127.0.0.1')
        self.assertEqual(mysql_gis_config['USER'], 'user')
        self.assertEqual(mysql_gis_config['PASSWORD'], 'password')
        self.assertEqual(mysql_gis_config['PORT'], '')

        oracle_config = self.env.db('DATABASE_ORACLE_TNS_URL')
        self.assertEqual(oracle_config['ENGINE'], 'django.db.backends.oracle')
        self.assertEqual(oracle_config['NAME'], 'sid')
        self.assertEqual(oracle_config['HOST'], '')
        self.assertEqual(oracle_config['USER'], 'user')
        self.assertEqual(oracle_config['PASSWORD'], 'password')
        self.assertFalse('PORT' in oracle_config)

        oracle_config = self.env.db('DATABASE_ORACLE_URL')
        self.assertEqual(oracle_config['ENGINE'], 'django.db.backends.oracle')
        self.assertEqual(oracle_config['NAME'], 'sid')
        self.assertEqual(oracle_config['HOST'], 'host')
        self.assertEqual(oracle_config['USER'], 'user')
        self.assertEqual(oracle_config['PASSWORD'], 'password')
        self.assertEqual(oracle_config['PORT'], '1521')

        redshift_config = self.env.db('DATABASE_REDSHIFT_URL')
        self.assertEqual(redshift_config['ENGINE'], 'django_redshift_backend')
        self.assertEqual(redshift_config['NAME'], 'dev')
        self.assertEqual(redshift_config['HOST'], 'examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com')
        self.assertEqual(redshift_config['USER'], 'user')
        self.assertEqual(redshift_config['PASSWORD'], 'password')
        self.assertEqual(redshift_config['PORT'], 5439)

        sqlite_config = self.env.db('DATABASE_SQLITE_URL')
        self.assertEqual(sqlite_config['ENGINE'], 'django.db.backends.sqlite3')
        self.assertEqual(sqlite_config['NAME'], '/full/path/to/your/database/file.sqlite')

        custom_backend_config = self.env.db('DATABASE_CUSTOM_BACKEND_URL')
        self.assertEqual(custom_backend_config['ENGINE'], 'custom.backend')
        self.assertEqual(custom_backend_config['NAME'], 'database')
        self.assertEqual(custom_backend_config['HOST'], 'example.com')
        self.assertEqual(custom_backend_config['USER'], 'user')
        self.assertEqual(custom_backend_config['PASSWORD'], 'password')
        self.assertEqual(custom_backend_config['PORT'], 5430)

    def test_cache_url_value(self):

        cache_config = self.env.cache_url()
        self.assertEqual(cache_config['BACKEND'], 'django.core.cache.backends.memcached.MemcachedCache')
        self.assertEqual(cache_config['LOCATION'], '127.0.0.1:11211')

        redis_config = self.env.cache_url('CACHE_REDIS')
        self.assertEqual(redis_config['BACKEND'], 'django_redis.cache.RedisCache')
        self.assertEqual(redis_config['LOCATION'], 'redis://127.0.0.1:6379/1')
        self.assertEqual(redis_config['OPTIONS'], {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'PASSWORD': 'secret',
        })

    def test_email_url_value(self):

        email_config = self.env.email_url()
        self.assertEqual(email_config['EMAIL_BACKEND'], 'django.core.mail.backends.smtp.EmailBackend')
        self.assertEqual(email_config['EMAIL_HOST'], 'smtp.example.com')
        self.assertEqual(email_config['EMAIL_HOST_PASSWORD'], 'password')
        self.assertEqual(email_config['EMAIL_HOST_USER'], 'user@domain.com')
        self.assertEqual(email_config['EMAIL_PORT'], 587)
        self.assertEqual(email_config['EMAIL_USE_TLS'], True)

    def test_json_value(self):
        self.assertEqual(self.JSON, self.env.json('JSON_VAR'))

    def test_path(self):
        root = self.env.path('PATH_VAR')
        self.assertTypeAndValue(Path, Path(self.PATH), root)

    def test_smart_cast(self):
        self.assertEqual(self.env.get_value('STR_VAR', default='string'), 'bar')
        self.assertEqual(self.env.get_value('BOOL_TRUE_VAR', default=True), True)
        self.assertEqual(self.env.get_value('BOOL_FALSE_VAR', default=True), False)
        self.assertEqual(self.env.get_value('INT_VAR', default=1), 42)
        self.assertEqual(self.env.get_value('FLOAT_VAR', default=1.2), 33.3)

    def test_exported(self):
        self.assertEqual(self.EXPORTED, self.env('EXPORTED_VAR'))


class FileEnvTests(EnvTests):

    def setUp(self):
        super(FileEnvTests, self).setUp()
        Env.ENVIRON = {}
        self.env = Env()
        env_test_path = yatest.common.source_path('contrib/python/django-environ/py2/environ/test_env.txt')
        file_path = Path(env_test_path, is_file=True)('test_env.txt')
        self.env.read_env(file_path, PATH_VAR=Path(env_test_path, is_file=True).__root__)

class SubClassTests(EnvTests):

    def setUp(self):
        super(SubClassTests, self).setUp()
        self.CONFIG = self.generateData()
        class MyEnv(Env):
            ENVIRON = self.CONFIG
        self.env = MyEnv()

    def test_singleton_environ(self):
        self.assertTrue(self.CONFIG is self.env.ENVIRON)


class SchemaEnvTests(BaseTests):

    def test_schema(self):
        env = Env(INT_VAR=int, NOT_PRESENT_VAR=(float, 33.3), STR_VAR=str,
                  INT_LIST=[int], DEFAULT_LIST=([int], [2]))

        self.assertTypeAndValue(int, 42, env('INT_VAR'))
        self.assertTypeAndValue(float, 33.3, env('NOT_PRESENT_VAR'))

        self.assertEqual('bar', env('STR_VAR'))
        self.assertEqual('foo', env('NOT_PRESENT2', default='foo'))

        self.assertTypeAndValue(list, [42, 33], env('INT_LIST'))
        self.assertTypeAndValue(list, [2], env('DEFAULT_LIST'))

        # Override schema in this one case
        self.assertTypeAndValue(str, '42', env('INT_VAR', cast=str))


class DatabaseTestSuite(unittest.TestCase):

    def test_postgres_parsing(self):
        url = 'postgres://uf07k1i6d8ia0v:wegauwhgeuioweg@ec2-107-21-253-135.compute-1.amazonaws.com:5431/d8r82722r2kuvn'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], DJANGO_POSTGRES)
        self.assertEqual(url['NAME'], 'd8r82722r2kuvn')
        self.assertEqual(url['HOST'], 'ec2-107-21-253-135.compute-1.amazonaws.com')
        self.assertEqual(url['USER'], 'uf07k1i6d8ia0v')
        self.assertEqual(url['PASSWORD'], 'wegauwhgeuioweg')
        self.assertEqual(url['PORT'], 5431)

    def test_postgres_parsing_unix_domain_socket(self):
        url = 'postgres:////var/run/postgresql/db'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], DJANGO_POSTGRES)
        self.assertEqual(url['NAME'], 'db')
        self.assertEqual(url['HOST'], '/var/run/postgresql')

    def test_postgis_parsing(self):
        url = 'postgis://uf07k1i6d8ia0v:wegauwhgeuioweg@ec2-107-21-253-135.compute-1.amazonaws.com:5431/d8r82722r2kuvn'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.contrib.gis.db.backends.postgis')
        self.assertEqual(url['NAME'], 'd8r82722r2kuvn')
        self.assertEqual(url['HOST'], 'ec2-107-21-253-135.compute-1.amazonaws.com')
        self.assertEqual(url['USER'], 'uf07k1i6d8ia0v')
        self.assertEqual(url['PASSWORD'], 'wegauwhgeuioweg')
        self.assertEqual(url['PORT'], 5431)

    def test_mysql_gis_parsing(self):
        url = 'mysqlgis://uf07k1i6d8ia0v:wegauwhgeuioweg@ec2-107-21-253-135.compute-1.amazonaws.com:5431/d8r82722r2kuvn'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.contrib.gis.db.backends.mysql')
        self.assertEqual(url['NAME'], 'd8r82722r2kuvn')
        self.assertEqual(url['HOST'], 'ec2-107-21-253-135.compute-1.amazonaws.com')
        self.assertEqual(url['USER'], 'uf07k1i6d8ia0v')
        self.assertEqual(url['PASSWORD'], 'wegauwhgeuioweg')
        self.assertEqual(url['PORT'], 5431)

    def test_cleardb_parsing(self):
        url = 'mysql://bea6eb025ca0d8:69772142@us-cdbr-east.cleardb.com/heroku_97681db3eff7580?reconnect=true'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.db.backends.mysql')
        self.assertEqual(url['NAME'], 'heroku_97681db3eff7580')
        self.assertEqual(url['HOST'], 'us-cdbr-east.cleardb.com')
        self.assertEqual(url['USER'], 'bea6eb025ca0d8')
        self.assertEqual(url['PASSWORD'], '69772142')
        self.assertEqual(url['PORT'], '')

    def test_mysql_no_password(self):
        url = 'mysql://travis@localhost/test_db'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.db.backends.mysql')
        self.assertEqual(url['NAME'], 'test_db')
        self.assertEqual(url['HOST'], 'localhost')
        self.assertEqual(url['USER'], 'travis')
        self.assertEqual(url['PASSWORD'], '')
        self.assertEqual(url['PORT'], '')

    def test_empty_sqlite_url(self):
        url = 'sqlite://'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.db.backends.sqlite3')
        self.assertEqual(url['NAME'], ':memory:')

    def test_memory_sqlite_url(self):
        url = 'sqlite://:memory:'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'django.db.backends.sqlite3')
        self.assertEqual(url['NAME'], ':memory:')
        
    def test_memory_sqlite_url_warns_about_netloc(self):
        url = 'sqlite://missing-slash-path'
        with warnings.catch_warnings(record=True) as w:
            url = Env.db_url_config(url)
            self.assertEqual(url['ENGINE'], 'django.db.backends.sqlite3')
            self.assertEqual(url['NAME'], ':memory:')
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, UserWarning))

    def test_database_options_parsing(self):
        url = 'postgres://user:pass@host:1234/dbname?conn_max_age=600'
        url = Env.db_url_config(url)
        self.assertEqual(url['CONN_MAX_AGE'], 600)

        url = 'postgres://user:pass@host:1234/dbname?conn_max_age=None&autocommit=True&atomic_requests=False'
        url = Env.db_url_config(url)
        self.assertEqual(url['CONN_MAX_AGE'], None)
        self.assertEqual(url['AUTOCOMMIT'], True)
        self.assertEqual(url['ATOMIC_REQUESTS'], False)

        url = 'mysql://user:pass@host:1234/dbname?init_command=SET storage_engine=INNODB'
        url = Env.db_url_config(url)
        self.assertEqual(url['OPTIONS'], {
            'init_command': 'SET storage_engine=INNODB',
        })

    def test_database_ldap_url(self):
        url = 'ldap://cn=admin,dc=nodomain,dc=org:some_secret_password@ldap.nodomain.org/'
        url = Env.db_url_config(url)

        self.assertEqual(url['ENGINE'], 'ldapdb.backends.ldap')
        self.assertEqual(url['HOST'], 'ldap.nodomain.org')
        self.assertEqual(url['PORT'], '')
        self.assertEqual(url['NAME'], 'ldap://ldap.nodomain.org')
        self.assertEqual(url['USER'], 'cn=admin,dc=nodomain,dc=org')
        self.assertEqual(url['PASSWORD'], 'some_secret_password')


class CacheTestSuite(unittest.TestCase):

    def test_base_options_parsing(self):
        url = 'memcache://127.0.0.1:11211/?timeout=0&key_prefix=cache_&key_function=foo.get_key&version=1'
        url = Env.cache_url_config(url)

        self.assertEqual(url['KEY_PREFIX'], 'cache_')
        self.assertEqual(url['KEY_FUNCTION'], 'foo.get_key')
        self.assertEqual(url['TIMEOUT'], 0)
        self.assertEqual(url['VERSION'], 1)

        url = 'redis://127.0.0.1:6379/?timeout=None'
        url = Env.cache_url_config(url)

        self.assertEqual(url['TIMEOUT'], None)

    def test_memcache_parsing(self):
        url = 'memcache://127.0.0.1:11211'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.memcached.MemcachedCache')
        self.assertEqual(url['LOCATION'], '127.0.0.1:11211')

    def test_memcache_pylib_parsing(self):
        url = 'pymemcache://127.0.0.1:11211'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.memcached.PyLibMCCache')
        self.assertEqual(url['LOCATION'], '127.0.0.1:11211')

    def test_memcache_multiple_parsing(self):
        url = 'memcache://172.19.26.240:11211,172.19.26.242:11212'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.memcached.MemcachedCache')
        self.assertEqual(url['LOCATION'], ['172.19.26.240:11211', '172.19.26.242:11212'])

    def test_memcache_socket_parsing(self):
        url = 'memcache:///tmp/memcached.sock'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.memcached.MemcachedCache')
        self.assertEqual(url['LOCATION'], 'unix:/tmp/memcached.sock')

    def test_dbcache_parsing(self):
        url = 'dbcache://my_cache_table'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.db.DatabaseCache')
        self.assertEqual(url['LOCATION'], 'my_cache_table')

    def test_filecache_parsing(self):
        url = 'filecache:///var/tmp/django_cache'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.filebased.FileBasedCache')
        self.assertEqual(url['LOCATION'], '/var/tmp/django_cache')

    def test_filecache_windows_parsing(self):
        url = 'filecache://C:/foo/bar'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.filebased.FileBasedCache')
        self.assertEqual(url['LOCATION'], 'C:/foo/bar')

    def test_locmem_parsing(self):
        url = 'locmemcache://'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.locmem.LocMemCache')
        self.assertEqual(url['LOCATION'], '')

    def test_locmem_named_parsing(self):
        url = 'locmemcache://unique-snowflake'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.locmem.LocMemCache')
        self.assertEqual(url['LOCATION'], 'unique-snowflake')

    def test_dummycache_parsing(self):
        url = 'dummycache://'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.dummy.DummyCache')
        self.assertEqual(url['LOCATION'], '')

    def test_redis_parsing(self):
        url = 'rediscache://127.0.0.1:6379/1?client_class=django_redis.client.DefaultClient&password=secret'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], REDIS_DRIVER)
        self.assertEqual(url['LOCATION'], 'redis://127.0.0.1:6379/1')
        self.assertEqual(url['OPTIONS'], {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'PASSWORD': 'secret',
        })

    def test_redis_socket_parsing(self):
        url = 'rediscache:///path/to/socket:1'
        url = Env.cache_url_config(url)
        self.assertEqual(url['BACKEND'], 'django_redis.cache.RedisCache')
        self.assertEqual(url['LOCATION'], 'unix:///path/to/socket:1')

    def test_redis_with_password_parsing(self):
        url = 'rediscache://:redispass@127.0.0.1:6379/0'
        url = Env.cache_url_config(url)
        self.assertEqual(REDIS_DRIVER, url['BACKEND'])
        self.assertEqual(url['LOCATION'], 'redis://:redispass@127.0.0.1:6379/0')

    def test_redis_multi_location_parsing(self):
        url = 'rediscache://host1:6379,host2:6379,host3:9999/1'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], REDIS_DRIVER)
        self.assertEqual(url['LOCATION'], [
            'redis://host1:6379/1',
            'redis://host2:6379/1',
            'redis://host3:9999/1',
        ])

    def test_redis_socket_url(self):
        url = 'redis://:redispass@/path/to/socket.sock?db=0'
        url = Env.cache_url_config(url)
        self.assertEqual(REDIS_DRIVER, url['BACKEND'])
        self.assertEqual(url['LOCATION'], 'unix://:redispass@/path/to/socket.sock')
        self.assertEqual(url['OPTIONS'], {
            'DB': 0
        })

    def test_options_parsing(self):
        url = 'filecache:///var/tmp/django_cache?timeout=60&max_entries=1000&cull_frequency=0'
        url = Env.cache_url_config(url)

        self.assertEqual(url['BACKEND'], 'django.core.cache.backends.filebased.FileBasedCache')
        self.assertEqual(url['LOCATION'], '/var/tmp/django_cache')
        self.assertEqual(url['TIMEOUT'], 60)
        self.assertEqual(url['OPTIONS'], {
            'MAX_ENTRIES': 1000,
            'CULL_FREQUENCY': 0,
        })

    def test_custom_backend(self):
        url = 'memcache://127.0.0.1:5400?foo=option&bars=9001'
        backend = 'django_redis.cache.RedisCache'
        url = Env.cache_url_config(url, backend)

        self.assertEqual(url['BACKEND'], backend)
        self.assertEqual(url['LOCATION'], '127.0.0.1:5400')
        self.assertEqual(url['OPTIONS'], {
            'FOO': 'option',
            'BARS': 9001,
        })


class SearchTestSuite(unittest.TestCase):

    solr_url = 'solr://127.0.0.1:8983/solr'
    elasticsearch_url = 'elasticsearch://127.0.0.1:9200/index'
    whoosh_url = 'whoosh:///home/search/whoosh_index'
    xapian_url = 'xapian:///home/search/xapian_index'
    simple_url = 'simple:///'

    def test_solr_parsing(self):
        url = Env.search_url_config(self.solr_url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.solr_backend.SolrEngine')
        self.assertEqual(url['URL'], 'http://127.0.0.1:8983/solr')

    def test_solr_multicore_parsing(self):
        timeout = 360
        index = 'solr_index'
        url = '%s/%s?TIMEOUT=%s' % (self.solr_url, index, timeout)
        url = Env.search_url_config(url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.solr_backend.SolrEngine')
        self.assertEqual(url['URL'], 'http://127.0.0.1:8983/solr/solr_index')
        self.assertEqual(url['TIMEOUT'], timeout)
        self.assertTrue('INDEX_NAME' not in url)
        self.assertTrue('PATH' not in url)

    def test_elasticsearch_parsing(self):
        timeout = 360
        url = '%s?TIMEOUT=%s' % (self.elasticsearch_url, timeout)
        url = Env.search_url_config(url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.elasticsearch_backend.ElasticsearchSearchEngine')
        self.assertTrue('INDEX_NAME' in url.keys())
        self.assertEqual(url['INDEX_NAME'], 'index')
        self.assertTrue('TIMEOUT' in url.keys())
        self.assertEqual(url['TIMEOUT'], timeout)
        self.assertTrue('PATH' not in url)

    def test_whoosh_parsing(self):
        storage = 'file'  # or ram
        post_limit = 128 * 1024 * 1024
        url = '%s?STORAGE=%s&POST_LIMIT=%s' % (self.whoosh_url, storage, post_limit)
        url = Env.search_url_config(url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.whoosh_backend.WhooshEngine')
        self.assertTrue('PATH' in url.keys())
        self.assertEqual(url['PATH'], '/home/search/whoosh_index')
        self.assertTrue('STORAGE' in url.keys())
        self.assertEqual(url['STORAGE'], storage)
        self.assertTrue('POST_LIMIT' in url.keys())
        self.assertEqual(url['POST_LIMIT'], post_limit)
        self.assertTrue('INDEX_NAME' not in url)

    def test_xapian_parsing(self):
        flags = 'myflags'
        url = '%s?FLAGS=%s' % (self.xapian_url, flags)
        url = Env.search_url_config(url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.xapian_backend.XapianEngine')
        self.assertTrue('PATH' in url.keys())
        self.assertEqual(url['PATH'], '/home/search/xapian_index')
        self.assertTrue('FLAGS' in url.keys())
        self.assertEqual(url['FLAGS'], flags)
        self.assertTrue('INDEX_NAME' not in url)

    def test_simple_parsing(self):
        url = Env.search_url_config(self.simple_url)

        self.assertEqual(url['ENGINE'], 'haystack.backends.simple_backend.SimpleEngine')
        self.assertTrue('INDEX_NAME' not in url)
        self.assertTrue('PATH' not in url)

    def test_common_args_parsing(self):
        excluded_indexes = 'myapp.indexes.A,myapp.indexes.B'
        include_spelling = 1
        batch_size = 100
        params = 'EXCLUDED_INDEXES=%s&INCLUDE_SPELLING=%s&BATCH_SIZE=%s' % (
            excluded_indexes,
            include_spelling,
            batch_size
        )
        for url in [
            self.solr_url,
            self.elasticsearch_url,
            self.whoosh_url,
            self.xapian_url,
            self.simple_url,
        ]:
            url = '?'.join([url, params])
            url = Env.search_url_config(url)

            self.assertTrue('EXCLUDED_INDEXES' in url.keys())
            self.assertTrue('myapp.indexes.A' in url['EXCLUDED_INDEXES'])
            self.assertTrue('myapp.indexes.B' in url['EXCLUDED_INDEXES'])
            self.assertTrue('INCLUDE_SPELLING'in url.keys())
            self.assertTrue(url['INCLUDE_SPELLING'])
            self.assertTrue('BATCH_SIZE' in url.keys())
            self.assertEqual(url['BATCH_SIZE'], 100)


class EmailTests(unittest.TestCase):

    def test_smtp_parsing(self):
        url = 'smtps://user@domain.com:password@smtp.example.com:587'
        url = Env.email_url_config(url)

        self.assertEqual(url['EMAIL_BACKEND'], 'django.core.mail.backends.smtp.EmailBackend')
        self.assertEqual(url['EMAIL_HOST'], 'smtp.example.com')
        self.assertEqual(url['EMAIL_HOST_PASSWORD'], 'password')
        self.assertEqual(url['EMAIL_HOST_USER'], 'user@domain.com')
        self.assertEqual(url['EMAIL_PORT'], 587)
        self.assertEqual(url['EMAIL_USE_TLS'], True)


class PathTests(unittest.TestCase):

    def test_path_class(self):

        root = Path(__file__, '..', is_file=True)
        root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        self.assertEqual(root(), root_path)
        self.assertEqual(root.__root__, root_path)

        web = root.path('public')
        self.assertEqual(web(), os.path.join(root_path, 'public'))
        self.assertEqual(web('css'), os.path.join(root_path, 'public', 'css'))

    def test_required_path(self):

        self.assertRaises(ImproperlyConfigured, Path, '/not/existing/path/', required=True)
        self.assertRaises(ImproperlyConfigured, Path(__file__), 'not_existing_path', required=True)

    def test_comparison(self):

        self.assertTrue(Path('/home') in Path('/'))
        self.assertTrue(Path('/home') not in Path('/other/dir'))

        self.assertTrue(Path('/home') == Path('/home'))
        self.assertTrue(Path('/home') != Path('/home/dev'))

        self.assertEqual(Path('/home/foo/').rfind('/'), str(Path('/home/foo')).rfind('/'))
        self.assertEqual(Path('/home/foo/').find('/home'), str(Path('/home/foo/')).find('/home'))
        self.assertEqual(Path('/home/foo/')[1], str(Path('/home/foo/'))[1])
        self.assertEqual(Path('/home/foo/').__fspath__(), str(Path('/home/foo/')))

        self.assertEqual(~Path('/home'), Path('/'))
        self.assertEqual(Path('/') + 'home', Path('/home'))
        self.assertEqual(Path('/') + '/home/public', Path('/home/public'))
        self.assertEqual(Path('/home/dev/public') - 2, Path('/home'))
        self.assertEqual(Path('/home/dev/public') - 'public', Path('/home/dev'))

        self.assertRaises(TypeError, lambda _: Path('/home/dev/') - 'not int')


def load_suite():

    test_suite = unittest.TestSuite()
    cases = [
        EnvTests, FileEnvTests, SubClassTests, SchemaEnvTests, PathTests,
        DatabaseTestSuite, CacheTestSuite, EmailTests, SearchTestSuite
    ]
    for case in cases:
        test_suite.addTest(unittest.makeSuite(case))
    return test_suite


if __name__ == "__main__":

    try:
        if sys.argv[1] == '-o':
            for key, value in BaseTests.generateData().items():
                print("{0}={1}".format(key, value))
            sys.exit()
    except IndexError:
        pass

    unittest.TextTestRunner().run(load_suite())
