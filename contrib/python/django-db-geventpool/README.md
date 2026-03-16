django-db-geventpool
====================

[![CI](https://github.com/jneight/django-db-geventpool/actions/workflows/ci.yml/badge.svg)](https://github.com/jneight/django-db-geventpool/actions/workflows/ci.yml)

[![pypi version](https://img.shields.io/pypi/v/django-db-geventpool.svg)](https://pypi.python.org/pypi/django-db-geventpool)

[![pypi license](http://img.shields.io/pypi/l/django-db-geventpool.svg)](https://pypi.python.org/pypi/django-db-geventpool)

Another DB pool using gevent for PostgreSQL DB.

If **gevent** is not installed, the pool will use **eventlet** as fallback.

psycopg2
--------

django-db-geventpool requires psycopg2:

-   `psycopg2>=2.5.1` for CPython 2 and 3 (or
    [psycopg2-binary](https://pypi.org/project/psycopg2-binary/)---see
    [notes in the psycopg2 2.7.4
    release](http://initd.org/psycopg/articles/2018/02/08/psycopg-274-released/))
-   `psycopg2cffi>=2.7` for PyPy

Patch psycopg2
--------------

Before using the pool, psycopg2 must be patched with psycogreen, if you
are using [gunicorn webserver](http://www.gunicorn.org/), a good place
is the
[post\_fork()](http://docs.gunicorn.org/en/latest/settings.html#post-fork)
function at the config file:

``` {.python}
from psycogreen.gevent import patch_psycopg     # use this if you use gevent workers
from psycogreen.eventlet import patch_psycopg   # use this if you use eventlet workers

def post_fork(server, worker):
    patch_psycopg()
    worker.log.info("Made Psycopg2 Green")
```

Settings
--------

> -
>
>     Set *ENGINE* in your database settings to:
>
>     :   -   *\'django\_db\_geventpool.backends.postgresql\_psycopg2\'*
>         -   For postgis: *\'django\_db\_geventpool.backends.postgis\'*
>
> -   Add *MAX\_CONNS* to *OPTIONS* to set the maximun number of
>     connections allowed to database (default=4)
>
> -   Add *REUSE\_CONNS* to *OPTIONS* to indicate how many of the
>     MAX\_CONNS should be reused by new requests. Will fallback to the
>     same value as MAX\_CONNS if not defined
>
> -   Add *\'CONN\_MAX\_AGE\': 0* to settings to disable default django
>     persistent connection feature. And read below note if you are
>     manually spawning greenlets

``` {.python}
DATABASES = {
    'default': {
        'ENGINE': 'django_db_geventpool.backends.postgresql_psycopg2',
        'NAME': 'db',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'HOST': '',
        'PORT': '',
        'ATOMIC_REQUESTS': False,
        'CONN_MAX_AGE': 0,
        'OPTIONS': {
            'MAX_CONNS': 20,
            'REUSE_CONNS': 10
        }
    }
}
```

Using ORM when not serving requests
-----------------------------------

If you are using django with celery (or other), or have code that
manually spawn greenlets it will not be sufficient to set CONN\_MAX\_AGE
to 0. Django only checks for long-live connections when finishing a
request - So if you manually spawn a greenlet (or task spawning one) its
connections will not get cleaned up and will live until timeout. In
production this can cause quite some open connections and while
developing it can hamper your tests cases.

To solve it make sure that each greenlet function (or task) either sends
the django.core.signals.request\_finished signal or calls
django.db.close\_old\_connections() right before it ends

The decorator method with your function is preferred, but the other
alternatives are also valid

``` {.python}
from django_db_geventpool.utils import close_connection

@close_connection
def foo_func()
     ...
```

or

``` {.python}
from django.core.signals import request_finished

def foo_func():
   ...
   request_finished.send(sender="greenlet")
```

or

``` {.python}
from django.db import close_old_connections

def foo_func():
   ...
   close_old_connections()
```

Other pools
-----------

-   [django-db-pool](https://github.com/gmcguire/django-db-pool)
-   [django-postgresql](https://github.com/kennethreitz/django-postgrespool)
