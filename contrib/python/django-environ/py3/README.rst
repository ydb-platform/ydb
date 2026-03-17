.. raw:: html

    <h1 align="center">django-environ</h1>
    <p align="center">
        <a href="https://pypi.python.org/pypi/django-environ">
            <img src="https://img.shields.io/pypi/v/django-environ.svg" alt="Latest version released on PyPi" />
        </a>
        <a href="https://coveralls.io/github/joke2k/django-environ">
            <img src="https://coveralls.io/repos/github/joke2k/django-environ/badge.svg" alt="Coverage Status" />
        </a>
        <a href="https://github.com/joke2k/django-environ/actions?workflow=CI">
            <img src="https://github.com/joke2k/django-environ/workflows/CI/badge.svg?branch=develop" alt="CI Status" />
        </a>
        <a href="https://opencollective.com/django-environ">
            <img src="https://opencollective.com/django-environ/sponsors/badge.svg" alt="Sponsors on Open Collective" />
        </a>
        <a href="https://opencollective.com/django-environ">
            <img src="https://opencollective.com/django-environ/backers/badge.svg" alt="Backers on Open Collective" />
        </a>
        <a href="https://saythanks.io/to/joke2k">
            <img src="https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg" alt="Say Thanks!" />
        </a>
        <a href="https://raw.githubusercontent.com/joke2k/django-environ/main/LICENSE.txt">
            <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="Package license" />
        </a>
    </p>

.. -teaser-begin-

``django-environ`` is the Python package that allows you to use
`Twelve-factor methodology <https://www.12factor.net/>`_ to configure your
Django application with environment variables.

.. -teaser-end-

For that, it gives you an easy way to configure Django application using
environment variables obtained from an environment file and provided by the OS:

.. -code-begin-

.. code-block:: python

   import environ
   import os

   env = environ.Env(
       # set casting, default value
       DEBUG=(bool, False)
   )

   # Set the project base directory
   BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

   # Take environment variables from .env file
   environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

   # False if not in os.environ because of casting above
   DEBUG = env('DEBUG')

   # Raises Django's ImproperlyConfigured
   # exception if SECRET_KEY not in os.environ
   SECRET_KEY = env('SECRET_KEY')

   # Parse database connection url strings
   # like psql://user:pass@127.0.0.1:8458/db
   DATABASES = {
       # read os.environ['DATABASE_URL'] and raises
       # ImproperlyConfigured exception if not found
       #
       # The db() method is an alias for db_url().
       'default': env.db(),

       # read os.environ['SQLITE_URL']
       'extra': env.db_url(
           'SQLITE_URL',
           default='sqlite:////tmp/my-tmp-sqlite.db'
       )
   }

   CACHES = {
       # Read os.environ['CACHE_URL'] and raises
       # ImproperlyConfigured exception if not found.
       #
       # The cache() method is an alias for cache_url().
       'default': env.cache(),

       # read os.environ['REDIS_URL']
       'redis': env.cache_url('REDIS_URL')
   }

.. -overview-

The idea of this package is to unify a lot of packages that make the same stuff:
Take a string from ``os.environ``, parse and cast it to some of useful python
typed variables. To do that and to use the `12factor <https://www.12factor.net/>`_
approach, some connection strings are expressed as url, so this package can parse
it and return a ``urllib.parse.ParseResult``. These strings from ``os.environ``
are loaded from a ``.env`` file and filled in ``os.environ`` with ``setdefault``
method, to avoid to overwrite the real environ.
A similar approach is used in
`Two Scoops of Django <https://www.feldroy.com/two-scoops-of-django>`_
book and explained in `12factor-django <https://dev.to/ale_jacques/django-drf-12-factor-app-with-examples-36jg>`_
article.


Using ``django-environ`` you can stop to make a lot of unversioned
``settings_*.py`` to configure your app.
See `cookiecutter-django <https://github.com/cookiecutter/cookiecutter-django>`_
for a concrete example on using with a django project.

**Feature Support**

- Fast and easy multi environment for deploy
- Fill ``os.environ`` with .env file variables
- Variables casting
- Url variables exploded to django specific package settings
- Optional support for Docker-style file based config variables (use
  ``environ.FileAwareEnv`` instead of ``environ.Env``)

.. -project-information-

Project Information
===================

``django-environ`` is released under the `MIT / X11 License <https://choosealicense.com/licenses/mit/>`__,
its documentation lives at `Read the Docs <https://django-environ.readthedocs.io/en/latest/>`_,
the code on `GitHub <https://github.com/joke2k/django-environ>`_,
and the latest release on `PyPI <https://pypi.org/project/django-environ/>`_.

It’s rigorously tested on Python 3.9+, and officially supports
Django 2.2, 3.0, 3.1, 3.2, 4.0, 4.1, 4.2, 5.0, 5.1, 5.2, and 6.0.

If you'd like to contribute to ``django-environ`` you're most welcome!

.. -support-

Support
=======

Should you have any question, any remark, or if you find a bug, or if there is
something you can't do with the ``django-environ``, please
`open an issue <https://github.com/joke2k/django-environ>`_.
