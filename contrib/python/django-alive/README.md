# django-alive 🕺

[![tests](https://github.com/lincolnloop/django-alive/actions/workflows/tox.yml/badge.svg)](https://github.com/lincolnloop/django-alive/actions/workflows/tox.yml)
[![coverage](https://img.shields.io/codacy/coverage/5d539d4956a44f55aec632f3a43ee6c1.svg)](https://app.codacy.com/project/ipmb/django-alive/dashboard)
[![PyPI](https://img.shields.io/pypi/v/django-alive.svg)](https://pypi.org/project/django-alive/)
![Python Versions](https://img.shields.io/pypi/pyversions/django-alive.svg)

Provides two healthcheck endpoints for your Django application:

### Alive

Verifies the WSGI server is responding.

* Default URL: `/-/alive/`
* Success:
    * status code: `200`
    * content: `ok`
* Failure: This view never returns a failure. A failure would mean your WSGI server is not running.

### Health

Verifies services are ready.

* Default URL: `/-/health/`
* Success:
    * status_code: `200`
    * content: `{"healthy": true}`
* Failure:
    * status_code: `503`
    * content: `{"healthy": false, "errors": ["error 1", "error 2"]}`

By default the health endpoint will test the database connection, but can be configured to check the cache, staticfiles, or any additional custom checks.

Supports Django 3.2+ on Python 3.6+.

## Install

```
pip install django-alive
```

## Configure

Add this to your project's `urlpatterns`:

```python
path("-/", include("django_alive.urls"))
```


If you wish to use the `healthcheck` [management command](#management-command), add
`django_alive` to the `INSTALLED_APPS`.

## Enabling Checks

The default "health" endpoint will test a simple `SELECT 1` query on the database. Additional checks can be enabled in your Django settings.

Use the `ALIVE_CHECKS` setting to configure the checks to include. It is a list of tuples with the path to a Python function as a first argiment and dict of keyword arguments to pass to that function as a second argument. A full example:

```python
ALIVE_CHECKS = [
    ("django_alive.checks.check_database", {}),
    ("django_alive.checks.check_staticfile", {
        "filename": "img/favicon.ico",
    }),
    ("django_alive.checks.check_cache", {
        "cache": "session",
        "key": "test123",
    }),
    ("django_alive.checks.check_migrations", {}),
]
```

**⚠️ Warning: Changed in version 1.3.0 ⚠️**

**NOTE:** Old settings with `ALIVE_CHECKS` as dict was deprecated in favor of a list of tuples.


### Built-in Checks

Defined in `django_alive.checks`.

```python
def check_cache(key="django-alive", cache="default")
```

Fetch a cache key against the specified cache.

#### Parameters:

- `key` (`str`):  Cache key to fetch (does not need to exist)
- `cache` (`str`):  Cache alias to execute against

---

```python
def check_database(query="SELECT 1", database="default")
```

Run a SQL query against the specified database.

#### Parameters:

- `query` (`str`):  SQL to execute
- `database` (`str`):  Database alias to execute against

---

```python
def check_migrations(alias=None)
```

Verify all defined migrations have been applied

#### Parameters:

- `alias` (`str`):  An optional database alias (default: check all defined databases)

---

```python
def check_staticfile(filename)
```

Verify a static file is reachable

#### Parameters:

- `filename` (`str`):  static file to verify

## Management Command

In addition to the view, the configured healthchecks can also be run via a management command with `manage.py healthcheck`. This will exit with an error code if all the healthchecks do not pass.

## Custom Checks

`django-alive` is designed to easily extend with your own custom checks. Simply define a function which performs your check and raises a `django_alive.HealthcheckFailure` exception in the event of a failure. See [`checks.py`](https://github.com/lincolnloop/django-alive/blob/master/django_alive/checks.py) for some examples on how to write a check.

## Disabling `ALLOWED_HOSTS` for Healthchecks

Often, load balancers will not pass a `Host` header when probing a healthcheck endpoint. This presents a problem for [Django's host header validation](https://docs.djangoproject.com/en/2.1/topics/security/#host-headers-virtual-hosting). A middleware is included that will turn off the host checking only for the healthcheck endpoints. This is safe since these views never do anything with the `Host` header.

Enable the middleware by inserting this **at the beginning** of your `MIDDLEWARE`:

```python
MIDDLEWARE = [
    "django_alive.middleware.healthcheck_bypass_host_check",
    # ...
]
```

## Handling `SECURE_SSL_REDIRECT`

If your load balancer is doing HTTPS termination and you have `SECURE_SSL_REDIRECT=True` in your settings, you want to make sure that your healtcheck URLs are not also redirected to HTTPS. In that case, add the following to your settings:

```python
SECURE_REDIRECT_EXEMPT = [r"^-/"]  # django-alive URLs
```
