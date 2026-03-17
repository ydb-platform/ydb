# Extensions and monkey-patching for django-stubs

[![Build Status](https://travis-ci.com/typeddjango/django-stubs.svg?branch=master)](https://travis-ci.com/typeddjango/django-stubs)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Gitter](https://badges.gitter.im/mypy-django/Lobby.svg)](https://gitter.im/mypy-django/Lobby)


This package contains extensions and monkey-patching functions for the [django-stubs](https://github.com/typeddjango/django-stubs) package. Certain features of django-stubs (i.e. generic django classes that don't define the `__class_getitem__` method) require runtime monkey-patching, which can't be done with type stubs. These extensions were split into a separate package so library consumers don't need `mypy` as a runtime dependency ([#526](https://github.com/typeddjango/django-stubs/pull/526#pullrequestreview-525798031)).

## Installation

```bash
pip install django-stubs-ext
```

## Usage

In your Django application, use the following code:

```py
import django_stubs_ext

django_stubs_ext.monkeypatch()
```

This only needs to be called once, so the call to `monkeypatch` should be placed in your top-level settings.
Real-life example [can be found here](https://github.com/wemake-services/wemake-django-template/blob/5bf1569e2710e11befc6991893f94419136d74bd/%7B%7Bcookiecutter.project_name%7D%7D/server/settings/__init__.py#L14-L19).

## Version compatibility

Since django-stubs supports multiple Django versions, this package takes care to only monkey-patch the features needed by your django version, and decides which features to patch at runtime. This is completely safe, as (currently) we only add a `__class_getitem__` method that does nothing:

```py
@classmethod
def __class_getitem__(cls, *args, **kwargs):
    return cls
```

## To get help

For help with django-stubs, please view the main repository at <https://github.com/typeddjango/django-stubs>

We have a Gitter chat here: <https://gitter.im/mypy-django/Lobby>
If you think you have a more generic typing issue, please refer to <https://github.com/python/mypy> and their Gitter.

## Contributing

The django-stubs-ext package is part of the [django-stubs](https://github.com/typeddjango/django-stubs) monorepo. If you would like to contribute, please view the django-stubs [contribution guide](https://github.com/typeddjango/django-stubs/blob/master/CONTRIBUTING.md).

You can always also reach out in gitter to discuss your contributions!
