# django-ltree-2

[![Downloads](https://static.pepy.tech/badge/django-ltree-2)](https://pepy.tech/project/django-ltree-2) [![Documentation Status](https://readthedocs.org/projects/django-ltree-2/badge/?version=latest)](https://django-ltree-2.readthedocs.io/en/latest/?badge=latest) [![CI](https://github.com/baseplate-admin/django-ltree-2/actions/workflows/CI.yml/badge.svg)](https://github.com/baseplate-admin/django-ltree-2/actions/workflows/test.yml) [![Pypi Badge](https://img.shields.io/pypi/v/django-ltree-2.svg)](https://pypi.org/project/django-ltree-2/) [![pre-commit.ci status](https://results.pre-commit.ci/badge/github/baseplate-admin/django-ltree-2/master.svg)](https://results.pre-commit.ci/latest/github/baseplate-admin/django-ltree-2/master)

A tree extension implementation to support hierarchical tree-like data in Django models,
using the native Postgres extension `ltree`.

Postgresql has already a optimized and very useful tree implementation for data.
The extension is [ltree](https://www.postgresql.org/docs/9.6/static/ltree.html)

This fork contains is a continuation of the work done by [`mariocesar`](https://github.com/mariocesar/) on [`django-ltree`](https://github.com/mariocesar/django-ltree) and merges the work done by [`simkimsia`](https://github.com/simkimsia) on [`greendeploy-django-ltree`](https://github.com/GreenDeploy-io/greendeploy-django-ltree)

## Install

Please remember to uninstall `django-ltree` before installing `django-ltree-2`, since both uses `django_ltree` namespace.

---

```
pip install django-ltree-2
```

Then add `django_ltree` to `INSTALLED_APPS` in your Django project settings.

```python
INSTALLED_APPS = [
    ...,
    'django_ltree',
    ...
]
```

Then use it like this:

```python

from django_ltree.models import TreeModel


class CustomTree(TreeModel):
    ...

```

## Requires

-   Django 3.2 or superior
-   Python 3.9 or higher
