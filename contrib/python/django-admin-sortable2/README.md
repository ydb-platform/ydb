# django-admin-sortable2

This Django package adds functionality for generic drag-and-drop ordering of items in the List, the Stacked- and the
Tabular-Inlines Views of the Django Admin interface.

[![Build Status](https://github.com/jrief/django-admin-sortable2/actions/workflows/tests.yml/badge.svg)](https://github.com/jrief/django-admin-sortable2/actions/workflows/tests.yml)
[![PyPI version](https://img.shields.io/pypi/v/django-admin-sortable2.svg)](https://pypi.python.org/pypi/django-admin-sortable2)
[![Python versions](https://img.shields.io/pypi/pyversions/django-admin-sortable2.svg)](https://pypi.python.org/pypi/django-admin-sortable2)
[![Django versions](https://img.shields.io/pypi/djversions/django-admin-sortable2)](https://pypi.python.org/pypi/django-admin-sortable2)
[![Downloads](https://img.shields.io/pypi/dm/django-admin-sortable2.svg)](https://img.shields.io/pypi/dm/django-admin-sortable2.svg)
[![Software license](https://img.shields.io/pypi/l/django-admin-sortable2.svg)](https://github.com/jrief/django-admin-sortable2/blob/master/LICENSE)

Check the demo:

![Demo](https://raw.githubusercontent.com/jrief/django-admin-sortable2/master/docs/source/_static/django-admin-sortable2.gif)

This library offers simple mixin classes which enrich the functionality of any existing class inheriting from
`admin.ModelAdmin`, `admin.StackedInline` or `admin.TabularInline`.

It thus makes it very easy to integrate with existing models and their model admin interfaces. Existing models can
inherit from `models.Model` or any other class derived thereof. No special base class is required.


## Version 2.0

This is a major rewrite of this **django-admin-sortable2**. It replaces the client side part against
[Sortable.JS](https://sortablejs.github.io/Sortable/) and thus the need for jQuery.

Replacing that library allowed me to add a new feature: Multiple items can now be dragged and dropped together.


## Project's Home

https://github.com/jrief/django-admin-sortable2

Detailled documentation can be found on [ReadTheDocs](https://django-admin-sortable2.readthedocs.org/en/latest/).

Before reporting bugs or asking questions, please read the
[contributor's guide](https://django-admin-sortable2.readthedocs.io/en/latest/contributing.html).


## License

Licensed under the terms of the MIT license.

Copyright &copy; 2013-2022 Jacob Rief and contributors.

Please follow me on
[![Twitter Follow](https://img.shields.io/twitter/follow/jacobrief.svg?style=social&label=Jacob+Rief)](https://twitter.com/jacobrief)
for updates and other news.
