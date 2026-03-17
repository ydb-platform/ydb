# django-tables2 - An app for creating HTML tables

[![Latest PyPI version](https://badge.fury.io/py/django-tables2.svg)](https://pypi.python.org/pypi/django-tables2)
[![Any color you like](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

django-tables2 simplifies the task of turning sets of data into HTML tables. It
has native support for pagination and sorting. It does for HTML tables what
`django.forms` does for HTML forms. e.g.

- Available on pypi as [django-tables2](https://pypi.python.org/pypi/django-tables2)
- Tested against currently supported versions of Django
  [and supported python 3 versions Django supports](https://docs.djangoproject.com/en/dev/faq/install/#what-python-version-can-i-use-with-django).
- [Documentation on readthedocs.org](https://django-tables2.readthedocs.io/en/latest/)
- [Bug tracker](http://github.com/jieter/django-tables2/issues)

Features:

- Any iterable can be a data-source, but special support for Django `QuerySets` is included.
- The builtin UI does not rely on JavaScript.
- Support for automatic table generation based on a Django model.
- Supports custom column functionality via subclassing.
- Pagination.
- Column based table sorting.
- Template tag to enable trivial rendering to HTML.
- Generic view mixin.

![An example table rendered using django-tables2](https://cdn.rawgit.com/jieter/django-tables2/master/docs/img/example.png)

![An example table rendered using django-tables2 and bootstrap theme](https://cdn.rawgit.com/jieter/django-tables2/master/docs/img/bootstrap.png)

![An example table rendered using django-tables2 and semantic-ui theme](
https://cdn.rawgit.com/jieter/django-tables2/master/docs/img/semantic.png)

## Example

Start by adding `django_tables2` to your `INSTALLED_APPS` setting like this:

```python
INSTALLED_APPS = (
    ...,
    "django_tables2",
)
```

Creating a table for a model `Simple` is as simple as:

```python
import django_tables2 as tables

class SimpleTable(tables.Table):
    class Meta:
        model = Simple
```
This would then be used in a view:

```python
class TableView(tables.SingleTableView):
    table_class = SimpleTable
    queryset = Simple.objects.all()
    template_name = "simple_list.html"
```
And finally in the template:

```
{% load django_tables2 %}
{% render_table table %}
```

This example shows one of the simplest cases, but django-tables2 can do a lot more!
Check out the [documentation](https://django-tables2.readthedocs.io/en/latest/) for more details.
