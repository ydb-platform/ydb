![PyPi Version](https://img.shields.io/pypi/v/django-dynamic-raw-id.svg)
![Github Actions](https://github.com/lincolnloop/django-dynamic-raw-id/actions/workflows/test.yml/badge.svg)

# django-dynamic-raw-id

A Django admin raw_id_fields widget replacement that handles display of an object's
string value on change and can be overridden via a template.

See this example:

<img src="https://d.pr/i/1kv7d.png" style="max-height: 400px;" alt="Screenshot of Django Admin"/>

## Compatibility Matrix:

| Py/Dj     | 3.8 | 3.9 | 3.10 | 3.11 | 3.12 |
| --------- | --- | --- | ---- | ---- | ---- |
| 3.2 (LTS) | ✓   | ✓   | ✓    | ✓    | ✓    |
| 4.0       | ✓   | ✓   | ✓    | ✓    | ✓    |
| 4.1       | ✓   | ✓   | ✓    | ✓    | ✓    |
| 4.2 (LTS) | ✓   | ✓   | ✓    | ✓    | ✓    |
| 5.0       | —   | —   | ✓    | ✓    | ✓    |
| 5.1       | —   | —   | ✓    | ✓    | ✓    |

## Rationale

By default, Django’s admin loads all possible related instances into a select-box
interface (`<select>`) for fields that are ForeignKey. This can result in long load
times and unresponsive admin pages for models with thousands of instances, or with
multiple ForeinKeys.

The normal fix is to use Django's [ModelAdmin.raw_id_fields][raw_id_docs],
but by default it _only_ shows the raw id of the related model instance, which is
somewhat unhelpful.

This package improves the user experience by providing the string representation or
other customized text for the related instance, linked to that instance's admin
change form, in addition to the raw id itself.

## Installation

Install the package with `pip``:

```bash
$ pip install django-dynamic-raw-id
```

Put `dynamic_raw_id` to your list of `INSTALLED_APPS`:

```python
INSTALLED_APPS = (
  # ... other apps
  'dynamic_raw_id',
)
```

And add the `urlpattern`. Make sure its listed _before_ the generic `admin.site.urls`
urlpattern include:

```python
urlpatterns = [
  # ...
  path('admin/dynamic_raw_id/', include('dynamic_raw_id.urls')),
  path("admin/", admin.site.urls),
]
```

`dynamic_raw_id` comes with some static files so don't forget to run
`manage.py collectstatic`.

## Usage

To start using django-dynamic-raw-id in your application all you need to do is
implement `DynamicRawIDMixin` in your `ModelAdmin` class and add the desired
fields to a list of `dynamic_raw_id_fields``:

```python
from dynamic_raw_id.admin import DynamicRawIDMixin

class UserProfileAdmin(DynamicRawIDMixin, admin.ModelAdmin):
  dynamic_raw_id_fields = ('user',)
```

You can use dynamic_raw_id widgets in a Admin filter as well:

```python
from dynamic_raw_id.admin import DynamicRawIDMixin
from dynamic_raw_id.filters import DynamicRawIDFilter


class UserProfileAdmin(DynamicRawIDMixin, admin.ModelAdmin):
  list_filter = (
    ('dynamic_raw_id_fk', DynamicRawIDFilter),
  )
```

### Customizing the value of the dynamic widget

The coolest feature of django-dynamic-raw-id is the ability to customize the output
of the value displayed alongside the `DynamicRawIDWidget`. There is a basic
implementation if all you want is your object's `__unicode__` value. To change
the value displayed all you need to do is implement the correct template.

django-dynamic-raw-id looks for this template
structure `dynamic_raw_id/<app>/<model>.html`
and `dynamic_raw_id/<app>/multi_<model>.html` (for multi-value lookups).

For instance, if I have a blog post with a `User` dynamic_raw_id field that I want
display as `Firstname Lastname`, I would create the template
`dynamic_raw_id/auth/user.html` with:

```html
<span>{{ object.0.first_name }} {{ object.0.last_name }}</span>
```

### A custom admin URL prefix

If you have your admin _and_ the dynamic_raw_id scripts located on a different
prefix than `/admin/dynamic_raw_id/` you need adjust the `DYNAMIC_RAW_ID_MOUNT_URL`
Javascript variable.

Example:

```python
# In case the app is setup at /foobar/dynamic_raw_id/
path('foobar/dynamic_raw_id/', include('dynamic_raw_id.urls')),
```

```html
<script>
  window.DYNAMIC_RAW_ID_MOUNT_URL = "{% url 'admin:index' %}";
</script>
```

An ideal place is the admin `admin/base_site.html` template. Full example:

```html
{% extends "admin/base_site.html" %} {% block extrahead %} {{ block.super }}
<script>
  window.DYNAMIC_RAW_ID_MOUNT_URL = "{% url 'admin:index' %}";
</script>
{% endblock %}
```

# Testing and Local Development

The testsuite uses Selenium to do frontend tests, we require Firefox and
[geckodriver][geckodriver] to be installed. You can install geckodriver on OS X with
Homebrew:

```bash
$ brew install geckodriver
```

Run the testsuite in your local environment using:

```bash
# If you don't have Poetry yet, install it globally.
$ pip install poetry

# Install Dependencies once and run pytest
$ poetry install
$ poetry run pytest
```

Or use tox to test against various Django and Python versions:

```bash
# If you don't have Tox yet, install it globally.
$ pip install tox

# Run tox against multiple Python versions.
$ tox
```

You can open a Poetry shell to invoke the test suite or other 'manage.py' commands
by calling the `django-admin` tool with the test app settings.

This also allows you to run the internal testing app in a testserver, to preview a
sample of what django-dynamic-raw-id is doing:

```shell
$ poetry shell

$ django-admin migrate
$ django-admin createsuperuser
$ django-admin runserver
```

[raw_id_docs]: https://docs.djangoproject.com/en/dev/ref/contrib/admin/#django.contrib.admin.ModelAdmin.raw_id_fields
[geckodriver]: https://github.com/mozilla/geckodriver
