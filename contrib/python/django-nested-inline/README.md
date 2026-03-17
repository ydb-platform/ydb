django-nested-inline
====================

Nested inline support for Django admin

Most of the code from this package is from [https://code.djangoproject.com/ticket/9025](https://code.djangoproject.com/ticket/9025)

Github
------

[https://github.com/s-block/django-nested-inline](https://github.com/s-block/django-nested-inline)


Installation
------------

pip install django-nested-inline


Usage
-----

Add `nested_inline` to `INSTALLED_APPS`

```python
# models.py

from django.db import models

class TopLevel(models.Model):
    name = models.CharField(max_length=200)

class LevelOne(models.Model):
    name = models.CharField(max_length=200)
    level = models.ForeignKey('TopLevel')

class LevelTwo(models.Model):
    name = models.CharField(max_length=200)
    level = models.ForeignKey('LevelOne')

class LevelThree(models.Model):
    name = models.CharField(max_length=200)
    level = models.ForeignKey('LevelTwo')


# admin.py

from django.contrib import admin
from nested_inline.admin import NestedStackedInline, NestedModelAdmin
from example.models import *

class LevelThreeInline(NestedStackedInline):
    model = LevelThree
    extra = 1
    fk_name = 'level'


class LevelTwoInline(NestedStackedInline):
    model = LevelTwo
    extra = 1
    fk_name = 'level'
    inlines = [LevelThreeInline]


class LevelOneInline(NestedStackedInline):
    model = LevelOne
    extra = 1
    fk_name = 'level'
    inlines = [LevelTwoInline]


class TopLevelAdmin(NestedModelAdmin):
    model = TopLevel
    inlines = [LevelOneInline]


admin.site.register(TopLevel, TopLevelAdmin)
```

Minifying Javascript
--------------------

Released versions of this library should have a minified version of the js.

Minification is done with uglifyjs. If npm is installed on your system,
you can install uglifyjs with the command:
```sh
npm install -g uglify-js
```

Then change to the directory where the file 'inlines-nested.js' and
run the following command:
```sh
uglifyjs --compress --mangle --output ./inlines-nested.min.js -- inlines-nested.js
```

Releasing to PyPi
-----------------

Create a file at $HOME/.pypirc and put the following text inside:

```txt
[pypi]
username = __token__
password = <Token from pypi>
```

Make sure the dependencies for this project are installed:
```sh
pip install django
```
Then install the build tool:
```sh
pip install build
```

Next run the distribution build of the project:
```sh
python -m build
```

Lastly, push the release to pypi
```sh
twine upload dist/*
```

Changelist
----------

0.4.6:

* Support django 4.1
* Fix inline media error (#130)
* Fix error when using 'save as new' (#149)
* Add .fieldBox styling for django 2.1+ (#114)
* Fix select2 import for Django >=2.2 (#150)
* Fixed FieldDoesNotExist import to support Django>=3.2 (#147)

0.4.5 - Support django 4.0 and django-csp

0.4.4 - Add formset:added and formset:removed events (#97)

0.4.3 - Update media so it expects to find jquery in the right place. (#75)

0.4.2 - Fix assets

0.4.1 - Fix permission checks

0.4.0 - Added support for Django 3.0

0.3.7 - added support for django 1.10, fix unique fieldset id

0.3.6 - added support for django 1.9

0.3.5 - Removed deprecated methods and updated for Django 1.8/1.9

0.3.4 - added licence and updated for python 3

0.3.3 - fixed bug where inlines without inlines would cause an error
