# django-partial-index

[![Build Status](https://api.travis-ci.org/mattiaslinnap/django-partial-index.svg?branch=master)](https://travis-ci.org/mattiaslinnap/django-partial-index)
[![PyPI version](https://badge.fury.io/py/django-partial-index.svg)](https://pypi.python.org/pypi/django-partial-index/)

Partial (sometimes also called filtered or conditional) index support for Django.

With partial indexes, only some subset of the rows in the table have corresponding index entries.
This can be useful for optimizing index size and query speed, and to add unique constraints for only selected rows.

More info on partial indexes:

* https://www.postgresql.org/docs/current/static/indexes-partial.html
* https://sqlite.org/partialindex.html


## Partial indexes now included in Django

Since the release of [Django 2.2 LTS](https://docs.djangoproject.com/en/2.2/releases/2.2/) in April 2019,
partial indexes are now supported by standard Django.

These are called [index conditions](https://docs.djangoproject.com/en/2.2/ref/models/indexes/#condition) there.

The django-partial-index package will live on in maintenance mode.

It can be useful if you are maintaining a project on and older version of Django, or wish to migrate django-partial-index indexes to Django 2.2 style on your own schedule.

## Install

`pip install django-partial-index`

Requirements:

* Django 1.11, 2.0, 2.1 or 2.2,
* Python 2.7, 3.4, 3.5, 3.6 or 3.7 (as supported by the Django version),
* PostgreSQL or SQLite database backend. (Partial indexes are not supported on MySQL, and require major hackery on Oracle.)

All Python versions which Django supports are also supported by this package. These are:

* Django 1.11 - Python 2.7 and 3.4 - 3.7,
* Django 2.0 - Python 3.4 - 3.7,
* Django 2.1 - Python 3.5 - 3.7,
* Django 2.2 - Python 3.5 - 3.7.


## Usage

Set up a PartialIndex and insert it into your model's class-based Meta.indexes list:

```python
from partial_index import PartialIndex, PQ

class MyModel(models.Model):
    class Meta:
        indexes = [
            PartialIndex(fields=['user', 'room'], unique=True, where=PQ(deleted_at__isnull=True)),
            PartialIndex(fields=['created_at'], unique=False, where=PQ(is_complete=False)),
        ]
```

The `PQ` uses the exact same syntax and supports all the same features as Django's `Q` objects ([see Django docs for a full tutorial](https://docs.djangoproject.com/en/1.11/topics/db/queries/#complex-lookups-with-q-objects)). It is provided for compatibility with Django 1.11.

Of course, these (unique) indexes could be created by a handwritten [RunSQL migration](https://docs.djangoproject.com/en/1.11/ref/migration-operations/#runsql).
But the constraints are part of the business logic, and best kept close to the model definitions.

### Partial unique constraints

With `unique=True`, this can be used to create unique constraints for a subset of the rows.

For example, you might have a model that has a deleted_at field to mark rows as archived instead of deleting them forever.
You wish to add unique constraints on "alive" rows, but allow multiple copies in the archive.
[Django's unique_together](https://docs.djangoproject.com/en/1.11/ref/models/options/#unique-together) is not sufficient here, as that cannot
distinguish between the archived and alive rows.

```python
from partial_index import PartialIndex, PQ

class RoomBooking(models.Model):
    user = models.ForeignKey(User)
    room = models.ForeignKey(Room)
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        # unique_together = [('user', 'room')] - Does not allow multiple deleted rows. Instead use:
        indexes = [
            PartialIndex(fields=['user', 'room'], unique=True, where=PQ(deleted_at__isnull=True))
        ]
```

### Partial non-unique indexes

With `unique=False`, partial indexes can be used to optimise lookups that return only a small subset of the rows.

For example, you might have a job queue table which keeps an archive of millions of completed jobs. Among these are a few pending jobs,
which you want to find with a `.filter(is_complete=0)` query.

```python
from partial_index import PartialIndex, PQ

class Job(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    is_complete = models.IntegerField(default=0)

    class Meta:
        indexes = [
            PartialIndex(fields=['created_at'], unique=False, where=PQ(is_complete=0))
        ]
```

Compared to an usual full index on the `is_complete` field, this can be significantly smaller in disk and memory use, and faster to update.

### Referencing multiple fields in the condition

With `F`-expressions, you can create conditions that reference multiple fields:

```python
from partial_index import PartialIndex, PQ, PF

class NotTheSameAgain(models.Model):
    a = models.IntegerField()
    b = models.IntegerField()

    class Meta:
        indexes = [
            PartialIndex(fields=['a', 'b'], unique=True, where=PQ(a=PF('b'))),
        ]
```

This PartialIndex allows multiple copies of `(2, 3)`, but only a single copy of `(2, 2)` to exist in the database.

The `PF` uses the exact same syntax and supports all the same features as Django's `F` expressions ([see Django docs for a full tutorial](https://docs.djangoproject.com/en/1.11/ref/models/expressions/#f-expressions)). It is provided for compatibility with Django 1.11.

### Unique validation on ModelForms

Unique partial indexes are validated by the PostgreSQL and SQLite databases. When they reject an INSERT or UPDATE, Django raises a `IntegrityError` exception. This results in a `500 Server Error` status page in the browser if not handled before the database query is run.

ModelForms perform unique validation before saving an object, and present the user with a descriptive error message.

Adding an index does not modify the parent model's unique validation, so partial index validations are not handled by them by default. To add that to your model, include the `ValidatePartialUniqueMixin` in your model definition:

```python
from partial_index import PartialIndex, PQ, ValidatePartialUniqueMixin

class MyModel(ValidatePartialUniqueMixin, models.Model):
    class Meta:
        indexes = [
            PartialIndex(fields=['user', 'room'], unique=True, where=PQ(deleted_at__isnull=True)),
        ]
```

Note that it should be added on the model itself, not the ModelForm class.

Adding the mixin for non-unique partial indexes is unnecessary, as they cannot cause database IntegrityErrors.

### Text-based where-conditions (deprecated)

Text-based where-conditions are deprecated and will be removed in the next release (0.6.0) of django-partial-index.

They are still supported in version 0.5.0 to simplify upgrading existing projects to the `PQ`-based indexes. New projects should not use them.


```python
from partial_index import PartialIndex

class TextExample(models.Model):
    class Meta:
        indexes = [
            PartialIndex(fields=['user', 'room'], unique=True, where='deleted_at IS NULL'),
            PartialIndex(fields=['created_at'], unique=False, where_postgresql='is_complete = false', where_sqlite='is_complete = 0')
        ]
```


## Version History

### 0.6.0 (latest)
* Add support for Django 2.2.
* Document (already existing) support for Django 2.1 and Python 3.7.

### 0.5.2
* Fix makemigrations for Django 1.11.
* Make sure PQ and PF are imported directly from partial_index in migration files.

### 0.5.1
* Fix README formatting in PyPI.

### 0.5.0
* Add support for Q-object based where-expressions.
* Deprecate support for text-based where-expressions. These will be removed in version 0.6.0.
* Add ValidatePartialUniqueMixin for model classes. This adds partial unique index validation for ModelForms, avoiding an IntegrityError and instead showing an error message as with usual unique_together constraints.

### 0.4.0
* Add support for Django 2.0.

### 0.3.0
* Add support for separate `where_postgresql=''` and `where_sqlite=''` predicates, when the expression has different syntax on the two
 database backends and you wish to support both.

### 0.2.1
* Ensure that automatically generated index names depend on the "unique" and "where" parameters. Otherwise two indexes with the same fields would be considered identical by Django.

### 0.2.0
* Fully tested SQLite and PostgreSQL support.
* Tests for generated SQL statements, adding and removing indexes, and that unique constraints work when inserting rows into the db tables.
* Python 2.7, 3.4-3.6 support.

### 0.1.1
* Experimental SQLite support.

### 0.1.0
* First release, working but untested PostgreSQL support.

## Future plans

* Add a validation mixin for DRF Serializers.
* Remove support for text-based where conditions.
