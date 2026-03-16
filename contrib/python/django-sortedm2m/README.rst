================
django-sortedm2m
================

.. image:: https://jazzband.co/static/img/badge.svg
   :target: https://jazzband.co/
   :alt: Jazzband

.. image:: https://img.shields.io/pypi/v/django-sortedm2m.svg
   :target: https://pypi.python.org/pypi/django-sortedm2m
   :alt: PyPI Release

.. image:: https://github.com/jazzband/django-sortedm2m/actions/workflows/test.yml/badge.svg?branch=master
   :target: https://github.com/jazzband/django-sortedm2m/actions?query=branch%3Amaster
   :alt: Build Status

.. image:: https://codecov.io/gh/jazzband/django-sortedm2m/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/jazzband/django-sortedm2m
   :alt: Code coverage

``sortedm2m`` is a drop-in replacement for django's own ``ManyToManyField``.
The provided ``SortedManyToManyField`` behaves like the original one but
remembers the order of added relations.

Use Cases
=========

Imagine that you have a gallery model and a photo model. Usually you want a
relation between these models so you can add multiple photos to one gallery
but also want to be able to have the same photo on many galleries.

This is where you usually can use many to many relation. The downside is that
django's default implementation doesn't provide a way to order the photos in
the gallery. So you only have a random ordering which is not suitable in most
cases.

You can work around this limitation by using the ``SortedManyToManyField``
provided by this package as drop in replacement for django's
``ManyToManyField``.

Requirements
============

**django-sortedm2m** runs on Python 3.6+ and multiple Django versions.
See the ``.github/workflows/test.yml`` configuration for the tested Django versions.

Usage
=====

Use ``SortedManyToManyField`` like ``ManyToManyField`` in your models:

.. code-block:: python

    from django.db import models
    from sortedm2m.fields import SortedManyToManyField

    class Photo(models.Model):
        name = models.CharField(max_length=50)
        image = models.ImageField(upload_to='...')

    class Gallery(models.Model):
        name = models.CharField(max_length=50)
        photos = SortedManyToManyField(Photo)

If you use the relation in your code like the following, it will remember the
order in which you have added photos to the gallery. :

.. code-block:: python

    gallery = Gallery.objects.create(name='Photos ordered by name')
    for photo in Photo.objects.order_by('name'):
        gallery.photos.add(photo)

``SortedManyToManyField``
-------------------------

You can use the following arguments to modify the default behavior:

``sorted``
~~~~~~~~~~

**Default:** ``True``

You can set the ``sorted`` to ``False`` which will force the
``SortedManyToManyField`` in behaving like Django's original
``ManyToManyField``. No ordering will be performed on relation nor will the
intermediate table have a database field for storing ordering information.

``sort_value_field_name``
~~~~~~~~~~~~~~~~~~~~~~~~~

**Default:** ``'sort_value'``

Specifies how the field is called in the intermediate database table by which
the relationship is ordered. You can change its name if you have a legacy
database that you need to integrate into your application.

``base_class``
~~~~~~~~~~~~~~

**Default:** ``None``

You can set the ``base_class``, which is the base class of the through model of
the sortedm2m relationship between models to an abstract base class containing
a ``__str__`` method to improve the string representations of sortedm2m
relationships.

.. note::

    You also could use it to add additional fields to the through model. But
    please beware: These fields will not be created or modified by an
    automatically created migration. You will need to take care of migrations
    yourself. In most cases when you want to add another field, consider
    *not* using sortedm2m but use a ordinary Django ManyToManyField and
    specify `your own through model`_.

.. _your own through model: https://docs.djangoproject.com/en/1.11/ref/models/fields/#django.db.models.ManyToManyField.through

Migrating a ``ManyToManyField`` to be a ``SortedManyToManyField``
=================================================================

If you are using Django's migration framework and want to change a
``ManyToManyField`` to be a ``SortedManyToManyField`` (or the other way
around), you will find that a migration created by Django's ``makemigrations``
will not work as expected.

In order to migrate a ``ManyToManyField`` to a ``SortedManyToManyField``, you
change the field in your models to be a ``SortedManyToManyField`` as
appropriate and create a new migration with ``manage.py makemigrations``.
Before applying it, edit the migration file and change in the ``operations``
list ``migrations.AlterField`` to ``AlterSortedManyToManyField`` (import it
from ``sortedm2m.operations``).  This operation will take care of changing the
intermediate tables, add the ordering field and fill in default values.

Admin
=====

``SortedManyToManyField`` provides a custom widget which can be used to sort
the selected items. It renders a list of checkboxes that can be sorted by
drag'n'drop.

To use the widget in the admin you need to add ``sortedm2m`` to your
INSTALLED_APPS settings, like:

.. code-block:: python

   INSTALLED_APPS = (
       'django.contrib.auth',
       'django.contrib.contenttypes',
       'django.contrib.sessions',
       'django.contrib.sites',
       'django.contrib.messages',
       'django.contrib.staticfiles',
       'django.contrib.admin',

       'sortedm2m',

       '...',
   )

Otherwise it will not find the css and js files needed to sort by drag'n'drop.

Finally, make sure *not* to have the model listed in any ``filter_horizontal``
or ``filter_vertical`` tuples inside of your ``ModelAdmin`` definitions.

If you did it right, you'll wind up with something like this:

.. image:: http://i.imgur.com/HjIW7MI.jpg

It's also possible to use the ``SortedManyToManyField`` with admin's
``raw_id_fields`` option in the ``ModelAdmin`` definition. Add the name of the
``SortedManyToManyField`` to this list to get a simple text input field. The
order in which the ids are entered into the input box is used to sort the
items of the sorted m2m relation.

Example:

.. code-block:: python

    from django.contrib import admin

    class GalleryAdmin(admin.ModelAdmin):
        raw_id_fields = ('photos',)

Contribute
==========
This is a `Jazzband <https://jazzband.co>`_ project. By contributing you agree to abide by the
`Contributor Code of Conduct <https://jazzband.co/about/conduct>`_ and follow the
`guidelines <https://jazzband.co/about/guidelines>`_.

You can find the latest development version on Github_. Get there and fork it, file bugs or send well wishes.

.. _github: http://github.com/jazzband/django-sortedm2m

Running the tests
-----------------

I recommend to use ``tox`` to run the tests for all relevant python versions
all at once. Therefore install ``tox`` with ``pip install tox``, then type in
the root directory of the ``django-sortedm2m`` checkout::

   tox

The tests are run against SQLite, then against PostgreSQL, then against mySQL -
so you need to install PostgreSQL and mySQL on your dev environment, and should
have a role/user ``sortedm2m`` set up for both PostgreSQL and mySQL.

Code Quality
------------
This project uses `isort <https://github.com/timothycrosley/isort>`_, `pycodestyle <https://github.com/PyCQA/pycodestyle>`_,
and `pylint <https://www.pylint.org>`_ to manage validate code quality. These validations can be run with the
following command::

   tox -e quality
