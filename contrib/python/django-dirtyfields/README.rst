===================
Django Dirty Fields
===================

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/romgar/django-dirtyfields
   :target: https://gitter.im/romgar/django-dirtyfields?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. image:: https://img.shields.io/pypi/v/django-dirtyfields.svg
   :alt: Published PyPI version
   :target: https://pypi.org/project/django-dirtyfields/
.. image:: https://github.com/romgar/django-dirtyfields/actions/workflows/tests.yml/badge.svg
   :alt: Github Actions Test status
   :target: https://github.com/romgar/django-dirtyfields/actions/workflows/tests.yml
.. image:: https://coveralls.io/repos/github/romgar/django-dirtyfields/badge.svg?branch=develop
   :alt: Coveralls code coverage status
   :target: https://coveralls.io/github/romgar/django-dirtyfields?branch=develop
.. image:: https://readthedocs.org/projects/django-dirtyfields/badge/?version=latest
   :alt: Read the Docs documentation status
   :target: https://django-dirtyfields.readthedocs.io/en/latest/

Tracking dirty fields on a Django model instance.
Dirty means that field in-memory and database values are different.

This package is compatible and tested with the following Python & Django versions:


+------------------------+------------------------------------------+
| Python                 | Django                                   |
+========================+==========================================+
| 3.10                   | 3.2, 4.0, 4.1, 4.2, 5.0, 5.1, 5.2        |
+------------------------+------------------------------------------+
| 3.11                   | 4.1, 4.2, 5.0, 5.1, 5.2                  |
+------------------------+------------------------------------------+
| 3.12                   | 4.2, 5.0, 5.1, 5.2, 6.0                  |
+------------------------+------------------------------------------+
| 3.13                   | 5.1, 5.2, 6.0                            |
+------------------------+------------------------------------------+
| 3.14                   | 5.2, 6.0                                 |
+------------------------+------------------------------------------+



Install
=======

.. code-block:: bash

    $ pip install django-dirtyfields


Usage
=====

To use ``django-dirtyfields``, you need to:

- Inherit from ``DirtyFieldsMixin`` in the Django model you want to track.

.. code-block:: python

    from django.db import models
    from dirtyfields import DirtyFieldsMixin

    class ExampleModel(DirtyFieldsMixin, models.Model):
        """A simple example model to test dirty fields mixin with"""
        boolean = models.BooleanField(default=True)
        characters = models.CharField(blank=True, max_length=80)

- Use one of these 2 functions on a model instance to know if this instance is dirty, and get the dirty fields:

  * ``is_dirty()``
  * ``get_dirty_fields()``


Example
-------

.. code-block:: python

    >>> model = ExampleModel.objects.create(boolean=True,characters="first value")
    >>> model.is_dirty()
    False
    >>> model.get_dirty_fields()
    {}

    >>> model.boolean = False
    >>> model.characters = "second value"

    >>> model.is_dirty()
    True
    >>> model.get_dirty_fields()
    {'boolean': True, "characters": "first_value"}


Consult the `full documentation <https://django-dirtyfields.readthedocs.io/>`_ for more information.
