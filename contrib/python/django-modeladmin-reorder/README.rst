=============================
django-modeladmin-reorder
=============================


.. image:: http://img.shields.io/travis/mishbahr/django-modeladmin-reorder.svg?style=flat-square
    :target: https://travis-ci.org/mishbahr/django-modeladmin-reorder/

.. image:: http://img.shields.io/pypi/v/django-modeladmin-reorder.svg?style=flat-square
    :target: https://pypi.python.org/pypi/django-modeladmin-reorder/
    :alt: Latest Version

.. image:: http://img.shields.io/pypi/dm/django-modeladmin-reorder.svg?style=flat-square
    :target: https://pypi.python.org/pypi/django-modeladmin-reorder/
    :alt: Downloads

.. image:: http://img.shields.io/pypi/l/django-modeladmin-reorder.svg?style=flat-square
    :target: https://pypi.python.org/pypi/django-modeladmin-reorder/
    :alt: License


Custom ordering for the apps and models in the admin app. You can also rename, cross link or exclude models from the app list.


Features
--------

* Reorder apps in admin index - this will allow you to position most used apps in top of the page, instead of listing apps alphabetically. e.g. ``sites`` app before the ``auth`` app

* Rename app labels easily for third party apps without having to modify the source code. e.g. rename ``auth`` app to ``Authorisation`` for the django admin app.

* Split large apps into smaller groups of models.

* Reorder models within an app. e.g. ``auth.User`` model before the ``auth.Group`` model.

* Exclude any of the models from the app list. e.g. Exclude ``auth.Group`` from the app list. Please note this only excludes the model from the app list and it doesn't protect it from access via url.

* Cross link models from multiple apps. e.g. Add ``sites.Site`` model to the ``auth`` app.

* Rename individual models in the app list. e.g. rename ``auth.User`` from ``User`` to ``Staff``


Documentation
-------------

The full documentation is at https://django-modeladmin-reorder.readthedocs.org.


Install
----------

Install django-modeladmin-reorder:

.. code-block:: bash

    pip install django-modeladmin-reorder


Configuration
-------------

1. Add `admin_reorder` to `INSTALLED_APPS`:

   .. code-block:: python

    INSTALLED_APPS = (
        ...
        'admin_reorder',
        ...
    )


2. Add the `ModelAdminReorder` to `MIDDLEWARE_CLASSES`:

   .. code-block:: python

    MIDDLEWARE_CLASSES = (
        ...
        'admin_reorder.middleware.ModelAdminReorder',
        ...
    )


3. Add the setting `ADMIN_REORDER` to your settings.py:

   .. code-block:: python

    ADMIN_REORDER = (
        # Keep original label and models
        'sites',

        # Rename app
        {'app': 'auth', 'label': 'Authorisation'},

        # Reorder app models
        {'app': 'auth', 'models': ('auth.User', 'auth.Group')},

        # Exclude models
        {'app': 'auth', 'models': ('auth.User', )},

        # Cross-linked models
        {'app': 'auth', 'models': ('auth.User', 'sites.Site')},

        # models with custom name
        {'app': 'auth', 'models': (
            'auth.Group',
            {'model': 'auth.User', 'label': 'Staff'},
        )},
    )
