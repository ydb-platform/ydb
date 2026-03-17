=====================
Admin View Permission
=====================

.. image:: https://travis-ci.org/ctxis/django-admin-view-permission.svg?branch=master
    :target: https://travis-ci.org/ctxis/django-admin-view-permission
    :alt: Build Status
.. image:: https://coveralls.io/repos/github/ctxis/django-admin-view-permission/badge.svg?branch=master
   :target: https://coveralls.io/github/ctxis/django-admin-view-permission?branch=master
   :alt: Coverage Status
.. image:: https://codeclimate.com/github/ctxis/django-admin-view-permission/badges/gpa.svg
   :target: https://codeclimate.com/github/ctxis/django-admin-view-permission
   :alt: Code Climate

Reusable application which provides a view permission for the existing models.

Requirements
------------

* Django

Support
-------

    The package is *deprecated* for *Django 2.1*. Django added the functionality
    into the `core <https://docs.djangoproject.com/en/2.1/releases/2.1/#model-view-permission>`_ (
    the 2 implementations are different). You should use this package only if you
    use Django < 2.1.

        * If you have installed this package by accident to your Django 2.1
          project, it won't affect the build-in view permission which comes
          with Django.
        * If you have upgraded you application to use Django > 2.1 just uninstall
          this package

* Django: 1.8, 1.9, 1.10, 1.11, 2.0
* Python: 2.7, 3.4, 3.5, 3.6

Compatible with `django-parler <https://django-parler.readthedocs.io/>`_'s translatable models. To verify which django-parler version our test suite runs against, check ``requirements-debug.txt``. You do not need django-parler to install django-admin-view-permission.

Documentation
-------------
For a full documentation you can visit: http://django-admin-view-permission.readthedocs.org/

Setup
-----

* ``pip install django-admin-view-permission``

and then add ``admin_view_permission`` at the INSTALLED_APPS like this::

    INSTALLED_APPS = [
        'admin_view_permission',
        'django.contrib.admin',
        ...
    ]

and finally run ``python manage.py migrate``.

    | You need to place the ``admin_view_permission`` before ``django.contrib.admin`` in INSTALLED_APPS.


In case of a customized AdminSite in order to apply the view permission, you
should inherit from the ``AdminViewPermissionAdminSite`` class::

    from admin_view_permission.admin import AdminViewPermissionAdminSite

    class MyAdminSite(AdminViewPermissionAdminSite):
        ...


Configuration
-------------

This app provides a setting::

    ADMIN_VIEW_PERMISSION_MODELS = [
        'auth.User',
        ...
    ]

in which you can provide which models you want to be added the view permission.
If you don't specify this setting then the view permission will be applied to
all the models.

Uninstall
---------

1. Remove the ``admin_view_permission`` from your ``INSTALLED_APPS`` setting
2. Delete the view permissions from the database::

        from django.contrib.auth.models import Permission
        permissions = Permission.objects.filter(codename__startswith='view')
        permissions.delete()

   It will be helpful to check if the queryset contains only the view
   permissions and not anything else (for example: custom permission added)
