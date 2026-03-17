====================
django-import-export
====================

.. |build| image:: https://github.com/django-import-export/django-import-export/actions/workflows/release.yml/badge.svg
    :target: https://github.com/django-import-export/django-import-export/actions/workflows/release.yml
    :alt: Build status on Github

.. |coveralls| image:: https://coveralls.io/repos/github/django-import-export/django-import-export/badge.svg?branch=main
    :target: https://coveralls.io/github/django-import-export/django-import-export?branch=main

.. |pypi| image:: https://img.shields.io/pypi/v/django-import-export.svg
    :target: https://pypi.org/project/django-import-export/
    :alt: Current version on PyPi

.. |docs| image:: http://readthedocs.org/projects/django-import-export/badge/?version=stable
    :target: https://django-import-export.readthedocs.io/en/stable/
    :alt: Documentation

.. |pyver| image:: https://img.shields.io/pypi/pyversions/django-import-export
    :alt: PyPI - Python Version

.. |djangover| image:: https://img.shields.io/pypi/djversions/django-import-export
    :alt: PyPI - Django Version

.. |downloads| image:: https://static.pepy.tech/personalized-badge/django-import-export?period=month&units=international_system&left_color=black&right_color=blue&left_text=Downloads/month
    :target: https://pepy.tech/project/django-import-export

.. |xfollow| image:: https://img.shields.io/twitter/url/https/twitter.com/django_import.svg?style=social&label=Follow%20%40django_import
   :alt: Follow us on X
   :target: https://twitter.com/django_import

.. |discord|  image:: https://img.shields.io/discord/1240294048653119508?style=flat
   :alt: Discord

|build| |coveralls| |pypi| |docs| |pyver| |djangover| |downloads| |xfollow| |discord|

Introduction
============

Straightforward, reliable and comprehensive file import / export for your Django application.

*django-import-export* is an application and library which lets you manage import / export from / to a variety of sources (csv, xlsx, json etc).

Can be run programmatically, or with optional integration with the Django Admin site:

..
  source of this video uploaded to this issue comment:
  https://github.com/django-import-export/django-import-export/pull/1833#issuecomment-2118777440

https://github.com/django-import-export/django-import-export/assets/6249838/ab56d8ba-c307-4bdf-8fa9-225669c72b37

`Screenshots <https://django-import-export.readthedocs.io/en/latest/screenshots.html>`_

Features
========

* Import / export via `Admin UI Integration <https://django-import-export.readthedocs.io/en/latest/admin_integration.html>`_ or `programmatically <https://django-import-export.readthedocs.io/en/latest/getting_started.html#importing-data>`_
* Import to and from a variety of file formats (csv, json, xlsx, pandas, HTML, YAML... and anything else that `tablib <https://github.com/jazzband/tablib>`_ supports)
* `Preview <https://django-import-export.readthedocs.io/en/latest/screenshots.html/>`_ data before importing in Admin UI
* Support for `bulk import <https://django-import-export.readthedocs.io/en/latest/bulk_import.html>`_
* Handles `CRUD (and 'skip') operations during import <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#create-or-update-model-instances>`_
* Flexible handling of `foreign key <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#importing-model-relations>`_ relationships
* `Many-to-many relationship <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#many-to-many-relations>`_ support
* `Validation <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#validation-during-import>`_ of imported data
* Define custom `transformations <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#advanced-data-manipulation-on-export>`_ for exported data
* Import / export the same model instance as `different views <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#customize-resource-options>`_
* Export using `natural keys <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#django-natural-keys>`__ for portability between environments
* `Select items for export <https://django-import-export.readthedocs.io/en/latest/screenshots.html/>`_ via the Admin UI object list
* `Select fields for export <https://django-import-export.readthedocs.io/en/latest/screenshots.html/>`_ via the export form
* `Export single object instances <https://django-import-export.readthedocs.io/en/latest/admin_integration.html#export-from-model-instance-change-form>`_
* Use `django permissions <https://django-import-export.readthedocs.io/en/latest/installation.html#import-export-import-permission-code>`_ to control import / export authorization
* Internationalization support
* Based on `tablib <https://github.com/jazzband/tablib>`__
* Support for MySQL / PostgreSQL / SQLite
* Extensible - `add custom logic to control import / export <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html>`_
* Handle import from various character encodings
* `Celery <https://django-import-export.readthedocs.io/en/latest/celery.html>`_ integration
* Test locally with `Docker <https://django-import-export.readthedocs.io/en/latest/testing.html>`_
* Comprehensive `documentation <https://django-import-export.readthedocs.io/en/latest/index.html>`__
* `Extensible API <https://django-import-export.readthedocs.io/en/latest/api_admin.html>`_
* test coverage :100:
* Supports dark mode :rocket:

Example use-cases
=================

*django-import-export* is designed to be extensible and can be used to support a variety of operations.
Here are some examples of how it has been used in the wild:

* Configure external cron jobs to run an import or export at set times
* Use `permissions <https://django-import-export.readthedocs.io/en/latest/installation.html#import-export-import-permission-code>`_ to define a subset of users able to import and export project data
* Safely update project reference data by importing from version controlled csv
* Create portable data to transfer between environments using `natural keys <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#django-natural-keys>`_
* Manage user access to an application by importing externally version controlled auth user lists
* Add `hooks <https://django-import-export.readthedocs.io/en/latest/advanced_usage.html#advanced-data-manipulation-on-export>`_ to anonymize data on export
* `Modify import / export UI forms <https://django-import-export.readthedocs.io/en/latest/admin_integration.html#customize-admin-import-forms>`_ to add dynamic filtering on import / export.
* Build a migration layer between platforms, for example take a `Wordpress <https://wordpress.org/>`_ export and import to `Wagtail <https://wagtail.org/>`_

Getting started
===============

* `Installation <https://django-import-export.readthedocs.io/en/latest/installation.html>`_
* `Getting started <https://django-import-export.readthedocs.io/en/latest/getting_started.html>`__
* `Example application <https://django-import-export.readthedocs.io/en/latest/installation.html#exampleapp>`_

Help and support
================

* `Documentation <https://django-import-export.readthedocs.io/en/latest/>`_
* `FAQ <https://django-import-export.readthedocs.io/en/latest/faq.html>`_
* `Getting help <https://django-import-export.readthedocs.io/en/latest/faq.html#what-s-the-best-way-to-communicate-a-problem-question-or-suggestion>`_
* `Contributing <https://django-import-export.readthedocs.io/en/latest/faq.html#how-can-i-help>`_
* Become a `sponsor <https://github.com/sponsors/django-import-export>`_
* Join our `discord <https://discord.gg/aCcec52kY4>`_
* Tutorial videos on `YouTube <https://www.youtube.com/results?search_query=django-import-export>`_
* `Raise a security issue <https://github.com/django-import-export/django-import-export/blob/main/SECURITY.md>`_

Commercial support
==================

Commercial support is provided by `Bellaport Systems Ltd <https://www.bellaport.co.uk>`_

Releases
========

* `Release notes <https://django-import-export.readthedocs.io/en/latest/release_notes.html>`_
* `Changelog <https://django-import-export.readthedocs.io/en/latest/changelog.html>`_

