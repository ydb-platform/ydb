=====================================================================
 Celery Result Backends using the Django ORM/Cache framework.
=====================================================================

|build-status| |coverage| |license| |wheel| |pyversion| |pyimp|

:Version: 2.6.0
:Web: https://django-celery-results.readthedocs.io/
:Download: https://pypi.python.org/pypi/django-celery-results
:Source: https://github.com/celery/django-celery-results
:Keywords: django, celery, database, results

About
=====

This extension enables you to store Celery task results using the Django ORM.

It defines a single model (``django_celery_results.models.TaskResult``)
used to store task results, and you can query this database table like
any other Django model.

Installing
==========

The installation instructions for this extension is available
from the `Celery documentation`_

.. _`Celery documentation`:
    https://docs.celeryq.dev/en/latest/django/first-steps-with-django.html#django-celery-results-using-the-django-orm-cache-as-a-result-backend

.. _installation:

Installation
============

You can install django-celery-results either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install -U django-celery-results

.. _installing-from-source:

Downloading and installing from source
--------------------------------------

Download the latest version of django-celery-results from
https://pypi.python.org/pypi/django-celery-results

You can install it by doing the following,::

    $ tar xvfz django-celery-results-0.0.0.tar.gz
    $ cd django-celery-results-0.0.0
    $ python setup.py build
    # python setup.py install

The last command must be executed as a privileged user if
you are not currently using a virtualenv.

.. _installing-from-git:

Using the development version
-----------------------------

With pip
~~~~~~~~

You can install the latest snapshot of django-celery-results using the following
pip command::

    $ pip install https://github.com/celery/django-celery-results/zipball/master#egg=django-celery-results


Issues with mysql
-----------------

If you want to run ``django-celery-results`` with MySQL, you might run into some issues.

One such issue is when you try to run ``python manage.py migrate django_celery_results``, you might get the following error::

    django.db.utils.OperationalError: (1071, 'Specified key was too long; max key length is 767 bytes')

To get around this issue, you can set::

    DJANGO_CELERY_RESULTS_TASK_ID_MAX_LENGTH=191

(or any other value if any other db other than MySQL is causing similar issues.)

max_length of **191** seems to work for MySQL.


.. |build-status| image:: https://secure.travis-ci.org/celery/django-celery-results.svg?branch=master
    :alt: Build status
    :target: https://travis-ci.org/celery/django-celery-results

.. |coverage| image:: https://codecov.io/github/celery/django-celery-results/coverage.svg?branch=master
    :target: https://codecov.io/github/celery/django-celery-results?branch=master

.. |license| image:: https://img.shields.io/pypi/l/django-celery-results.svg
    :alt: BSD License
    :target: https://opensource.org/licenses/BSD-3-Clause

.. |wheel| image:: https://img.shields.io/pypi/wheel/django-celery-results.svg
    :alt: django-celery-results can be installed via wheel
    :target: https://pypi.python.org/pypi/django-celery-results/

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/django-celery-results.svg
    :alt: Supported Python versions.
    :target: https://pypi.python.org/pypi/django-celery-results/

.. |pyimp| image:: https://img.shields.io/pypi/implementation/django-celery-results.svg
    :alt: Support Python implementations.
    :target: https://pypi.python.org/pypi/django-celery-results/

django-celery-results for enterprise
------------------------------------

Available as part of the Tidelift Subscription.

The maintainer of django-celery-results and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open source packages you use to build your applications. Save time, reduce risk, and improve code health, while paying the maintainer of the exact packages you use. `Learn more. <https://tidelift.com/subscription/pkg/pypi-django-celery-results?utm_source=pypi-django-celery-results&utm_medium=referral&utm_campaign=readme>`_

