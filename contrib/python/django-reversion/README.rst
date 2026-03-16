================
django-reversion
================

|PyPI latest| |PyPI Version| |PyPI License| |Github actions| |Docs|


**django-reversion** is an extension to the Django web framework that provides
version control for model instances.

Requirements
============

- Python 3.8 or later
- Django 4.2 or later

Features
========

-  Roll back to any point in a model instance's history.
-  Recover deleted model instances.
-  Simple admin integration.

Documentation
=============

Check out the latest ``django-reversion`` documentation at `Getting Started <http://django-reversion.readthedocs.io/>`_


Issue tracking and source code can be found at the
`main project website <http://github.com/etianen/django-reversion>`_.

You can keep up to date with the latest announcements by joining the
`django-reversion discussion group <http://groups.google.com/group/django-reversion>`_.

Upgrading
=========

Please check the `Changelog <https://github.com/etianen/django-reversion/blob/master/CHANGELOG.rst>`_ before upgrading
your installation of django-reversion.

Contributing
============

Bug reports, bug fixes, and new features are always welcome. Please raise issues on the
`django-reversion project site <http://github.com/etianen/django-reversion>`_, and submit
pull requests for any new code.

1. Fork the `repository <http://github.com/etianen/django-reversion>`_ on GitHub.
2. Make a branch off of master and commit your changes to it.
3. Install requirements.

.. code:: bash

    $ pip install django psycopg2 mysqlclient -e .

4. Run the tests

.. code:: bash

    $ tests/manage.py test tests

5. Create a Pull Request with your contribution

Contributors
============

The django-reversion project was developed by `Dave Hall <http://www.etianen.com/>`_ and contributed
to by `many other people <https://github.com/etianen/django-reversion/graphs/contributors>`_.


.. |Docs| image:: https://readthedocs.org/projects/django-reversion/badge/?version=latest
   :target: http://django-reversion.readthedocs.org/en/latest/?badge=latest
.. |PyPI Version| image:: https://img.shields.io/pypi/pyversions/django-reversion.svg?maxAge=60
   :target: https://pypi.python.org/pypi/django-reversion
.. |PyPI License| image:: https://img.shields.io/pypi/l/django-reversion.svg?maxAge=120
   :target: https://github.com/rhenter/django-reversion/blob/master/LICENSE
.. |PyPI latest| image:: https://img.shields.io/pypi/v/django-reversion.svg?maxAge=120
   :target: https://pypi.python.org/pypi/django-reversion
.. |Github actions| image:: https://github.com/etianen/django-reversion/workflows/Python%20package/badge.svg
   :target: https://github.com/etianen/django-reversion
