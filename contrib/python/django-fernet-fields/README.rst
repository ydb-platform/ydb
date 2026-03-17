====================
django-fernet-fields
====================

.. image:: https://secure.travis-ci.org/orcasgit/django-fernet-fields.png?branch=master
   :target: http://travis-ci.org/orcasgit/django-fernet-fields
   :alt: Test status
.. image:: https://coveralls.io/repos/orcasgit/django-fernet-fields/badge.png?branch=master
   :target: https://coveralls.io/r/orcasgit/django-fernet-fields
   :alt: Test coverage
.. image:: https://readthedocs.org/projects/django-fernet-fields/badge/?version=latest
   :target: https://readthedocs.org/projects/django-fernet-fields/?badge=latest
   :alt: Documentation Status
.. image:: https://badge.fury.io/py/django-fernet-fields.svg
   :target: https://pypi.python.org/pypi/django-fernet-fields
   :alt: Latest version

`Fernet`_ symmetric encryption for Django model fields, using the
`cryptography`_ library.

``django-fernet-fields`` supports `Django`_ 1.11 and later on Python 2.7, 3.5, 3.6, 3.7, pypy, and pypy3.

Only PostgreSQL, SQLite, and MySQL are tested, but any Django database backend
with support for ``BinaryField`` should work.

.. _Django: http://www.djangoproject.com/
.. _Fernet: https://cryptography.io/en/latest/fernet/
.. _cryptography: https://cryptography.io/en/latest/


Getting Help
============

Documentation for django-fernet-fields is available at
https://django-fernet-fields.readthedocs.org/

This app is available on `PyPI`_ and can be installed with ``pip install
django-fernet-fields``.

.. _PyPI: https://pypi.python.org/pypi/django-fernet-fields/


Contributing
============

See the `contributing docs`_.

.. _contributing docs: https://github.com/orcasgit/django-fernet-fields/blob/master/CONTRIBUTING.rst

