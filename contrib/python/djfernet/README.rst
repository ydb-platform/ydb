========
djfernet
========

Maintained fork of github.com/orcasgit/django-fernet-fields used by
yourlabs.io/oss/djwebdapp

`Fernet`_ symmetric encryption for Django model fields, using the
`cryptography`_ library.

``djfernet`` supports `Django`_ 4.0 and later on Python 3.

Only PostgreSQL, SQLite, and MySQL are tested, but any Django database backend
with support for ``BinaryField`` should work.

.. _Django: http://www.djangoproject.com/
.. _Fernet: https://cryptography.io/en/latest/fernet/
.. _cryptography: https://cryptography.io/en/latest/

.. danger:: If you have data created with djfernet < 0.8, you will need the
            following setting to be able to decrypt existing data:
            ``DJFERNET_PREFIX = b'djfernet'``. Make sure you use a bytestring
            right there with ``b'..'``!!

Getting Help
============

Documentation for djfernet is available at
https://djfernet.readthedocs.org/

This app is available on `PyPI`_ and can be installed with ``pip install
djfernet``.

.. _PyPI: https://pypi.python.org/pypi/djfernet/
