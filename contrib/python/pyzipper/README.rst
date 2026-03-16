.. image:: https://img.shields.io/pypi/v/pyzipper.svg
        :target: https://pypi.org/project/pyzipper/
        :alt: Current Version on PyPi

.. image:: https://img.shields.io/pypi/pyversions/pyzipper.svg
        :target: https://pypi.org/project/pyzipper/
        :alt: Supported Python Versions


pyzipper
========

A replacement for Python's ``zipfile`` that can read and write AES encrypted
zip files. Forked from Python 3.7's ``zipfile`` module, it features the same
``zipfile`` API from that time (most notably, lacking support for
``pathlib``-compatible wrappers that were introduced in Python 3.8).

Installation
------------

.. code-block:: bash

   pip install pyzipper


Usage
-----

.. code-block:: python

   import pyzipper

   secret_password = b'lost art of keeping a secret'

   with pyzipper.AESZipFile('new_test.zip',
                            'w',
                            compression=pyzipper.ZIP_LZMA,
                            encryption=pyzipper.WZ_AES) as zf:
       zf.setpassword(secret_password)
       zf.writestr('test.txt', "What ever you do, don't tell anyone!")

   with pyzipper.AESZipFile('new_test.zip') as zf:
       zf.setpassword(secret_password)
       my_secrets = zf.read('test.txt')


AES Strength
------------

The strength of the AES encryption can be configure to be 128, 192 or 256 bits.
By default it is 256 bits. Use the ``setencryption()`` method to specify the
encryption kwargs:

.. code-block:: python

   import pyzipper

   secret_password = b'lost art of keeping a secret'

   with pyzipper.AESZipFile('new_test.zip',
                            'w',
                            compression=pyzipper.ZIP_LZMA) as zf:
       zf.setpassword(secret_password)
       zf.setencryption(pyzipper.WZ_AES, nbits=128)
       zf.writestr('test.txt', "What ever you do, don't tell anyone!")

   with pyzipper.AESZipFile('new_test.zip') as zf:
       zf.setpassword(secret_password)
       my_secrets = zf.read('test.txt')

Documentation
-------------

Official Python ZipFile documentation is available here: https://docs.python.org/3/library/zipfile.html

Credits
-------

The docs skeleton was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
