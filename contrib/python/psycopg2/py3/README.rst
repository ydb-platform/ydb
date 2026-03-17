psycopg2 - Python-PostgreSQL Database Adapter
=============================================

Psycopg is the most popular PostgreSQL database adapter for the Python
programming language.  Its main features are the complete implementation of
the Python DB API 2.0 specification and the thread safety (several threads can
share the same connection).  It was designed for heavily multi-threaded
applications that create and destroy lots of cursors and make a large number
of concurrent "INSERT"s or "UPDATE"s.

Psycopg 2 is mostly implemented in C as a libpq wrapper, resulting in being
both efficient and secure.  It features client-side and server-side cursors,
asynchronous communication and notifications, "COPY TO/COPY FROM" support.
Many Python types are supported out-of-the-box and adapted to matching
PostgreSQL data types; adaptation can be extended and customized thanks to a
flexible objects adaptation system.

Psycopg 2 is both Unicode and Python 3 friendly.

.. Note::

    The psycopg2 package is still widely used and actively maintained, but it
    is not expected to receive new features.

    `Psycopg 3`__ is the evolution of psycopg2 and is where `new features are
    being developed`__: if you are starting a new project you should probably
    start from 3!

    .. __: https://pypi.org/project/psycopg/
    .. __: https://www.psycopg.org/psycopg3/docs/index.html


Documentation
-------------

Documentation is included in the ``doc`` directory and is `available online`__.

.. __: https://www.psycopg.org/docs/

For any other resource (source code repository, bug tracker, mailing list)
please check the `project homepage`__.

.. __: https://psycopg.org/


Installation
------------

Building Psycopg requires a few prerequisites (a C compiler, some development
packages): please check the install_ and the faq_ documents in the ``doc`` dir
or online for the details.

If prerequisites are met, you can install psycopg like any other Python
package, using ``pip`` to download it from PyPI_::

    $ pip install psycopg2

or using ``setup.py`` if you have downloaded the source package locally::

    $ python setup.py build
    $ sudo python setup.py install

You can also obtain a stand-alone package, not requiring a compiler or
external libraries, by installing the `psycopg2-binary`_ package from PyPI::

    $ pip install psycopg2-binary

The binary package is a practical choice for development and testing but in
production it is advised to use the package built from sources.

.. _PyPI: https://pypi.org/project/psycopg2/
.. _psycopg2-binary: https://pypi.org/project/psycopg2-binary/
.. _install: https://www.psycopg.org/docs/install.html#install-from-source
.. _faq: https://www.psycopg.org/docs/faq.html#faq-compile

:Build status: |gh-actions|

.. |gh-actions| image:: https://github.com/psycopg/psycopg2/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/psycopg/psycopg2/actions/workflows/tests.yml
    :alt: Build status
