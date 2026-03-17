Momoko
======

.. image:: https://img.shields.io/pypi/v/momoko.svg
    :target: https://pypi.python.org/pypi/momoko

.. image:: https://img.shields.io/travis/FSX/momoko.svg
        :target: https://travis-ci.org/FSX/momoko

Momoko wraps Psycopg2_'s functionality for use in Tornado_. Have a look at tutorial_ or full documentation_.

**Important:** This is the 2.x version of Momoko. It requires 4.0 <= Tornado < **6.0**, uses futures instead of calllbacks
and introduces a slightly different API compared to 1.x version. While transition is very
straightforward, the API is not backward compatible with 1.x!

.. _Psycopg2: http://initd.org/psycopg/
.. _Tornado: http://www.tornadoweb.org/
.. _tutorial: http://momoko.readthedocs.org/en/master/tutorial.html
.. _documentation: http://momoko.readthedocs.org/en/master

Maintainer wanted
-----------------
Unfortunately none of the developers of this project actively use it anymore in their work. Test-covered pull requests will be happily accepted, but no active development is planned so far. If you have serious intentions to maintain this project, please get in touch.

Installation
------------

With pip::

    pip install momoko

Or manually::

    python setup.py install


Testing
-------

Set the following environment variables with your own values before running the
unit tests::

    make -C tcproxy
    export MOMOKO_TEST_DB='your_db'
    export MOMOKO_TEST_USER='your_user'
    export MOMOKO_TEST_PASSWORD='your_password'
    export MOMOKO_TEST_HOST='localhost'
    export MOMOKO_TEST_PORT='5432'

And run the tests with::

    python setup.py test
