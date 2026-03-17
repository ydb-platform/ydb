pycares: Python interface for c-ares
====================================

.. image:: https://badge.fury.io/py/pycares.png
    :target: https://pypi.org/project/pycares/

.. image:: https://github.com/saghul/pycares/workflows/Test/badge.svg
    :target: https://github.com/saghul/pycares/actions

pycares is a Python module which provides an interface to c-ares.
`c-ares <https://c-ares.org>`_ is a C library that performs
DNS requests and name resolutions asynchronously.


Documentation
-------------

https://pycares.readthedocs.io/latest/


Bundled c-ares
--------------

pycares currently bundles c-ares as a submodule for ease of building. Using the system
provided c-ares is possible if the ``PYCARES_USE_SYSTEM_LIB`` environment variable is
set to ``1`` when building.

NOTE: Versions prior to 4.0.0 used to embed a modified c-ares with extended TTL support.
That is no longer the case and as a result only A and AAAA records will have TTL information.
Follow this PR in uppstream c-ares, looks like TTLs will be added: https://github.com/c-ares/c-ares/pull/393


Installation
------------

GNU/Linux, macOS, Windows, others:

::

    pip install pycares

FreeBSD:

::

    cd /usr/ports/dns/py-pycares && make install


IDNA 2008 support
^^^^^^^^^^^^^^^^^

If the ``idna`` package is installed, pycares will support IDNA 2008 encoding otherwise the builtin idna codec will be used,
which provides IDNA 2003 support.

You can force this at installation time as follows:

::

   pip install pycares[idna]


Running the test suite
----------------------

From the top level directory, run: ``python -m unittest -v``

NOTE: Running the tests requires internet access and are somewhat environment sensitive because real DNS quesries
are made, there is no mocking. If you observe a failure that the CI cannot reproduce, please try to setup an
environment as close as the current CI.


Using it from the cli, a la dig
-------------------------------

This module can be used directly from the command line in a similar fashion to dig (limited, of course):

::

   $ python -m pycares google.com
   ;; QUESTION SECTION:
   ;google.com			IN	A

   ;; ANSWER SECTION:
   google.com		300	IN	A	172.217.17.142

   $ python -m pycares mx google.com
   ;; QUESTION SECTION:
   ;google.com			IN	MX

   ;; ANSWER SECTION:
   google.com		600	IN	MX	50 alt4.aspmx.l.google.com
   google.com		600	IN	MX	10 aspmx.l.google.com
   google.com		600	IN	MX	40 alt3.aspmx.l.google.com
   google.com		600	IN	MX	20 alt1.aspmx.l.google.com
   google.com		600	IN	MX	30 alt2.aspmx.l.google.com


Author
------

Saúl Ibarra Corretgé <s@saghul.net>


License
-------

Unless stated otherwise on-file pycares uses the MIT license, check LICENSE file.


Supported Python versions
-------------------------

Python >= 3.9 are supported. Both CPython and PyPy are supported.


Contributing
------------

If you'd like to contribute, fork the project, make a patch and send a pull
request. Have a look at the surrounding code and please, make yours look
alike :-)
