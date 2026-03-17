==========
pyelftools
==========

.. image:: https://github.com/eliben/pyelftools/workflows/pyelftools-tests/badge.svg
  :align: center
  :target: https://github.com/eliben/pyelftools/actions

**pyelftools** is a pure-Python library for parsing and analyzing ELF files
and DWARF debugging information. See the
`User's guide <https://github.com/eliben/pyelftools/wiki/User's-guide>`_
for more details.

Pre-requisites
--------------

As a user of **pyelftools**, one only needs Python 3 to run. While there is no
reason for the library to not work on earlier versions of Python, our CI
tests are based on the official
`Status of Python versions <https://devguide.python.org/versions/>`__.

Installing
----------

**pyelftools** can be installed from PyPI (Python package index)::

    > pip install pyelftools

Alternatively, you can download the source distribution for the most recent and
historic versions from the *Downloads* tab on the `pyelftools project page
<https://github.com/eliben/pyelftools>`_ (by going to *Tags*). Then, you can
install from source, as usual::

    > python setup.py install

Since **pyelftools** is a work in progress, it's recommended to have the most
recent version of the code. This can be done by downloading the `master zip
file <https://github.com/eliben/pyelftools/archive/master.zip>`_ or just
cloning the Git repository.

Since **pyelftools** has no external dependencies, it's also easy to use it
without installing, by locally adjusting ``PYTHONPATH``.

How to use it?
--------------

**pyelftools** is a regular Python library: you import and invoke it from your
own code. For a detailed usage guide and links to examples, please consult the
`user's guide <https://github.com/eliben/pyelftools/wiki/User's-guide>`_.

Contributing
------------

See the `Hacking Guide <https://github.com/eliben/pyelftools/wiki/Hacking-guide>`__.

License
-------

**pyelftools** is open source software. Its code is in the public domain. See
the ``LICENSE`` file for more details.
