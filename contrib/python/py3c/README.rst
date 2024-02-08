py3c
====

py3c helps you port C extensions to Python 3.

It provides a detailed guide, and a set of macros to make porting easy
and reduce boilerplate.


Design principles
-----------------

* Reduce chances of accidental subtle errors
* Minimize boilerplate
* Prefer the Python 3 way of doing things
* Minimize semantic changes under Python 2


Versions
--------

Projects using py3c will be compatible with CPython 2.6, 2.7, and 3.3+.


Guides
------

A detailed `porting guide`_ is provided.

A `cheatsheet`_ is available for those that already know the Python C API,
and want to know what the py3c macros do.

.. _porting guide: http://py3c.readthedocs.org/en/latest/guide.html
.. _cheatsheet: http://py3c.readthedocs.org/en/latest/cheatsheet.html


Installation
------------

If your build system supports pkg-config, you can set it up
to look for a system-wide installation of py3c.

Alternately, py3c can be used as a header-only library:
copy the headers to your project and use them.

If you wish to install py3c system-wide (e.g. if you are a distro packager),
see the Contributing_ chapter in the documentation.

.. _Contributing: http://py3c.readthedocs.org/en/latest/contributing.html


Contribute
----------

- Issue Tracker: http://github.com/encukou/py3c/issues
- Source Code: http://github.com/encukou/py3c


License
-------

The code is licensed under the MIT license. May it serve you well.

The documentation is licensed under CC-BY-SA 3.0.

See the files LICENSE.MIT and doc/LICENSE.CC-BY-SA-3.0.
