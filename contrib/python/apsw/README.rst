APSW stands for **A**\ nother **P**\ ython **S**\ QLite **W**\ rapper.   APSW
supports CPython 3.6 onwards.

About
=====

APSW is a Python wrapper for the `SQLite <https://sqlite.org/>`__
embedded relational database engine.  It focuses translating between
the complete `SQLite C API <https://sqlite.org/c3ref/intro.html>`__
and `Python's C API <https://docs.python.org/3/c-api/index.html>`__,
letting you get the most out of SQLite from Python.

It is recommended to use the builtin `sqlite3 module
<https://docs.python.org/3/library/sqlite3.html>`__ if you want SQLite
to be interchangeable with the other database drivers.

Use APSW when you want to use SQLite fully, and have an improved
developer experience.  The `documentation
<https://rogerbinns.github.io/apsw/pysqlite.html>`__ has a section on
the differences between APSW and sqlite3.

Help/Documentation
==================

There is a tour and example code using APSW at
https://rogerbinns.github.io/apsw/example.html

The latest documentation is at https://rogerbinns.github.io/apsw/

Mailing lists/contacts
======================

* `Python SQLite discussion group <http://groups.google.com/group/python-sqlite>`__
  (preferred)
* You can also email the author at `rogerb@rogerbinns.com
  <mailto:rogerb@rogerbinns.com>`__

Releases and Changes
====================

Releases are made to `PyPI <https://pypi.org/project/apsw/>`__
(install using pip) and `Github
<https://github.com/rogerbinns/apsw/releases>`__

New release announcements are sent to the `Python SQLite discussion
group <http://groups.google.com/group/python-sqlite>`__ and there is
an `RSS feed from PyPI
<https://pypi.org/rss/project/apsw/releases.xml>`__.

`Full detailed list of changes <http://rogerbinns.github.io/apsw/changes.html>`__

Bugs
====

You can find existing and fixed bugs by clicking on `Issues
<https://github.com/rogerbinns/apsw/issues>`__ and using "New Issue"
to report previously unknown issues.

License
=======

See `LICENSE
<https://github.com/rogerbinns/apsw/blob/master/LICENSE>`__ - in
essence any OSI approved open source license.

Older Python versions
=====================

A `release
<https://www.rogerbinns.com/blog/apsw-ending-python2early3.html>`__
from January 2022 supports all CPython versions back to 2.3.  The
`tips <https://rogerbinns.github.io/apsw/tips.html>`__ include more
information about versions.
