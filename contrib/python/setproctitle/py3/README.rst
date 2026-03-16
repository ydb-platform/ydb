A Python module to customize the process title
==============================================

.. image:: https://github.com/dvarrazzo/py-setproctitle/workflows/Tests/badge.svg
    :target: https://github.com/dvarrazzo/py-setproctitle/actions?query=workflow%3ATests
    :alt: Tests

:author: Daniele Varrazzo

The ``setproctitle`` module allows a process to change its title (as displayed
by system tools such as ``ps``, ``top`` or MacOS Activity Monitor).

Changing the title is mostly useful in multi-process systems, for example
when a master process is forked: changing the children's title allows to
identify the task each process is busy with.  The technique is used by
PostgreSQL_ and the `OpenSSH Server`_ for example.

The procedure is hardly portable across different systems.  PostgreSQL provides
a good `multi-platform implementation`__:  this package was born as a wrapper
around PostgreSQL code.

- `Homepage <https://github.com/dvarrazzo/py-setproctitle>`__
- `Download <http://pypi.python.org/pypi/setproctitle/>`__
- `Bug tracker <https://github.com/dvarrazzo/py-setproctitle/issues>`__


.. _PostgreSQL: http://www.postgresql.org
.. _OpenSSH Server: http://www.openssh.com/
.. __: http://doxygen.postgresql.org/ps__status_8c_source.html


Installation
------------

``setproctitle`` is a C extension: in order to build it you will need a C
compiler and the Python development support (the ``python-dev`` or
``python3-dev`` package in most Linux distributions). No further external
dependencies are required.

You can use ``pip`` to install the module::

    pip install setproctitle

You can use ``pip -t`` or ``virtualenv`` for local installations, ``sudo pip``
for a system-wide one... the usual stuff. Read pip_ or virtualenv_ docs for
all the details.

.. _pip: https://pip.readthedocs.org/
.. _virtualenv: https://virtualenv.readthedocs.org/


Usage
-----

.. note::
   You should import and use the module (even just calling ``getproctitle()``)
   pretty early in your program lifetime: code writing env vars `may
   interfere`__ with the module initialisation.

    .. __: https://github.com/dvarrazzo/py-setproctitle/issues/42


The ``setproctitle`` module exports the following functions:

``setproctitle(title)``
    Set *title* as the title for the current process.

``getproctitle()``
    Return the current process title.

The process title is usually visible in files such as ``/proc/PID/cmdline``,
``/proc/PID/status``, ``/proc/PID/comm``, depending on the operating system
and kernel version. These information are used by user-space tools such as
``ps`` and ``top``.


``setthreadtitle(title)``
    Set *title* as the title for the current thread.

``getthreadtitle()``
    Get the current thread title.

The thread title is exposed by some operating systems as the file
``/proc/PID/task/TID/comm``, which is used by certain tools such as ``htop``.


Environment variables
~~~~~~~~~~~~~~~~~~~~~

A few environment variables can be used to customize the module behavior:

``SPT_NOENV``
    Avoid clobbering ``/proc/PID/environ``.

    On many platforms, setting the process title will clobber the
    ``environ`` memory area. ``os.environ`` will work as expected from within
    the Python process, but the content of the file ``/proc/PID/environ`` will
    be overwritten.  If you require this file not to be broken you can set the
    ``SPT_NOENV`` environment variable to any non-empty value: in this case
    the maximum length for the title will be limited to the length of the
    command line.

``SPT_DEBUG``
    Print debug information on ``stderr``.

    If the module doesn't work as expected you can set this variable to a
    non-empty value to generate information useful for debugging.  Note that
    the most useful information is printed when the module is imported, not
    when the functions are called.


Module status
-------------

The module can be currently compiled and effectively used on the following
platforms:

- GNU/Linux
- BSD
- MacOS X
- Windows

Note that on Windows there is no way to change the process string:
what the module does is to create a *Named Object* whose value can be read
using a tool such as `Process Explorer`_ (contribution of a more useful tool
to be used together with ``setproctitle`` would be well accepted).

The module can probably work on HP-UX, but I haven't found any to test with.
It is unlikely that it can work on Solaris instead.

.. _Process Explorer: http://technet.microsoft.com/en-us/sysinternals/bb896653.aspx
