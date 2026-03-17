Welcome to apipkg !
-------------------

With apipkg you can control the exported namespace of a Python package and
greatly reduce the number of imports for your users.
It is a `small pure Python module`_ that works on CPython 3.7+,
Jython and PyPy. It cooperates well with Python's ``help()`` system,
custom importers (PEP302) and common command-line completion tools.

Usage is very simple: you can require 'apipkg' as a dependency or you
can copy paste the ~200 lines of code into your project.


Tutorial example
-------------------

Here is a simple ``mypkg`` package that specifies one namespace
and exports two objects imported from different modules:

.. code-block:: python

    # mypkg/__init__.py
    import apipkg
    apipkg.initpkg(__name__, {
        'path': {
            'Class1': "_mypkg.somemodule:Class1",
            'clsattr': "_mypkg.othermodule:Class2.attr",
        }
    }

The package is initialized with a dictionary as namespace.

You need to create a ``_mypkg`` package with a ``somemodule.py``
and ``othermodule.py`` containing the respective classes.
The ``_mypkg`` is not special - it's a completely
regular Python package.

Namespace dictionaries contain ``name: value`` mappings
where the value may be another namespace dictionary or
a string specifying an import location.  On accessing
an namespace attribute an import will be performed:

.. code-block:: pycon

    >>> import mypkg
    >>> mypkg.path
    <ApiModule 'mypkg.path'>
    >>> mypkg.path.Class1   # '_mypkg.somemodule' gets imported now
    <class _mypkg.somemodule.Class1 at 0xb7d428fc>
    >>> mypkg.path.clsattr  # '_mypkg.othermodule' gets imported now
    4 # the value of _mypkg.othermodule.Class2.attr

The ``mypkg.path`` namespace and its two entries are
loaded when they are accessed.   This means:

* lazy loading - only what is actually needed is ever loaded

* only the root "mypkg" ever needs to be imported to get
  access to the complete functionality

* the underlying modules are also accessible, for example:

.. code-block:: python

    from mypkg.sub import Class1


Including apipkg in your package
--------------------------------------

If you don't want to add an ``apipkg`` dependency to your package you
can copy the `apipkg.py`_ file somewhere to your own package,
for example ``_mypkg/apipkg.py`` in the above example.  You
then import the ``initpkg`` function from that new place and
are good to go.

.. _`small pure Python module`:
.. _`apipkg.py`: https://github.com/pytest-dev/apipkg/blob/master/src/apipkg/__init__.py

Feedback?
-----------------------

If you have questions you are welcome to

* join the **#pytest** channel on irc.libera.chat_
  (using an IRC client, via webchat_, or via Matrix_).
* create an issue on the bugtracker_

.. _irc.libera.chat: ircs://irc.libera.chat:6697/#pytest
.. _webchat: https://web.libera.chat/#pytest
.. _matrix: https://matrix.to/#/%23pytest:libera.chat
.. _bugtracker: https://github.com/pytest-dev/apipkg/issues
