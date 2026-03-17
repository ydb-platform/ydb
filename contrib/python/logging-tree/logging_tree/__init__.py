"""Introspection for the ``logging`` logger tree in the Standard Library.

You can install this package with the standard ``pip`` command::

    $ pip install logging_tree

The simplest way to use this package is to call ``printout()`` to see
the loggers, filters, and handlers that your application has configured:

    >>> import logging
    >>> a = logging.getLogger('a')
    >>> b = logging.getLogger('a.b').setLevel(logging.DEBUG)
    >>> c = logging.getLogger('x.c')

    >>> import sys
    >>> h = logging.StreamHandler(sys.stdout)
    >>> logging.getLogger().addHandler(h)

    >>> from logging_tree import printout
    >>> printout()
    <--""
       Level WARNING
       Handler Stream <sys.stdout>
       |
       o<--"a"
       |   Level NOTSET so inherits level WARNING
       |   |
       |   o<--"a.b"
       |       Level DEBUG
       |
       o<--[x]
           |
           o<--"x.c"
               Level NOTSET so inherits level WARNING

If you instead want to write the tree diagram to a file, stream, or
other file-like object, use::

    file_object.write(logging_tree.format.build_description())

The logging tree should always print successfully, no matter how
complicated.  A node whose name is in square brackets, like the ``[x]``
node above, is a "place holder" that has never itself been named in a
``getLogger()`` call, but which was created automatically to serve as
the parent of loggers further down the tree.

Propagation
-----------

A quick reminder about how ``logging`` works: by default, a node will
not only submit a message to its own handlers (if any), but will also
"propagate" each message up to its parent.  For example, a ``Stream``
handler attached to the root logger will not only receive messages sent
directly to the root, but also messages that propagate up from a child
like ``a.b``.

    >>> logging.getLogger().warning('message sent to the root')
    message sent to the root
    >>> logging.getLogger('a.b').warning('message sent to a.b')
    message sent to a.b

But messages are *not* subjected to filtering as they propagate.  So a
debug-level message, which our root node will discard because the root's
level is set to ``WARNING``, will be accepted by the ``a.b`` node and
will be allowed to propagate up to the root handler.

    >>> logging.getLogger().debug('this message is ignored')
    >>> logging.getLogger('a.b').debug('but this message prints!')
    but this message prints!

If both the root node and ``a.b`` have a handler attached, then a
message accepted by ``a.b`` will be printed twice, once by its own node,
and then a second time when the message propagates up to the root.

    >>> logging.getLogger('a.b').addHandler(h)
    >>> logging.getLogger('a.b').warning('this message prints twice')
    this message prints twice
    this message prints twice

But you can stop a node from propagating messages to its parent by
setting its ``propagate`` attribute to ``False``.

    >>> logging.getLogger('a.b').propagate = False
    >>> logging.getLogger('a.b').warning('does not propagate')
    does not propagate

The logging tree will indicate that propagate is turned off by no longer
drawing the arrow ``<--`` that points from the node to its parent:

    >>> printout()
    <--""
       Level WARNING
       Handler Stream <sys.stdout>
       |
       o<--"a"
       |   Level NOTSET so inherits level WARNING
       |   |
       |   o   "a.b"
       |       Level DEBUG
       |       Propagate OFF
       |       Handler Stream <sys.stdout>
       |
       o<--[x]
           |
           o<--"x.c"
               Level NOTSET so inherits level WARNING

You can turn propagate back on again by setting the attribute ``True``.

API
---

Even though most users will simply call the top-level ``printout()``
routine, this package also offers a few lower-level calls.  Here's the
complete list:

``logging_tree.printout(node=None)``

    Prints the current logging tree, or the tree based at the given
    `node`, to the standard output.

``logging_tree.format.build_description(node=None)``

    Builds and returns the multi-line description of the current logger
    tree, or the tree based at the given ``node``, as a single string
    with newlines inside and a newline at the end.

``logging_tree.format.describe(node)``

    A generator that yields a series of lines that describe the tree
    based at the given ``node``.  Note that the lines are returned
    without newline terminators attached.

``logging_tree.tree()``

    Fetch the current tree of loggers from the ``logging`` module.
    Returns a node, that is simply a tuple with three fields:

    | ``[0]`` the logger name (``""`` for the root logger).
    | ``[1]`` the ``logging.Logger`` object itself.
    | ``[2]`` a list of zero or more child nodes.

You can find this package's issue tracker `on GitHub
<https://github.com/brandon-rhodes/logging_tree>`_.  You can run this
package's test suite with::

    $ python -m unittest discover logging_tree

On older versions of Python you will instead have to install
``unittest2`` and use its ``unit2`` command line tool to run the tests.

Changelog
---------

**Version 1.10** - 2024 May 3
    Declare compatibility with Python 3.12, and expand the documentation
    to describe the basics of log message propagation.

**Version 1.9** - 2021 April 10
    Declare compatibility with Python 3.9.  Improve how the logging
    module's built-in ``Formatter`` class is displayed under old Python
    versions where the ``logging`` module uses old-style classes.

**Version 1.8.1** - 2020 January 26
    Adjust one test to make it pass under Python 3.8, and update the
    distribution classifiers to declare compatibility with Python
    versions through 3.8.

**Version 1.8** - 2018 August 5
    Improve the output to better explain what happens if a "parent"
    attribute has been set to None.

**Version 1.7** - 2016 January 23
    Detect whether each logger has the correct "parent" attribute and,
    if not, print where its log messages are being sent instead.

**Version 1.6** - 2015 January 8
    Fixed a crash that would occur if a custom logging Formatter was
    missing its format string attributes.

**Version 1.5** - 2014 December 24
    Handlers now display their logging level if one has been set, and
    their custom logging formatter if one has been installed.

**Version 1.4** - 2014 January 8
    Thanks to a contribution from Dave Brondsema, disabled loggers are
    now actually marked as "Disabled" to make it less of a surprise that
    they fail to log anything.

**Version 1.3** - 2013 October 29
    Be explicit and display the logger level ``NOTSET`` along with the
    effective level inherited from the logger's ancestors; and display
    the list of ``.filters`` of a custom logging handler even though it
    might contain custom code that ignores them.

**Version 1.2** - 2013 January 19
    Compatible with Python 3.3 thanks to @ralphbean.

**Version 1.1** - 2012 February 17
    Now compatible with 2.3 <= Python <= 3.2.

**Version 1.0** - 2012 February 13
    Can display the handler inside a MemoryHandler; entire public
    interface documented; 100% test coverage.

**Version 0.6** - 2012 February 10
    Added a display format for every ``logging.handlers`` class.

**Version 0.5** - 2012 February 8
    Initial release.

"""
__version__ = '1.10'
__all__ = ('tree', 'printout')

from logging_tree.nodes import tree
from logging_tree.format import printout
