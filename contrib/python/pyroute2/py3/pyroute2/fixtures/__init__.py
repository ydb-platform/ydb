'''
..
    8<-----------------------------------------------------------
    Doctest setup.

    It is not rendered into html. For the docs, skip to tne
    CI test fixtures section below.

.. testsetup:: *

    from pyroute2.fixtures.doctest import *

..
    End of the doctest setup.
    8<-----------------------------------------------------------


CI test fixtures
----------------

Added in version 0.9.1.

The library provides a set of fixtures that can be used with pytest
to setup a simple test environment for functional network tests.

Usage:

.. testcode::

    # file: conftest.py
    pytest_plugins = [
        'pyroute2.fixtures.iproute',
        'pyroute2.fixtures.ndb',
        'pyroute2.fixtures.plan9'
    ]

    # file: my_test.py
    def test_my_code(sync_context):
        # here you have access to
        #
        sync_context.ipr        # IPRoute instance running in a netns
        sync_context.netns      # ready to use netns with a test link up
        sync_context.test_link  # test link in the netns
        sync_context.test_link.index    # interface index
        sync_context.test_link.ifname   # interface name
        sync_context.test_link.address  # MAC address

The fixtures set up a network namespace with a unique name, a dummy
interface within the namespace, and bring the interface up. They form
a tree of dependencies, so if you use e.g. `test_link_ifname` fixture,
you may be sure that the namespace and the interface are already set
up properly.

Fixtures dependencies diagram:

.. aafig::
    :scale: 80
    :textual:

                        +---------------------+
                        | `test_link`         |--+
                        +---------------------+  |
                             ^           |       v
    +---------------------+  |           |  +----------------------+
    | `test_link_index`   |__+           |  | `test_link_ifinfmsg` |
    |                     |  |           |  |                      |
    +---------------------+  |           |  +----------------------+
                             |           |       |
    +---------------------+  |           |       v
    | `test_link_address` |__+           |  +----------------------+
    |                     |  |           +->| netns                |
    +---------------------+  |           |  |                      |
                             |           |  +----------------------+
    +---------------------+  |           |
    | `test_link_ifname`  |__+           |
    |                     |  |           |
    +---------------------+  |           |
                             |           |
    +---------------------+  |           |
    | `async_context`     |__+           |
    |                     |_ | ___       |
    +---------------------+  |    |      |
                             |    |      |
    +---------------------+  |    |      |
    | `sync_context`      |__|    |      |
    |                     |_____  |      |
    +---------------------+     | |      |
                                | |      |
    +---------------------+     | |      |
    | `sync_ipr`          |<----+ |      |
    |                     |______ | _____+
    +---------------------+       |      |
                                  |      |
    +---------------------+       |      |
    | `async_ipr`         |<------+      |
    |                     |______________+
    +---------------------+              |
                                         |
    +---------------------+              |
    | `ndb`               |______________|
    |                     |
    +---------------------+

.. autofunction:: pyroute2.fixtures.iproute.nsname

.. autofunction:: pyroute2.fixtures.iproute.test_link_ifinfmsg

.. autofunction:: pyroute2.fixtures.iproute.test_link

.. autofunction:: pyroute2.fixtures.iproute.test_link_address

.. autofunction:: pyroute2.fixtures.iproute.test_link_index

.. autofunction:: pyroute2.fixtures.iproute.test_link_ifname

.. autofunction:: pyroute2.fixtures.iproute.async_ipr

.. autofunction:: pyroute2.fixtures.iproute.sync_ipr

.. autofunction:: pyroute2.fixtures.iproute.async_context

.. autofunction:: pyroute2.fixtures.iproute.sync_context

.. autofunction:: pyroute2.fixtures.ndb.ndb

.. autoclass:: pyroute2.fixtures.iproute.TestInterface
    :members:

.. autoclass:: pyroute2.fixtures.iproute.TestContext
    :members:

'''
