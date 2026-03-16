Zake
====

.. image:: https://travis-ci.org/yahoo/Zake.png?branch=master
   :target: https://travis-ci.org/yahoo/Zake


A python package that works to provide a nice set of testing utilities for the `kazoo`_ library.

It includes the following functionality:

* Storage access (for viewing what was saved/created).
* Kazoo *mostly* compatible client API.
* Sync/transaction/create/get/delete... commands.
* Listener support.
* And more...

It simplifies testing by providing a client that has a similar API as the kazoo
client so that your tests (or applications/libraries that use kazoo) do not
require a real zookeeper server to be  tested with (since this is not available
in all testing environments).

.. _kazoo: https://kazoo.readthedocs.org/en/latest/
