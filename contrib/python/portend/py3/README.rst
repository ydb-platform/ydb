.. image:: https://img.shields.io/pypi/v/portend.svg
   :target: https://pypi.org/project/portend

.. image:: https://img.shields.io/pypi/pyversions/portend.svg

.. image:: https://github.com/jaraco/portend/actions/workflows/main.yml/badge.svg
   :target: https://github.com/jaraco/portend/actions?query=workflow%3A%22tests%22
   :alt: tests

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

.. image:: https://readthedocs.org/projects/portend/badge/?version=latest
   :target: https://portend.readthedocs.io/en/latest/?badge=latest

.. image:: https://img.shields.io/badge/skeleton-2025-informational
   :target: https://blog.jaraco.com/skeleton

.. image:: https://tidelift.com/badges/package/pypi/portend
   :target: https://tidelift.com/subscription/pkg/pypi-portend?utm_source=pypi-portend&utm_medium=readme

por·tend
pôrˈtend/
verb

    be a sign or warning that (something, especially something momentous or calamitous) is likely to happen.

Usage
=====

Use portend to monitor TCP ports for bound or unbound states.

For example, to wait for a port to be occupied, timing out after 3 seconds::

    portend.occupied('www.google.com', 80, timeout=3)

Or to wait for a port to be free, timing out after 5 seconds::

    portend.free('::1', 80, timeout=5)

The portend may also be executed directly. If the function succeeds, it
returns nothing and exits with a status of 0. If it fails, it prints a
message and exits with a status of 1. For example::

    python -m portend localhost:31923 free
    (exits immediately)

    python -m portend -t 1 localhost:31923 occupied
    (one second passes)
    Port 31923 not bound on localhost.

Portend also exposes a ``find_available_local_port`` for identifying
a suitable port for binding locally::

    port = portend.find_available_local_port()
    print(port, "is available for binding")

Portend additionally exposes the lower-level port checking functionality
in the ``Checker`` class, which currently exposes only one public
method, ``assert_free``::

    portend.Checker().assert_free('localhost', 31923)

If assert_free is passed a host/port combination that is occupied by
a bound listener (i.e. a TCP connection is established to that host/port),
assert_free will raise a ``PortNotFree`` exception.


For Enterprise
==============

Available as part of the Tidelift Subscription.

This project and the maintainers of thousands of other packages are working with Tidelift to deliver one enterprise subscription that covers all of the open source you use.

`Learn more <https://tidelift.com/subscription/pkg/pypi-portend?utm_source=pypi-portend&utm_medium=referral&utm_campaign=github>`_.
