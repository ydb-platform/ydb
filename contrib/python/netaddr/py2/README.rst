netaddr
=======

.. image:: https://codecov.io/gh/netaddr/netaddr/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/netaddr/netaddr
.. image:: https://github.com/netaddr/netaddr/workflows/CI/badge.svg
   :target: https://github.com/netaddr/netaddr/actions?query=workflow%3ACI+branch%3Amaster
.. image:: https://img.shields.io/pypi/v/netaddr.svg
   :target: https://pypi.org/project/netaddr/
.. image:: https://img.shields.io/pypi/pyversions/netaddr.svg
   :target: pypi.python.org/pypi/netaddr

A Python library for representing and manipulating network addresses.

Provides support for:

Layer 3 addresses

-  IPv4 and IPv6 addresses, subnets, masks, prefixes
-  iterating, slicing, sorting, summarizing and classifying IP networks
-  dealing with various ranges formats (CIDR, arbitrary ranges and
   globs, nmap)
-  set based operations (unions, intersections etc) over IP addresses
   and subnets
-  parsing a large variety of different formats and notations
-  looking up IANA IP block information
-  generating DNS reverse lookups
-  supernetting and subnetting

Layer 2 addresses

-  representation and manipulation MAC addresses and EUI-64 identifiers
-  looking up IEEE organisational information (OUI, IAB)
-  generating derived IPv6 addresses

Documentation: https://netaddr.readthedocs.io/en/latest/

Share and enjoy!
