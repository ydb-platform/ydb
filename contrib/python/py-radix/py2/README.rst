py-radix
========

.. image:: https://travis-ci.org/mjschultz/py-radix.svg?branch=master
   :target: https://travis-ci.org/mjschultz/py-radix

.. image:: https://coveralls.io/repos/mjschultz/py-radix/badge.png?branch=master
   :target: https://coveralls.io/r/mjschultz/py-radix?branch=master

py-radix implements the radix tree data structure for the storage and
retrieval of IPv4 and IPv6 network prefixes.

The radix tree is commonly used for routing table lookups. It efficiently
stores network prefixes of varying lengths and allows fast lookups of
containing networks.

Installation
------------

Installation is a breeze via pip: ::

    pip install py-radix

Or with the standard Python distutils incantation: ::

	python setup.py build
	python setup.py install

The C extension will be built for supported python versions. If you do not
want the C extension, set the environment variable ``RADIX_NO_EXT=1``.

Tests are in the ``tests/`` directory and can be run with
``python setup.py nosetests``.

Usage
-----

A simple example that demonstrates most of the features: ::

	import radix

	# Create a new tree
	rtree = radix.Radix()

	# Adding a node returns a RadixNode object. You can create
	# arbitrary members in its 'data' dict to store your data
	rnode = rtree.add("10.0.0.0/8")
	rnode.data["blah"] = "whatever you want"

	# You can specify nodes as CIDR addresses, or networks with
	# separate mask lengths. The following three invocations are
	# identical:
	rnode = rtree.add("10.0.0.0/16")
	rnode = rtree.add("10.0.0.0", 16)
	rnode = rtree.add(network = "10.0.0.0", masklen = 16)

	# It is also possible to specify nodes using binary packed
	# addresses, such as those returned by the socket module
	# functions. In this case, the radix module will assume that
	# a four-byte address is an IPv4 address and a sixteen-byte
	# address is an IPv6 address. For example:
	binary_addr = inet_ntoa("172.18.22.0")
	rnode = rtree.add(packed = binary_addr, masklen = 23)

	# Exact search will only return prefixes you have entered
	# You can use all of the above ways to specify the address
	rnode = rtree.search_exact("10.0.0.0/8")
	# Get your data back out
	print rnode.data["blah"]
	# Use a packed address
	addr = socket.inet_ntoa("10.0.0.0")
	rnode = rtree.search_exact(packed = addr, masklen = 8)

	# Best-match search will return the longest matching prefix
	# that contains the search term (routing-style lookup)
	rnode = rtree.search_best("10.123.45.6")

	# Worst-search will return the shortest matching prefix
	# that contains the search term (inverse routing-style lookup)
	rnode = rtree.search_worst("10.123.45.6")

	# Covered search will return all prefixes inside the given
	# search term, as a list (including the search term itself,
	# if present in the tree)
	rnodes = rtree.search_covered("10.123.0.0/16")

	# There are a couple of implicit members of a RadixNode:
	print rnode.network	# -> "10.0.0.0"
	print rnode.prefix	# -> "10.0.0.0/8"
	print rnode.prefixlen	# -> 8
	print rnode.family	# -> socket.AF_INET
	print rnode.packed	# -> '\n\x00\x00\x00'

	# IPv6 prefixes are fully supported in the same tree
	rnode = rtree.add("2001:DB8::/3")
	rnode = rtree.add("::/0")

	# Use the nodes() method to return all RadixNodes created
	nodes = rtree.nodes()
	for rnode in nodes:
		print rnode.prefix

	# The prefixes() method will return all the prefixes (as a
	# list of strings) that have been entered
	prefixes = rtree.prefixes()

	# You can also directly iterate over the tree itself
	# this would save some memory if the tree is big
	# NB. Don't modify the tree (add or delete nodes) while
	# iterating otherwise you will abort the iteration and
	# receive a RuntimeWarning. Changing a node's data dict
	# is permitted.
	for rnode in rtree:
  		print rnode.prefix


License
-------

py-radix is licensed under a ISC/BSD licence. The underlying radix tree 
implementation is taken (and modified) from MRTd and is subject to a 4-term 
BSD license. See the LICENSE file for details.

Contributing
------------

Please report bugs via GitHub at https://github.com/mjschultz/py-radix/issues.
Code changes can be contributed through a pull request on GitHub or emailed
directly to me <mjschultz@gmail.com>.

The main portions of the directory tree are as follows: ::

    .
    ├── radix/*.py      # Pure Python code
    ├── radix/_radix.c  # C extension code (compatible with pure python code)
    ├── radix/_radix/*  # C extension code (compatible with pure python code)
    ├── tests/          # Tests (regression and unit)
    └── setup.py        # Standard setup.py for installation/testing/etc.
