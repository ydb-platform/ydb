CityHash
========

A fork of Python wrapper around `CityHash <https://github.com/google/cityhash>`__
with downgraded version of algorithm. This fork used as 3-rd party library
for hashing data in ClickHouse protocol. Unfortunately ClickHouse
server comes with built-in old version of this algorithm.

Please use original `python-cityhash <https://github.com/escherba/python-cityhash>`_
package for other purposes.

Getting Started
---------------

To use this package in your program, simply type

.. code-block:: bash

    pip install clickhouse-cityhash

See Also
--------
For other fast non-cryptographic hashing implementations available as Python
extensions, see `MetroHash <https://github.com/escherba/python-metrohash>`__
and `xxh <https://github.com/lebedov/xxh>`__.

Authors
-------
The Python bindings were originally written by Alexander [Amper] Marshalov and
were subsequently edited for more speed/versatility and packaged for PyPI by
Eugene Scherba. The original CityHash algorithm is by Google.

License
-------
This software is licensed under the `MIT License
<http://www.opensource.org/licenses/mit-license>`_.  See the included LICENSE
file for more information.
