Django PostgreSQL Netfields
===========================

This project is an attempt at making proper PostgreSQL net related fields for
Django. In Django pre 1.4 the built in ``IPAddressField`` does not support IPv6
and uses an inefficient ``HOST()`` cast in all lookups. As of 1.4 you can use
``GenericIPAddressField`` for IPv6, but the casting problem remains.

In addition to the basic ``IPAddressField`` replacement, ``InetAddressField``,
a ``CidrAddressField`` a ``MACAddressField``, and a ``MACAddress8Field`` have
been added. This library also provides a manager that allows for advanced IP
based lookups directly in the ORM.

In Python, the values of the IP address fields are represented as types from
the ipaddress_ module. In Python 2.x, a backport_ is used. The MAC address
fields are represented as EUI types from the netaddr_ module.

.. _ipaddress: https://docs.python.org/3/library/ipaddress.html
.. _backport: https://pypi.python.org/pypi/ipaddress/
.. _netaddr: http://pythonhosted.org/netaddr/

Dependencies
------------

This module requires ``Django >= 1.11``, ``psycopg2`` or ``psycopg``, and ``netaddr``.

Installation
------------

.. code-block:: bash

 $ pip install django-netfields

Getting started
---------------

Make sure ``netfields`` is in your ``PYTHONPATH`` and in ``INSTALLED_APPS``.

``InetAddressField`` will store values in PostgreSQL as type ``INET``. In
Python, the value will be represented as an ``ipaddress.ip_interface`` object
representing an IP address and netmask/prefix length pair unless the
``store_prefix_length`` argument is set to ``False``, in which case the value
will be represented as an ``ipaddress.ip_address`` object.

.. code-block:: python

 from netfields import InetAddressField, NetManager

 class Example(models.Model):
     inet = InetAddressField()
     # ...

     objects = NetManager()

``CidrAddressField`` will store values in PostgreSQL as type ``CIDR``. In
Python, the value will be represented as an ``ipaddress.ip_network`` object.

.. code-block:: python

 from netfields import CidrAddressField, NetManager

 class Example(models.Model):
     inet = CidrAddressField()
     # ...

     objects = NetManager()

``MACAddressField`` will store values in PostgreSQL as type ``MACADDR``. In
Python, the value will be represented as a ``netaddr.EUI`` object. Note that
the default text representation of EUI objects is not the same as that of the
``netaddr`` module. It is represented in a format that is more commonly used
in network utilities and by network administrators (``00:11:22:aa:bb:cc``).

.. code-block:: python

 from netfields import MACAddressField, NetManager

 class Example(models.Model):
     inet = MACAddressField()
     # ...

``MACAddress8Field`` will store values in PostgreSQL as type ``MACADDR8``. In
Python, the value will be represented as a ``netaddr.EUI`` object. As with
``MACAddressField``, the representation is the common one
(``00:11:22:aa:bb:cc:dd:ee``).

.. code-block:: python

 from netfields import MACAddress8Field, NetManager

 class Example(models.Model):
     inet = MACAddress8Field()
     # ...

For ``InetAddressField`` and ``CidrAddressField``, ``NetManager`` is required
for the extra lookups to be available. Lookups for ``INET`` and ``CIDR``
database types will be handled differently than when running vanilla Django.
All lookups are case-insensitive and text based lookups are avoided whenever
possible. In addition to Django's default lookup types the following have been
added:

``__net_contained``
    is contained within the given network

``__net_contained_or_equal``
    is contained within or equal to the given network

``__net_contains``
    contains the given address

``__net_contains_or_equals``
    contains or is equal to the given address/network

``__net_overlaps``
    contains or contained by the given address

``__family``
    matches the given address family

``__host``
    matches the host part of an address regardless of prefix length

``__prefixlen``
    matches the prefix length part of an address

These correspond with the operators and functions from
http://www.postgresql.org/docs/9.4/interactive/functions-net.html

``CidrAddressField`` includes two extra lookups (these will be depreciated in the future by ``__prefixlen``):

``__max_prefixlen``
    Maximum value (inclusive) for ``CIDR`` prefix, does not distinguish between IPv4 and IPv6

``__min_prefixlen``
    Minimum value (inclusive) for ``CIDR`` prefix, does not distinguish between IPv4 and IPv6

Database Functions
''''''''''''''''''

`Postgres network address functions <https://www.postgresql.org/docs/11/functions-net.html>`_ are exposed via the ``netfields.functions`` module.  They can be used to extract additional information from these fields or to construct complex queries.

.. code-block:: python

 from django.db.models import F

 from netfields import CidrAddressField, NetManager
 from netfields.functions import Family, Masklen

 class Example(models.Model):
     inet = CidrAddressField()
     # ...

 ipv4_with_num_ips = (
     Example.objects.annotate(
         family=Family(F('inet')),
         num_ips=2 ** (32 - Masklen(F('inet')))  # requires Django >2.0 to resolve
     )
     .filter(family=4)
 )

**CidrAddressField and InetAddressField Functions**

+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| Postgres Function              | Django Function  | Return Type          | Description                                                    |
+================================+==================+======================+================================================================+
| abbrev(``T``)                  | Abbrev           | ``TextField``        | abbreviated display format as text                             |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| broadcast(``T``)               | Broadcast        | ``InetAddressField`` | broadcast address for network                                  |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| family(``T``)                  | Family           | ``IntegerField``     | extract family of address; 4 for IPv4, 6 for IPv6              |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| host(``T``)                    | Host             | ``TextField``        | extract IP address as text                                     |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| hostmask(``T``)                | Hostmask         | ``InetAddressField`` | construct host mask for network                                |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| masklen(``T``)                 | Masklen          | ``IntegerField``     | extract netmask length                                         |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| netmask(``T``)                 | Netmask          | ``InetAddressField`` | construct netmask for network                                  |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| network(``T``)                 | Network          | ``CidrAddressField`` | extract network part of address                                |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| set_masklen(``T``, int)        | SetMasklen       | ``T``                | set netmask length for inet value                              |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| text(``T``)                    | AsText           | ``TextField``        | extract IP address and netmask length as text                  |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| inet_same_family(``T``, ``T``) | IsSameFamily     | ``BooleanField``     | are the addresses from the same family?                        |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| inet_merge(``T``, ``T``)       | Merge            | ``CidrAddressField`` | the smallest network which includes both of the given networks |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+

**MACAddressField Functions**

+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| Postgres Function              | Django Function  | Return Type          | Description                                                    |
+================================+==================+======================+================================================================+
| trunc(``T``)                   | Trunc            | ``T``                | set last 3 bytes to zero                                       |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+

**MACAddress8Field Functions**

+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| Postgres Function              | Django Function  | Return Type          | Description                                                    |
+================================+==================+======================+================================================================+
| trunc(``T``)                   | Trunc            | ``T``                | set last 5 bytes to zero                                       |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+
| macaddr8_set7bit(``T``)        | Macaddr8Set7bit  | ``T``                | set 7th bit to one. Used to generate link-local IPv6 addresses |
+--------------------------------+------------------+----------------------+----------------------------------------------------------------+

Indexes
'''''''

As of Django 2.2, indexes can be created for ``InetAddressField`` and ``CidrAddressField`` extra lookups directly on the model.

.. code-block:: python

 from django.contrib.postgres.indexes import GistIndex
 from netfields import CidrAddressField, NetManager

 class Example(models.Model):
     inet = CidrAddressField()
     # ...

     class Meta:
         indexes = (
             GistIndex(
                 fields=('inet',), opclasses=('inet_ops',),
                 name='app_example_inet_idx'
             ),
         )

For earlier versions of Django, a custom migration can be used to install an index.

.. code-block:: python

 from django.db import migrations

 class Migration(migrations.Migration):
     # ...

     operations = [
         # ...
         migrations.RunSQL(
             "CREATE INDEX app_example_inet_idx ON app_example USING GIST (inet inet_ops);"
         ),
         # ...
     ]

Related Django bugs
-------------------

* 11442_ - Postgresql backend casts inet types to text, breaks IP operations and IPv6 lookups.
* 811_ - IPv6 address field support.

https://docs.djangoproject.com/en/dev/releases/1.4/#extended-ipv6-support is also relevant

.. _11442: http://code.djangoproject.com/ticket/11442
.. _811: http://code.djangoproject.com/ticket/811


Similar projects
----------------

https://bitbucket.org/onelson/django-ipyfield tries to solve some of the same
issues as this library. However, instead of supporting just postgres via the proper
fields types the ipyfield currently uses a ``VARCHAR(39)`` as a fake unsigned 64 bit
number in its implementation.

History
-------

Main repo was originally kept https://github.com/adamcik/django-postgresql-netfields
Late April 2013 the project was moved to https://github.com/jimfunk/django-postgresql-netfields
to pass the torch on to someone who actually uses this code actively :-)
