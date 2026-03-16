"""Postgres network address functions.

https://www.postgresql.org/docs/11/functions-net.html
"""

from django.db.models import BooleanField, Func, IntegerField, TextField

from .fields import CidrAddressField, InetAddressField, MACAddress8Field


class Abbrev(Func):
    """Function to abbreviate field as text."""

    arity = 1
    function = 'ABBREV'
    output_field = TextField()


class Broadcast(Func):
    """Function to extract broadcast address for network."""

    arity = 1
    function = 'BROADCAST'
    output_field = InetAddressField()


class Family(Func):
    """Function to extract family of address; 4 for IPv4, 6 for IPv6."""

    arity = 1
    function = 'FAMILY'
    output_field = IntegerField()


class Host(Func):
    """Function to extract IP address as text."""

    arity = 1
    function = 'HOST'
    output_field = TextField()


class Hostmask(Func):
    """Function to construct host mask for network."""

    arity = 1
    function = 'HOSTMASK'
    output_field = InetAddressField()


class Masklen(Func):
    """Function to extract netmask length."""

    arity = 1
    function = 'MASKLEN'
    output_field = IntegerField()


class Netmask(Func):
    """Function to construct netmask for network."""

    arity = 1
    function = 'NETMASK'
    output_field = InetAddressField()


class Network(Func):
    """Function to extract network part of address."""

    arity = 1
    function = 'NETWORK'
    output_field = CidrAddressField()


class SetMasklen(Func):
    """Function to set netmask length."""

    arity = 2
    function = 'SET_MASKLEN'
    output_field = InetAddressField()


class AsText(Func):
    """Function to extract IP address and netmask length as text."""

    arity = 1
    function = 'TEXT'
    output_field = TextField()


class IsSameFamily(Func):
    """Function to test that addresses are from the same family."""

    arity = 2
    function = 'INET_SAME_FAMILY'
    output_field = BooleanField()


class Merge(Func):
    """Function to calculate the smallest network which includes both of the given
    networks.
    """

    arity = 2
    function = 'INET_MERGE'
    output_field = CidrAddressField()


class Trunc(Func):

    arity = 1
    function = 'TRUNC'


class Macaddr8Set7bit(Func):
    """Function that sets 7th bit to one, also known as modified EUI-64, for inclusion in an IPv6 address"""
    arity = 1
    function = 'MACADDR8_SET7BIT'
    output_field = MACAddress8Field()
