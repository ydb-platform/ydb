import django
from django.apps import AppConfig
from django.db.models import Field

from netfields.fields import CidrAddressField, InetAddressField
from netfields.lookups import (
    EndsWith,
    Family,
    IEndsWith,
    IRegex,
    IStartsWith,
    InvalidLookup,
    InvalidSearchLookup,
    MaxPrefixlen,
    MinPrefixlen,
    NetContained,
    NetContainedOrEqual,
    NetContains,
    NetContainsOrEquals,
    NetOverlaps,
    Prefixlen,
    Regex,
    StartsWith,
    HostMatches,
)


class NetfieldsConfig(AppConfig):
    name = 'netfields'

    if django.VERSION < (1, 9):
        for lookup in Field.class_lookups.keys():
            if lookup not in ['contains', 'startswith', 'endswith', 'icontains', 'istartswith', 'iendswith', 'isnull', 'in',
                              'exact', 'iexact', 'regex', 'iregex', 'lt', 'lte', 'gt', 'gte', 'equals', 'iequals', 'range', 'search']:
                invalid_lookup = InvalidLookup
                invalid_lookup.lookup_name = lookup
                CidrAddressField.register_lookup(invalid_lookup)
                InetAddressField.register_lookup(invalid_lookup)
        CidrAddressField.register_lookup(InvalidSearchLookup)
        InetAddressField.register_lookup(InvalidSearchLookup)

    CidrAddressField.register_lookup(EndsWith)
    CidrAddressField.register_lookup(IEndsWith)
    CidrAddressField.register_lookup(StartsWith)
    CidrAddressField.register_lookup(IStartsWith)
    CidrAddressField.register_lookup(Regex)
    CidrAddressField.register_lookup(IRegex)
    CidrAddressField.register_lookup(NetContained)
    CidrAddressField.register_lookup(NetContains)
    CidrAddressField.register_lookup(NetContainedOrEqual)
    CidrAddressField.register_lookup(NetContainsOrEquals)
    CidrAddressField.register_lookup(NetOverlaps)
    CidrAddressField.register_lookup(Family)
    CidrAddressField.register_lookup(MaxPrefixlen)
    CidrAddressField.register_lookup(MinPrefixlen)
    CidrAddressField.register_lookup(Prefixlen)
    CidrAddressField.register_lookup(HostMatches)

    InetAddressField.register_lookup(EndsWith)
    InetAddressField.register_lookup(IEndsWith)
    InetAddressField.register_lookup(StartsWith)
    InetAddressField.register_lookup(IStartsWith)
    InetAddressField.register_lookup(Regex)
    InetAddressField.register_lookup(IRegex)
    InetAddressField.register_lookup(NetContained)
    InetAddressField.register_lookup(NetContains)
    InetAddressField.register_lookup(NetContainedOrEqual)
    InetAddressField.register_lookup(NetContainsOrEquals)
    InetAddressField.register_lookup(NetOverlaps)
    InetAddressField.register_lookup(Family)
    InetAddressField.register_lookup(Prefixlen)
    InetAddressField.register_lookup(HostMatches)
