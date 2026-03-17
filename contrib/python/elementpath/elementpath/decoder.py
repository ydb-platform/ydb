#
# Copyright (c), 2024-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
A module for providing XSD atomic values for schema context evaluation
and for decoding text into XSD atomic data values.
"""
from collections.abc import Iterator
from decimal import Decimal
from typing import Optional, Union

from elementpath import aliases
from elementpath.aliases import AnyNsmapType
from elementpath.protocols import XsdTypeProtocol
from elementpath.exceptions import xpath_error
from elementpath.namespaces import XSD_NAMESPACE
import elementpath.datatypes as dt


class _Notation(dt.Notation):
    """An instantiable xs:NOTATION."""


_ATOMIC_VALUES: dict[str, dict[str, aliases.AtomicType]] = {'1.0': {
    f'{{{XSD_NAMESPACE}}}untypedAtomic': dt.UntypedAtomic('1'),
    f'{{{XSD_NAMESPACE}}}anyType': dt.UntypedAtomic('1'),
    f'{{{XSD_NAMESPACE}}}anySimpleType': dt.UntypedAtomic('1'),
    f'{{{XSD_NAMESPACE}}}anyAtomicType': dt.UntypedAtomic('1'),
    f'{{{XSD_NAMESPACE}}}boolean': True,
    f'{{{XSD_NAMESPACE}}}decimal': Decimal('1.0'),
    f'{{{XSD_NAMESPACE}}}double': float('1.0'),
    f'{{{XSD_NAMESPACE}}}float': dt.Float('1.0'),
    f'{{{XSD_NAMESPACE}}}string': '  alpha\t',
    f'{{{XSD_NAMESPACE}}}date': dt.Date10.fromstring('2000-01-01'),
    f'{{{XSD_NAMESPACE}}}dateTime': dt.DateTime10.fromstring('2000-01-01T12:00:00'),
    f'{{{XSD_NAMESPACE}}}gDay': dt.GregorianDay.fromstring('---31'),
    f'{{{XSD_NAMESPACE}}}gMonth': dt.GregorianMonth.fromstring('--12'),
    f'{{{XSD_NAMESPACE}}}gMonthDay': dt.GregorianMonthDay.fromstring('--12-01'),
    f'{{{XSD_NAMESPACE}}}gYear': dt.GregorianYear10.fromstring('1999'),
    f'{{{XSD_NAMESPACE}}}gYearMonth': dt.GregorianYearMonth10.fromstring('1999-09'),
    f'{{{XSD_NAMESPACE}}}time': dt.Time.fromstring('09:26:54'),
    f'{{{XSD_NAMESPACE}}}duration': dt.Duration.fromstring('P1MT1S'),
    f'{{{XSD_NAMESPACE}}}dayTimeDuration': dt.DayTimeDuration.fromstring('P1DT1S'),
    f'{{{XSD_NAMESPACE}}}yearMonthDuration': dt.YearMonthDuration.fromstring('P1Y1M'),
    f'{{{XSD_NAMESPACE}}}QName': dt.QName(XSD_NAMESPACE, 'xs:element'),
    f'{{{XSD_NAMESPACE}}}NOTATION': _Notation(XSD_NAMESPACE, 'xs:element'),
    f'{{{XSD_NAMESPACE}}}anyURI': dt.AnyURI('https://example.com'),
    f'{{{XSD_NAMESPACE}}}normalizedString': dt.NormalizedString(' alpha  '),
    f'{{{XSD_NAMESPACE}}}token': dt.XsdToken('a token'),
    f'{{{XSD_NAMESPACE}}}language': dt.Language('en-US'),
    f'{{{XSD_NAMESPACE}}}Name': dt.Name('_a.name::'),
    f'{{{XSD_NAMESPACE}}}NCName': dt.NCName('nc-name'),
    f'{{{XSD_NAMESPACE}}}ID': dt.Id('id1'),
    f'{{{XSD_NAMESPACE}}}IDREF': dt.Idref('id_ref1'),
    f'{{{XSD_NAMESPACE}}}ENTITY': dt.Entity('entity1'),
    f'{{{XSD_NAMESPACE}}}NMTOKEN': dt.NMToken('a_token'),
    f'{{{XSD_NAMESPACE}}}base64Binary': dt.Base64Binary('YWxwaGE='),
    f'{{{XSD_NAMESPACE}}}hexBinary': dt.HexBinary('31'),
    f'{{{XSD_NAMESPACE}}}integer': dt.Integer('1'),
    f'{{{XSD_NAMESPACE}}}long': dt.Long('1'),
    f'{{{XSD_NAMESPACE}}}int': dt.Int('1'),
    f'{{{XSD_NAMESPACE}}}short': dt.Short('1'),
    f'{{{XSD_NAMESPACE}}}byte': dt.Byte('1'),
    f'{{{XSD_NAMESPACE}}}positiveInteger': dt.PositiveInteger('1'),
    f'{{{XSD_NAMESPACE}}}negativeInteger': dt.NegativeInteger('-1'),
    f'{{{XSD_NAMESPACE}}}nonPositiveInteger': dt.NonPositiveInteger('0'),
    f'{{{XSD_NAMESPACE}}}nonNegativeInteger': dt.NonNegativeInteger('0'),
    f'{{{XSD_NAMESPACE}}}unsignedLong': dt.UnsignedLong('1'),
    f'{{{XSD_NAMESPACE}}}unsignedInt': dt.UnsignedInt('1'),
    f'{{{XSD_NAMESPACE}}}unsignedShort': dt.UnsignedShort('1'),
    f'{{{XSD_NAMESPACE}}}unsignedByte': dt.UnsignedByte('1'),
}}

_ATOMIC_VALUES['1.1'] = {
    **_ATOMIC_VALUES['1.0'],
    f'{{{XSD_NAMESPACE}}}date': dt.Date.fromstring('2000-01-01'),
    f'{{{XSD_NAMESPACE}}}dateTime': dt.DateTime.fromstring('2000-01-01T12:00:00'),
    f'{{{XSD_NAMESPACE}}}gYear': dt.GregorianYear.fromstring('1999'),
    f'{{{XSD_NAMESPACE}}}gYearMonth': dt.GregorianYearMonth.fromstring('1999-09'),
    f'{{{XSD_NAMESPACE}}}dateTimeStamp': dt.DateTimeStamp.fromstring('2000-01-01T12:00:00+01:00'),
}

# List based types
_LIST_VALUES = {
    f'{{{XSD_NAMESPACE}}}IDREFS': dt.Idrefs([dt.Idref('id_ref1'), dt.Idref('id_ref2')]),
    f'{{{XSD_NAMESPACE}}}ENTITIES': dt.Entities([dt.Entity('entity1'), dt.Entity('entity2')]),
    f'{{{XSD_NAMESPACE}}}NMTOKENS': dt.NMTokens([dt.NMToken('a_token'), dt.NMToken('b_token')]),
}


def iter_atomic_values(xsd_type: XsdTypeProtocol) -> Iterator[aliases.AtomicType]:
    """Generates a list of XSD atomic values related to provided XSD type."""

    def _iter_values(root_type: XsdTypeProtocol, depth: int) -> Iterator[aliases.AtomicType]:
        if depth > 15:
            return
        if root_type.name in atomic_values:
            yield atomic_values[root_type.name]
        elif hasattr(root_type, 'member_types'):
            for member_type in root_type.member_types:
                yield from _iter_values(member_type, depth + 1)

    atomic_values = _ATOMIC_VALUES[xsd_type.xsd_version]
    if xsd_type.name in atomic_values:
        yield atomic_values[xsd_type.name]
    elif xsd_type.is_simple() or (simple_type := xsd_type.simple_type) is None:
        yield from _iter_values(xsd_type.root_type, 1)
    elif simple_type.name in atomic_values:
        yield atomic_values[simple_type.name]
    else:
        yield from _iter_values(simple_type.root_type, 1)


def get_atomic_sequence(xsd_type: Optional[XsdTypeProtocol],
                        text: Optional[str] = None,
                        namespaces: AnyNsmapType = None) -> Iterator[aliases.AtomicType]:
    """Returns a decoder function for atomic values of an XSD type instance."""
    def decode(s: str) -> aliases.AtomicType:
        if isinstance(value, (dt.AbstractDateTime, dt.Duration)):
            return value.fromstring(s)
        elif not isinstance(value, dt.AbstractQName):
            return value.__class__(s)
        else:
            nonlocal namespaces
            if namespaces is None:
                namespaces = {}
            if ':' not in s:
                return value.__class__(namespaces.get(''), s)
            else:
                return value.__class__(namespaces[s.split(':')[0]], s)

    if xsd_type is None:
        yield dt.UntypedAtomic(text or '')
    elif text is None:
        yield from iter_atomic_values(xsd_type)
    else:
        error: Union[None, ValueError, ArithmeticError] = None
        code = 'FORG0001'

        for value in iter_atomic_values(xsd_type):
            try:
                if xsd_type.is_list():
                    for item in text.split():
                        yield decode(item)
                else:
                    yield decode(text)
            except (ArithmeticError, ValueError) as err:
                if error is None:
                    error = err
                    if isinstance(err, ArithmeticError):
                        if isinstance(value, dt.AbstractDateTime):
                            code = 'FODT0001'
                        elif isinstance(value, dt.Duration):
                            code = 'FODT0002'
                        else:
                            code = 'FOCA0002'
            else:
                return
        else:
            if error is not None:
                raise xpath_error(code, error, namespaces=namespaces)
            elif hasattr(xsd_type, 'decode'):
                yield xsd_type.decode(text or '')
            else:
                yield dt.UntypedAtomic(text if isinstance(text, str) else '')


__all__ = ['get_atomic_sequence']
