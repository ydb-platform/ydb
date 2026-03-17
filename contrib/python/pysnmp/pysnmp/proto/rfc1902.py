#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import warnings

from pyasn1.type import constraint, namedtype, namedval, tag, univ
from pysnmp.proto import error, rfc1155

__all__ = [
    "Opaque",
    "TimeTicks",
    "Bits",
    "Integer",
    "OctetString",
    "IpAddress",
    "Counter64",
    "Unsigned32",
    "Gauge32",
    "Integer32",
    "ObjectIdentifier",
    "Counter32",
    "Null",
]


class Null(univ.Null):
    """Creates an instance of SNMP Null class.

    :py:class:`~pysnmp.proto.rfc1902.Null` type represents the absence
    of value.

    Parameters
    ----------
    initializer: str
        Python string object. Must be an empty string.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Null('')
        Null('')
        >>>
    """


class Integer32(univ.Integer):
    """Creates an instance of SNMP Integer32 class.

    :py:class:`~pysnmp.proto.rfc1902.Integer32` type represents
    integer-valued information between -2147483648 to 2147483647
    inclusive (:RFC:`1902#section-7.1.1`). This type is indistinguishable
    from the :py:class:`~pysnmp.proto.rfc1902.Integer` type.
    The :py:class:`~pysnmp.proto.rfc1902.Integer32` type may be sub-typed
    to be more constrained than the base
    :py:class:`~pysnmp.proto.rfc1902.Integer32` type.

    Parameters
    ----------
    initializer : int
        Python integer in range between -2147483648 to 2147483647 inclusive
        or :py:class:`~pysnmp.proto.rfc1902.Integer32`.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Integer32(1234)
        Integer32(1234)
        >>> Integer32(1) > 2
        True
        >>> Integer32(1) + 1
        Integer32(2)
        >>> int(Integer32(321))
        321
        >>> SmallInteger = Integer32.with_range(1,3)
        >>> SmallInteger(1)
        Integer32(1)
        >>> DiscreetInteger = Integer32.with_values(4, 8, 1)
        >>> DiscreetInteger(4)
        Integer32(4)
        >>>

    """

    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        -2147483648, 2147483647
    )  # noqa: N815

    @classmethod
    def with_values(cls, *values):
        """Creates a subclass with discreet values constraint."""

        class X(cls):
            subtypeSpec = cls.subtypeSpec + constraint.SingleValueConstraint(
                *values
            )  # noqa: N815

        X.__name__ = cls.__name__
        return X

    @classmethod
    def with_range(cls, minimum, maximum):
        """Creates a subclass with value range constraint."""

        class X(cls):
            subtypeSpec = cls.subtypeSpec + constraint.ValueRangeConstraint(
                minimum, maximum
            )  # noqa: N815

        X.__name__ = cls.__name__
        return X


class Integer(Integer32):
    """Creates an instance of SNMP INTEGER class.

    The :py:class:`~pysnmp.proto.rfc1902.Integer` type represents
    integer-valued information as named-number enumerations
    (:RFC:`1902#section-7.1.1`). This type inherits and is indistinguishable
    from :py:class:`~pysnmp.proto.rfc1902.Integer32` class.
    The :py:class:`~pysnmp.proto.rfc1902.Integer` type may be sub-typed
    to be more constrained than the base
    :py:class:`~pysnmp.proto.rfc1902.Integer` type.

    Parameters
    ----------
    initializer : int
        Python integer in range between -2147483648 to 2147483647 inclusive
        or :py:class:`~pysnmp.proto.rfc1902.Integer`  class instance.
        In case of named-numbered enumerations, initialization is also
        possible by enumerated literal.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Integer(1234)
        Integer(1234)
        >>> Integer(1) > 2
        True
        >>> Integer(1) + 1
        Integer(2)
        >>> int(Integer(321))
        321
        >>> SomeState = Integer.with_named_values(enable=1, disable=0)
        >>> SomeState(1)
        Integer('enable')
        >>> int(SomeState('disable'))
        0
        >>>

    """

    @classmethod
    def with_named_values(cls, **values):
        """Create a subclass with discreet named values constraint.

        Reduce fully duplicate enumerations along the way.
        """
        enums = set(cls.namedValues.items())
        enums.update(values.items())

        class X(cls):
            namedValues = namedval.NamedValues(*enums)
            subtypeSpec = cls.subtypeSpec + constraint.SingleValueConstraint(
                *values.values()
            )  # noqa: N815

        X.__name__ = cls.__name__
        return X


class OctetString(univ.OctetString):
    r"""Creates an instance of SNMP OCTET STRING class.

    The :py:class:`~pysnmp.proto.rfc1902.OctetString` type represents
    arbitrary binary or text data (:RFC:`1902#section-7.1.2`).
    It may be sub-typed to be constrained in size.

    Parameters
    ----------
    strValue : str
        Python string or :py:class:`~pysnmp.proto.rfc1902.OctetString`
        class instance.

    Other parameters
    ----------------
    hexValue : str
        Python string representing octets in a hexadecimal notation
        (e.g. DEADBEEF).

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> OctetString('some apples')
        OctetString('some apples')
        >>> OctetString('some apples') + ' and oranges'
        OctetString('some apples and oranges')
        >>> str(OctetString('some apples'))
        'some apples'
        >>> SomeString = OctetString.with_size(3, 12)
        >>> str(SomeString(hexValue='deadbeef'))
        '\xde\xad\xbe\xef'
        >>>

    """

    subtypeSpec = univ.OctetString.subtypeSpec + constraint.ValueSizeConstraint(
        0, 65535
    )  # noqa: N815

    # rfc1902 uses a notion of "fixed length string" what might mean
    # having zero-range size constraint applied. The following is
    # supposed to be used for setting and querying this property.

    fixed_length = None

    def set_fixed_length(self, value):
        """Set fixed length."""
        self.fixed_length = value
        return self

    def is_fixed_length(self):
        """Return if fixed length."""
        return self.fixed_length is not None

    def get_fixed_length(self):
        """Return fixed length."""
        return self.fixed_length

    def clone(self, *args, **kwargs):
        """Clone the data."""
        return univ.OctetString.clone(self, *args, **kwargs).set_fixed_length(
            self.get_fixed_length()
        )

    def subtype(self, *args, **kwargs):
        """Subtype the data."""
        return univ.OctetString.subtype(self, *args, **kwargs).set_fixed_length(
            self.get_fixed_length()
        )

    @classmethod
    def with_size(cls, minimum, maximum):
        """Creates a subclass with value size constraint."""

        class X(cls):
            subtypeSpec = cls.subtypeSpec + constraint.ValueSizeConstraint(
                minimum, maximum
            )  # noqa: N815

        X.__name__ = cls.__name__
        return X

    # Compatibility API
    deprecated_attributes = {
        "setFixedLength": "set_fixed_length",
        "isFixedLength": "is_fixed_length",
        "getFixedLength": "get_fixed_length",
        "fixedLength": "fixed_length",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )


class ObjectIdentifier(univ.ObjectIdentifier):
    """Creates an instance of SNMP OBJECT IDENTIFIER class.

    The :py:class:`~pysnmp.proto.rfc1902.ObjectIdentifier` type represents
    administratively assigned names (:RFC:`1902#section-7.1.3`).
    Supports sequence protocol where elements are integer sub-identifiers.

    Parameters
    ----------
    initializer: tuple, str
        Python tuple of up to 128 integers in range between 0 to 4294967295
        inclusive or Python string containing OID in "dotted" form or
        :py:class:`~pysnmp.proto.rfc1902.ObjectIdentifier`.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> ObjectIdentifier((1, 3, 6))
        ObjectIdentifier('1.3.6')
        >>> ObjectIdentifier('1.3.6')
        ObjectIdentifier('1.3.6')
        >>> tuple(ObjectIdentifier('1.3.6'))
        (1, 3, 6)
        >>> str(ObjectIdentifier('1.3.6'))
        '1.3.6'
        >>>

    """


class IpAddress(OctetString):
    r"""Creates an instance of SNMP IpAddress class.

    The :py:class:`~pysnmp.proto.rfc1902.IpAddress` class represents
    a 32-bit internet address as an OCTET STRING of length 4, in network
    byte-order (:RFC:`1902#section-7.1.5`).

    Parameters
    ----------
    strValue : str
        The same as :py:class:`~pysnmp.proto.rfc1902.OctetString`,
        additionally IPv4 address in dotted notation ('127.0.0.1').

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> IpAddress('127.0.0.1')
        IpAddress(hexValue='7f000001')
        >>> str(IpAddress(hexValue='7f000001'))
        '\x7f\x00\x00\x01'
        >>> IpAddress('\x7f\x00\x00\x01')
        IpAddress(hexValue='7f000001')
        >>>

    """

    tagSet = OctetString.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x00)
    )
    subtypeSpec = OctetString.subtypeSpec + constraint.ValueSizeConstraint(
        4, 4
    )  # noqa: N815
    fixed_length = 4

    def prettyIn(self, value):  # noqa: N802
        """Convert string to IP address."""
        if isinstance(value, str) and len(value) != 4:
            try:
                value = [int(x) for x in value.split(".")]
            except:
                raise error.ProtocolError("Bad IP address syntax %s" % value)
        value = OctetString.prettyIn(self, value)
        if len(value) != 4:
            raise error.ProtocolError("Bad IP address syntax")
        return value

    def prettyOut(self, value):  # noqa: N802
        """Convert IP address to a string."""
        if value:
            return ".".join(["%d" % x for x in self.__class__(value).asNumbers()])
        else:
            return ""


class Counter32(univ.Integer):
    """Creates an instance of SNMP Counter32 class.

    :py:class:`~pysnmp.proto.rfc1902.Counter32` type represents
    a non-negative integer which monotonically increases until it
    reaches a maximum value of 4294967295, when it wraps around and
    starts increasing again from zero (:RFC:`1902#section-7.1.6`).

    Parameters
    ----------
    initializer : int
        Python integer in range between 0 to 4294967295 inclusive
        or any :py:class:`~pysnmp.proto.rfc1902.Integer`-based class.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Counter32(1234)
        Counter32(1234)
        >>> Counter32(1) + 1
        Counter32(2)
        >>> int(Counter32(321))
        321
        >>>

    """

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x01)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class Gauge32(univ.Integer):
    """Creates an instance of SNMP Gauge32 class.

    :py:class:`~pysnmp.proto.rfc1902.Gauge32` type represents
    a non-negative integer, which may increase or decrease, but shall
    never exceed a maximum value. The maximum value can not be greater
    than 4294967295 (:RFC:`1902#section-7.1.7`).

    Parameters
    ----------
    initializer : int
        Python integer in range between 0 to 4294967295 inclusive
        or any :py:class:`~pysnmp.proto.rfc1902.Integer`-based class.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Gauge32(1234)
        Gauge32(1234)
        >>> Gauge32(1) + 1
        Gauge32(2)
        >>> int(Gauge32(321))
        321
        >>>

    """

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x02)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class Unsigned32(univ.Integer):
    """Creates an instance of SNMP Unsigned32 class.

    :py:class:`~pysnmp.proto.rfc1902.Unsigned32` type represents
    integer-valued information between 0 and 4294967295
    (:RFC:`1902#section-7.1.11`).

    Parameters
    ----------
    initializer : int
        Python integer in range between 0 to 4294967295 inclusive
        or any :py:class:`~pysnmp.proto.rfc1902.Integer`-based class.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Unsigned32(1234)
        Unsigned32(1234)
        >>> Unsigned32(1) + 1
        Unsigned32(2)
        >>> int(Unsigned32(321))
        321
        >>>

    """

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x02)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class TimeTicks(univ.Integer):
    """Creates an instance of SNMP TimeTicks class.

    :py:class:`~pysnmp.proto.rfc1902.TimeTicks` type represents
    a non-negative integer which represents the time, modulo 4294967296,
    in hundredths of a second between two epochs (:RFC:`1902#section-7.1.8`).

    Parameters
    ----------
    initializer : int
        Python integer in range between 0 to 4294967295 inclusive
        or any :py:class:`~pysnmp.proto.rfc1902.Integer`-based class.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> TimeTicks(1234)
        TimeTicks(1234)
        >>> TimeTicks(1) + 1
        TimeTicks(2)
        >>> int(TimeTicks(321))
        321
        >>>

    """

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x03)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 4294967295
    )  # noqa: N815


class Opaque(univ.OctetString):
    r"""Creates an instance of SNMP Opaque class.

    The :py:class:`~pysnmp.proto.rfc1902.Opaque` type supports the
    capability to pass arbitrary ASN.1 syntax.  A value is encoded
    using the ASN.1 BER into a string of octets.  This, in turn, is
    encoded as an OCTET STRING, in effect "double-wrapping" the original
    ASN.1 value (:RFC:`1902#section-7.1.9`).

    Parameters
    ----------
    strValue : str
        Python string or :py:class:`~pysnmp.proto.rfc1902.OctetString`-based
        class instance.

    Other parameters
    ----------------
    hexValue : str
        Python string representing octets in a hexadecimal notation
        (e.g. DEADBEEF).

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Opaque('some apples')
        Opaque('some apples')
        >>> Opaque('some apples') + ' and oranges'
        Opaque('some apples and oranges')
        >>> str(Opaque('some apples'))
        'some apples'
        >>> str(Opaque(hexValue='deadbeef'))
        '\xde\xad\xbe\xef'
        >>>

    """

    tagSet = univ.OctetString.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x04)
    )


class Counter64(univ.Integer):
    """Creates an instance of SNMP Counter64 class.

    :py:class:`~pysnmp.proto.rfc1902.Counter64` type represents
    a non-negative integer which monotonically increases until it reaches
    a maximum value of 18446744073709551615, when it wraps around and starts
    increasing again from zero (:RFC:`1902#section-7.1.10`).

    Parameters
    ----------
    initializer : int
        Python integer in range between 0 to 4294967295 inclusive
        or any :py:class:`~pysnmp.proto.rfc1902.Integer`-based class.

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> Counter64(1234)
        Counter64(1234)
        >>> Counter64(1) + 1
        Counter64(2)
        >>> int(Counter64(321))
        321
        >>>

    """

    tagSet = univ.Integer.tagSet.tagImplicitly(  # noqa: N815
        tag.Tag(tag.tagClassApplication, tag.tagFormatSimple, 0x06)
    )
    subtypeSpec = univ.Integer.subtypeSpec + constraint.ValueRangeConstraint(
        0, 18446744073709551615
    )  # noqa: N815


class Bits(OctetString):
    r"""Creates an instance of SNMP BITS class.

    The :py:class:`~pysnmp.proto.rfc1902.Bits` type represents
    an enumeration of named bits. This collection is assigned non-negative,
    contiguous values, starting at zero. Only those named-bits so enumerated
    may be present in a value (:RFC:`1902#section-7.1.4`).

    The bits are named and identified by their position in the octet string.
    Position zero is the high order (or left-most) bit in the first octet of
    the string. Position 7 is the low order (or right-most) bit of the first
    octet of the string. Position 8 is the high order bit in the second octet
    of the string, and so on
    (`BITS Pseudotype <https://tools.ietf.org/html/draft-perkins-bits-00>`_).

    Parameters
    ----------
    strValue : str, tuple
        Sequence of bit names or a Python string (as a raw data) or
        :py:class:`~pysnmp.proto.rfc1902.OctetString` class instance.

    Other parameters
    ----------------
    hexValue : str
        Python string representing octets in a hexadecimal notation
        (e.g. DEADBEEF).

    Raises
    ------
        PyAsn1Error :
            On constraint violation or bad initializer.

    Examples
    --------
        >>> from pysnmp.proto.rfc1902 import *
        >>> SomeBits = Bits.with_named_bits(apple=0, orange=1, peach=2)
        >>> SomeBits(('apple', 'orange')).prettyPrint()
        'apple, orange'
        >>> SomeBits(('apple', 'orange'))
        Bits(hexValue='c0')
        >>> SomeBits('\x80')
        Bits(hexValue='80')
        >>> SomeBits(hexValue='80')
        Bits(hexValue='80')
        >>> SomeBits(hexValue='80').prettyPrint()
        'apple'
        >>>

    """

    namedValues: namedval.NamedValues = namedval.NamedValues()

    def __new__(cls, *args, **kwargs):
        """Create a new instance of the class."""
        if "namedValues" in kwargs:
            Bits = cls.with_named_bits(**dict(kwargs.pop("namedValues")))
            return Bits(*args, **kwargs)

        return OctetString.__new__(cls)

    def prettyIn(self, bits):  # noqa: N802
        """Return raw bitstring."""
        if not isinstance(bits, (tuple, list)):
            return OctetString.prettyIn(self, bits)  # raw bitstring
        octets = []
        for bit in bits:  # tuple of named bits
            v = self.namedValues.getValue(bit)
            if v is None:
                raise error.ProtocolError("Unknown named bit %s" % bit)
            d, m = divmod(v, 8)
            if d >= len(octets):
                octets.extend([0] * (d - len(octets) + 1))
            octets[d] |= 0x01 << (7 - m)
        return OctetString.prettyIn(self, octets)

    def prettyOut(self, value):  # noqa: N802
        """Return named bits."""
        names = []
        ints = self.__class__(value).asNumbers()
        for i, v in enumerate(ints):
            v = ints[i]
            j = 7
            while j >= 0:
                if v & (0x01 << j):
                    name = self.namedValues.getName(i * 8 + 7 - j)
                    if name is None:
                        name = f"UnknownBit-{i * 8 + 7 - j}"
                    names.append(name)
                j -= 1
        return ", ".join([str(x) for x in names])

    @classmethod
    def with_named_bits(cls, **values):
        """Creates a subclass with discreet named bits constraint.

        Reduce fully duplicate enumerations along the way.
        """
        enums = set(cls.namedValues.items())
        enums.update(values.items())

        class X(cls):
            namedValues = namedval.NamedValues(*enums)

        X.__name__ = cls.__name__
        return X


class ObjectName(univ.ObjectIdentifier):
    pass


class SimpleSyntax(rfc1155.TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("integer-value", Integer()),
        namedtype.NamedType("string-value", OctetString()),
        namedtype.NamedType("objectID-value", univ.ObjectIdentifier()),
    )


class ApplicationSyntax(rfc1155.TypeCoercionHackMixIn, univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("ipAddress-value", IpAddress()),
        namedtype.NamedType("counter-value", Counter32()),
        namedtype.NamedType("timeticks-value", TimeTicks()),
        namedtype.NamedType("arbitrary-value", Opaque()),
        namedtype.NamedType("big-counter-value", Counter64()),
        # This conflicts with Counter32
        # namedtype.NamedType('unsigned-integer-value', Unsigned32()),
        namedtype.NamedType("gauge32-value", Gauge32()),
    )  # BITS misplaced?


class ObjectSyntax(univ.Choice):
    componentType = namedtype.NamedTypes(  # noqa: N815
        namedtype.NamedType("simple", SimpleSyntax()),
        namedtype.NamedType("application-wide", ApplicationSyntax()),
    )
