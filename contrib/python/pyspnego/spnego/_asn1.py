# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import collections
import datetime
import enum
import struct
import typing

from spnego._text import to_bytes, to_text

ASN1Value = collections.namedtuple("ASN1Value", ["tag_class", "constructed", "tag_number", "b_data"])
"""A representation of an ASN.1 TLV as a Python object.

Defines the ASN.1 Type Length Value (TLV) values as separate objects for easier parsing. This is returned by
:method:`unpack_asn1`.

Attributes:
    tag_class (TagClass): The tag class of the TLV.
    constructed (bool): Whether the value is constructed or 0, 1, or more element encodings (True) or not (False).
    tag_number (Union[TypeTagNumber, int]): The tag number of the value, can be a TypeTagNumber if the tag_class
        is `universal` otherwise it's an explicit tag number value.
    b_data (bytes): The raw byes of the TLV value.
"""


class TagClass(enum.IntEnum):
    universal = 0
    application = 1
    context_specific = 2
    private = 3

    @classmethod
    def native_labels(cls) -> typing.Dict["TagClass", str]:
        return {
            TagClass.universal: "Universal",
            TagClass.application: "Application",
            TagClass.context_specific: "Context-specific",
            TagClass.private: "Private",
        }


class TypeTagNumber(enum.IntEnum):
    end_of_content = 0
    boolean = 1
    integer = 2
    bit_string = 3
    octet_string = 4
    null = 5
    object_identifier = 6
    object_descriptor = 7
    external = 8
    real = 9
    enumerated = 10
    embedded_pdv = 11
    utf8_string = 12
    relative_oid = 13
    time = 14
    reserved = 15
    sequence = 16
    sequence_of = 16
    set = 17
    set_of = 17
    numeric_string = 18
    printable_string = 19
    t61_string = 20
    videotex_string = 21
    ia5_string = 22
    utc_time = 23
    generalized_time = 24
    graphic_string = 25
    visible_string = 26
    general_string = 27
    universal_string = 28
    character_string = 29
    bmp_string = 30
    date = 31
    time_of_day = 32
    date_time = 33
    duration = 34
    oid_iri = 35
    relative_oid_iri = 36

    @classmethod
    def native_labels(cls) -> typing.Dict[int, str]:
        return {
            TypeTagNumber.end_of_content: "End-of-Content (EOC)",
            TypeTagNumber.boolean: "BOOLEAN",
            TypeTagNumber.integer: "INTEGER",
            TypeTagNumber.bit_string: "BIT STRING",
            TypeTagNumber.octet_string: "OCTET STRING",
            TypeTagNumber.null: "NULL",
            TypeTagNumber.object_identifier: "OBJECT IDENTIFIER",
            TypeTagNumber.object_descriptor: "Object Descriptor",
            TypeTagNumber.external: "EXTERNAL",
            TypeTagNumber.real: "REAL (float)",
            TypeTagNumber.enumerated: "ENUMERATED",
            TypeTagNumber.embedded_pdv: "EMBEDDED PDV",
            TypeTagNumber.utf8_string: "UTF8String",
            TypeTagNumber.relative_oid: "RELATIVE-OID",
            TypeTagNumber.time: "TIME",
            TypeTagNumber.reserved: "RESERVED",
            TypeTagNumber.sequence: "SEQUENCE or SEQUENCE OF",
            TypeTagNumber.set: "SET or SET OF",
            TypeTagNumber.numeric_string: "NumericString",
            TypeTagNumber.printable_string: "PrintableString",
            TypeTagNumber.t61_string: "T61String",
            TypeTagNumber.videotex_string: "VideotexString",
            TypeTagNumber.ia5_string: "IA5String",
            TypeTagNumber.utc_time: "UTCTime",
            TypeTagNumber.generalized_time: "GeneralizedTime",
            TypeTagNumber.graphic_string: "GraphicString",
            TypeTagNumber.visible_string: "VisibleString",
            TypeTagNumber.general_string: "GeneralString",
            TypeTagNumber.universal_string: "UniversalString",
            TypeTagNumber.character_string: "CHARACTER",
            TypeTagNumber.bmp_string: "BMPString",
            TypeTagNumber.date: "DATE",
            TypeTagNumber.time_of_day: "TIME-OF-DAY",
            TypeTagNumber.date_time: "DATE-TIME",
            TypeTagNumber.duration: "DURATION",
            TypeTagNumber.oid_iri: "OID-IRI",
            TypeTagNumber.relative_oid_iri: "RELATIVE-OID-IRI",
        }


def extract_asn1_tlv(
    tlv: typing.Union[bytes, ASN1Value],
    tag_class: TagClass,
    tag_number: typing.Union[int, TypeTagNumber],
) -> bytes:
    """Extract the bytes and validates the existing tag of an ASN.1 value."""
    if isinstance(tlv, ASN1Value):
        if tag_class == TagClass.universal:
            label_name = TypeTagNumber.native_labels().get(tag_number, "Unknown tag type")
            msg = "Invalid ASN.1 %s tags, actual tag class %s and tag number %s" % (
                label_name,
                f"{type(tlv.tag_class).__name__}.{tlv.tag_class.name}",
                (
                    f"{type(tlv.tag_number).__name__}.{tlv.tag_number.name}"
                    if isinstance(tlv.tag_number, TypeTagNumber)
                    else tlv.tag_number
                ),
            )

        else:
            msg = "Invalid ASN.1 tags, actual tag %s and number %s, expecting class %s and number %s" % (
                f"{type(tlv.tag_class).__name__}.{tlv.tag_class.name}",
                (
                    f"{type(tlv.tag_number).__name__}.{tlv.tag_number.name}"
                    if isinstance(tlv.tag_number, TypeTagNumber)
                    else tlv.tag_number
                ),
                f"{type(tag_class).__name__}.{tag_class.name}",
                (
                    f"{type(tag_number).__name__}.{tag_number.name}"
                    if isinstance(tag_number, TypeTagNumber)
                    else tag_number
                ),
            )

        if tlv.tag_class != tag_class or tlv.tag_number != tag_number:
            raise ValueError(msg)

        return tlv.b_data

    return tlv


def get_sequence_value(
    sequence: typing.Dict[int, ASN1Value],
    tag: int,
    structure_name: str,
    field_name: typing.Optional[str] = None,
    unpack_func: typing.Optional[typing.Callable[[typing.Union[bytes, ASN1Value]], typing.Any]] = None,
) -> typing.Any:
    """Gets an optional tag entry in a tagged sequence will a further unpacking of the value."""
    if tag not in sequence:
        return

    if not unpack_func:
        return sequence[tag]

    try:
        return unpack_func(sequence[tag])
    except ValueError as e:
        where = "%s in %s" % (field_name, structure_name) if field_name else structure_name
        raise ValueError("Failed unpacking %s: %s" % (where, str(e))) from e


def pack_asn1(
    tag_class: TagClass,
    constructed: bool,
    tag_number: typing.Union[TypeTagNumber, int],
    b_data: bytes,
) -> bytes:
    """Pack the ASN.1 value into the ASN.1 bytes.

    Will pack the raw bytes into an ASN.1 Type Length Value (TLV) value. A TLV is in the form:

    | Identifier Octet(s) | Length Octet(s) | Data Octet(s) |

    Args:
        tag_class: The tag class of the data.
        constructed: Whether the data is constructed (True), i.e. contains 0, 1, or more element encodings, or is
            primitive (False).
        tag_number: The type tag number if tag_class is universal else the explicit tag number of the TLV.
        b_data: The encoded value to pack into the ASN.1 TLV.

    Returns:
        bytes: The ASN.1 value as raw bytes.
    """
    b_asn1_data = bytearray()

    # ASN.1 Identifier octet is
    #
    # |             Octet 1             |  |              Octet 2              |
    # | 8 | 7 |  6  | 5 | 4 | 3 | 2 | 1 |  |   8   | 7 | 6 | 5 | 4 | 3 | 2 | 1 |
    # | Class | P/C | Tag Number (0-30) |  | More  | Tag number                |
    #
    # If Tag Number is >= 31 the first 5 bits are 1 and the 2nd octet is used to encode the length.
    if tag_class < 0 or tag_class > 3:
        raise ValueError("tag_class must be between 0 and 3")

    identifier_octets = tag_class << 6
    identifier_octets |= (1 if constructed else 0) << 5

    if tag_number < 31:
        identifier_octets |= tag_number
        b_asn1_data.append(identifier_octets)
    else:
        # Set the first 5 bits of the first octet to 1 and encode the tag number in subsequent octets.
        identifier_octets |= 31
        b_asn1_data.append(identifier_octets)
        b_asn1_data.extend(_pack_asn1_octet_number(tag_number))

    # ASN.1 Length octet for DER encoding is always in the definite form. This form packs the lengths in the following
    # octet structure:
    #
    # |                       Octet 1                       |  |            Octet n            |
    # |     8     |  7  |  6  |  5  |  4  |  3  |  2  |  1  |  | 8 | 7 | 6 | 5 | 4 | 3 | 2 | 1 |
    # | Long form | Short = length, Long = num octets       |  | Big endian length for long    |
    #
    # Basically if the length < 127 it's encoded in the first octet, otherwise the first octet 7 bits indicates how
    # many subsequent octets were used to encode the length.
    length = len(b_data)
    if length < 128:
        b_asn1_data.append(length)
    else:
        length_octets = bytearray()
        while length:
            length_octets.append(length & 0b11111111)
            length >>= 8

        # Reverse the octets so the higher octets are first, add the initial length octet with the MSB set and add them
        # all to the main ASN.1 byte array.
        length_octets.reverse()
        b_asn1_data.append(len(length_octets) | 0b10000000)
        b_asn1_data.extend(length_octets)

    return bytes(b_asn1_data) + b_data


def pack_asn1_bit_string(
    value: bytes,
    tag: bool = True,
) -> bytes:
    # First octet is the number of unused bits in the last octet from the LSB.
    b_data = b"\x00" + value
    if tag:
        b_data = pack_asn1(TagClass.universal, False, TypeTagNumber.bit_string, b_data)

    return b_data


def pack_asn1_enumerated(
    value: int,
    tag: bool = True,
) -> bytes:
    """Packs an int into an ASN.1 ENUMERATED byte value with optional universal tagging."""
    b_data = pack_asn1_integer(value, tag=False)
    if tag:
        b_data = pack_asn1(TagClass.universal, False, TypeTagNumber.enumerated, b_data)

    return b_data


def pack_asn1_general_string(
    value: typing.Union[str, bytes],
    tag: bool = True,
    encoding: str = "ascii",
) -> bytes:
    """Packs an string value into an ASN.1 GeneralString byte value with optional universal tagging."""
    b_data = to_bytes(value, encoding=encoding)
    if tag:
        b_data = pack_asn1(TagClass.universal, False, TypeTagNumber.general_string, b_data)

    return b_data


def pack_asn1_integer(
    value: int,
    tag: bool = True,
) -> bytes:
    """Packs an int value into an ASN.1 INTEGER byte value with optional universal tagging."""
    # Thanks to https://github.com/andrivet/python-asn1 for help with the negative value logic.
    is_negative = False
    limit = 0x7F
    if value < 0:
        value = -value
        is_negative = True
        limit = 0x80

    b_int = bytearray()
    while value > limit:
        val = value & 0xFF

        if is_negative:
            val = 0xFF - val

        b_int.append(val)
        value >>= 8

    b_int.append(((0xFF - value) if is_negative else value) & 0xFF)

    if is_negative:
        for idx, val in enumerate(b_int):
            if val < 0xFF:
                b_int[idx] += 1
                break

            b_int[idx] = 0

    if is_negative and b_int[-1] == 0x7F:  # Two's complement corner case
        b_int.append(0xFF)

    b_int.reverse()

    b_value = bytes(b_int)
    if tag:
        b_value = pack_asn1(TagClass.universal, False, TypeTagNumber.integer, b_value)

    return b_value


def pack_asn1_object_identifier(
    oid: str,
    tag: bool = True,
) -> bytes:
    """Packs an str value into an ASN.1 OBJECT IDENTIFIER byte value with optional universal tagging."""
    b_oid = bytearray()
    oid_split = [int(i) for i in oid.split(".")]

    if len(oid_split) < 2:
        raise ValueError("An OID must have 2 or more elements split by '.'")

    # The first byte of the OID is the first 2 elements (x.y) as (x * 40) + y
    b_oid.append((oid_split[0] * 40) + oid_split[1])

    for val in oid_split[2:]:
        b_oid.extend(_pack_asn1_octet_number(val))

    b_value = bytes(b_oid)
    if tag:
        b_value = pack_asn1(TagClass.universal, False, TypeTagNumber.object_identifier, b_value)

    return b_value


def pack_asn1_octet_string(
    b_data: bytes,
    tag: bool = True,
) -> bytes:
    """Packs an bytes value into an ASN.1 OCTET STRING byte value with optional universal tagging."""
    if tag:
        b_data = pack_asn1(TagClass.universal, False, TypeTagNumber.octet_string, b_data)

    return b_data


def pack_asn1_sequence(
    sequence: typing.List[bytes],
    tag: bool = True,
) -> bytes:
    """Packs a list of encoded bytes into an ASN.1 SEQUENCE byte value with optional universal tagging."""
    b_data = b"".join(sequence)
    if tag:
        b_data = pack_asn1(TagClass.universal, True, TypeTagNumber.sequence, b_data)

    return b_data


def _pack_asn1_octet_number(num: int) -> bytes:
    """Packs an int number into an ASN.1 integer value that spans multiple octets."""
    num_octets = bytearray()

    while num:
        # Get the 7 bit value of the number.
        octet_value = num & 0b01111111

        # Set the MSB if this isn't the first octet we are processing (overall last octet)
        if len(num_octets):
            octet_value |= 0b10000000

        num_octets.append(octet_value)

        # Shift the number by 7 bits as we've just processed them.
        num >>= 7

    # Finally we reverse the order so the higher octets are first.
    num_octets.reverse()

    return num_octets


def unpack_asn1(b_data: bytes) -> typing.Tuple[ASN1Value, bytes]:
    """Unpacks an ASN.1 TLV into each element.

    Unpacks the raw ASN.1 value into a `ASN1Value` tuple and returns the remaining bytes that are not part of the
    ASN.1 TLV.

    Args:
        b_data: The raw bytes to unpack as an ASN.1 TLV.

    Returns:
        ASN1Value: The ASN.1 value that is unpacked from the raw bytes passed in.
        bytes: Any remaining bytes that are not part of the ASN1Value.
    """
    octet1 = struct.unpack("B", b_data[:1])[0]
    tag_class = TagClass((octet1 & 0b11000000) >> 6)
    constructed = bool(octet1 & 0b00100000)
    tag_number = octet1 & 0b00011111

    length_offset = 1
    if tag_number == 31:
        tag_number, octet_count = _unpack_asn1_octet_number(b_data[1:])
        length_offset += octet_count

    if tag_class == TagClass.universal:
        tag_number = TypeTagNumber(tag_number)

    b_data = b_data[length_offset:]

    length = struct.unpack("B", b_data[:1])[0]
    length_octets = 1

    if length & 0b10000000:
        # If the MSB is set then the length octet just contains the number of octets that encodes the actual length.
        length_octets += length & 0b01111111
        length = 0

        for idx in range(1, length_octets):
            octet_val = struct.unpack("B", b_data[idx : idx + 1])[0]
            length += octet_val << (8 * (length_octets - 1 - idx))

    value = ASN1Value(
        tag_class=tag_class,
        constructed=constructed,
        tag_number=tag_number,
        b_data=b_data[length_octets : length_octets + length],
    )

    return value, b_data[length_octets + length :]


def unpack_asn1_bit_string(value: typing.Union[ASN1Value, bytes]) -> bytes:
    """Unpacks an ASN.1 BIT STRING value."""
    b_data = extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.bit_string)

    # First octet is the number of unused bits in the last octet from the LSB.
    unused_bits = struct.unpack("B", b_data[:1])[0]
    last_octet = struct.unpack("B", b_data[-2:-1])[0]
    last_octet = (last_octet >> unused_bits) << unused_bits

    return b_data[1:-1] + struct.pack("B", last_octet)


def unpack_asn1_boolean(value: typing.Union[ASN1Value, bytes]) -> bool:
    """Unpacks an ASN.1 BOOLEAN value."""
    b_data = extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.boolean)

    return b_data != b"\x00"


def unpack_asn1_enumerated(value: typing.Union[ASN1Value, bytes]) -> int:
    """Unpacks an ASN.1 ENUMERATED value."""
    b_data = extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.enumerated)

    return unpack_asn1_integer(b_data)


def unpack_asn1_general_string(value: typing.Union[ASN1Value, bytes]) -> bytes:
    """Unpacks an ASN.1 GeneralString value."""
    return extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.general_string)


def unpack_asn1_generalized_time(value: typing.Union[ASN1Value, bytes]) -> datetime.datetime:
    """Unpacks an ASN.1 GeneralizedTime value."""
    data = to_text(extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.generalized_time))

    # While ASN.1 can have a timezone encoded, KerberosTime is the only thing we use and it is always in UTC with the
    # Z prefix. We strip out the Z because Python 2 doesn't support the %z identifier and add the UTC tz to the object.
    # https://www.rfc-editor.org/rfc/rfc4120#section-5.2.3
    if data.endswith("Z"):
        data = data[:-1]

    err = None
    for datetime_format in ["%Y%m%d%H%M%S.%f", "%Y%m%d%H%M%S"]:
        try:
            dt = datetime.datetime.strptime(data, datetime_format)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError as e:
            err = e

    else:
        raise err  # type: ignore


def unpack_asn1_integer(value: typing.Union[ASN1Value, bytes]) -> int:
    """Unpacks an ASN.1 INTEGER value."""
    b_int = bytearray(extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.integer))

    is_negative = b_int[0] & 0b10000000
    if is_negative:
        # Get the two's compliment.
        for i in range(len(b_int)):
            b_int[i] = 0xFF - b_int[i]

        for i in range(len(b_int) - 1, -1, -1):
            if b_int[i] == 0xFF:
                b_int[i - 1] += 1
                b_int[i] = 0
                break

            else:
                b_int[i] += 1
                break

    int_value = 0
    for val in b_int:
        int_value = (int_value << 8) | val

    if is_negative:
        int_value *= -1

    return int_value


def unpack_asn1_object_identifier(value: typing.Union[ASN1Value, bytes]) -> str:
    """Unpacks an ASN.1 OBJECT IDENTIFIER value."""
    b_data = extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.object_identifier)

    first_element = struct.unpack("B", b_data[:1])[0]
    second_element = first_element % 40
    ids = [(first_element - second_element) // 40, second_element]

    idx = 1
    while idx != len(b_data):
        oid, octet_len = _unpack_asn1_octet_number(b_data[idx:])
        ids.append(oid)
        idx += octet_len

    return ".".join([str(i) for i in ids])


def unpack_asn1_octet_string(value: typing.Union[ASN1Value, bytes]) -> bytes:
    """Unpacks an ASN.1 OCTET STRING value."""
    return extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.octet_string)


def unpack_asn1_sequence(value: typing.Union[ASN1Value, bytes]) -> typing.List[ASN1Value]:
    """Unpacks an ASN.1 SEQUENCE value."""
    b_data = extract_asn1_tlv(value, TagClass.universal, TypeTagNumber.sequence)

    values = []
    while b_data:
        v, b_data = unpack_asn1(b_data)
        values.append(v)

    return values


def unpack_asn1_tagged_sequence(value: typing.Union[ASN1Value, bytes]) -> typing.Dict[int, ASN1Value]:
    """Unpacks an ASN.1 SEQUENCE value as a dictionary."""
    return dict([(e.tag_number, unpack_asn1(e.b_data)[0]) for e in unpack_asn1_sequence(value)])


def _unpack_asn1_octet_number(b_data: bytes) -> typing.Tuple[int, int]:
    """Unpacks an ASN.1 INTEGER value that can span across multiple octets."""
    i = 0
    idx = 0
    while True:
        element = struct.unpack("B", b_data[idx : idx + 1])[0]
        idx += 1

        i = (i << 7) + (element & 0b01111111)
        if not element & 0b10000000:
            break

    return i, idx  # int value and the number of octets used.
