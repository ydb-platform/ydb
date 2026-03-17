# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import struct
import typing

from spnego._asn1 import (
    TagClass,
    TypeTagNumber,
    get_sequence_value,
    pack_asn1,
    pack_asn1_bit_string,
    pack_asn1_enumerated,
    pack_asn1_general_string,
    pack_asn1_object_identifier,
    pack_asn1_octet_string,
    pack_asn1_sequence,
    unpack_asn1,
    unpack_asn1_bit_string,
    unpack_asn1_enumerated,
    unpack_asn1_general_string,
    unpack_asn1_object_identifier,
    unpack_asn1_octet_string,
    unpack_asn1_sequence,
    unpack_asn1_tagged_sequence,
)
from spnego._context import GSSMech
from spnego._kerberos import KerberosV5Msg
from spnego._ntlm_raw.messages import NTLMMessage


def pack_mech_type_list(
    mech_list: typing.Union[str, typing.List[str], typing.Tuple[str, ...], typing.Set[str]],
) -> bytes:
    """Packs a list of OIDs for the mechListMIC value.

    Will pack a list of object identifiers to the raw byte string value for the mechListMIC.

    Args:
        mech_list: The list of OIDs to back

    Returns:
        bytes: The byte string of the packed ASN.1 MechTypeList SEQUENCE OF value.
    """
    if not isinstance(mech_list, (list, tuple, set)):
        mech_list = [mech_list]

    return pack_asn1_sequence([pack_asn1_object_identifier(oid) for oid in mech_list])


def unpack_token(
    b_data: bytes,
    mech: typing.Optional[GSSMech] = None,
    unwrap: bool = False,
    encoding: typing.Optional[str] = None,
) -> typing.Any:
    """Unpacks a raw GSSAPI/SPNEGO token to a Python object.

    Unpacks the byte string into a Python object that represents the token passed in. This can return many different
    token types such as:

    * NTLM message(s)
    * SPNEGO/Negotiate init or response
    * Kerberos message(s)

    Args:
        b_data: The raw byte string to unpack.
        mech: A hint as to what the byte string is for.
        unwrap: Whether to unwrap raw bytes to a structured message or return the raw tokens bytes.
        encoding: Optional encoding used when unwrapping NTLM messages.

    Returns:
        any: The unpacked SPNEGO, Kerberos, or NTLM token.
    """
    # First check if the message is an NTLM message.
    if b_data.startswith(b"NTLMSSP\x00"):
        if unwrap:
            return NTLMMessage.unpack(b_data, encoding=encoding)

        else:
            return b_data

    if mech and mech.is_kerberos_oid:
        # A Kerberos value inside an InitialContextToken contains 2 bytes which we ignore.
        raw_data = unpack_asn1(b_data[2:])[0]

    else:
        raw_data = unpack_asn1(b_data)[0]

    if raw_data.tag_class == TagClass.application and mech and mech.is_kerberos_oid:
        return KerberosV5Msg.unpack(unpack_asn1(raw_data.b_data)[0])

    elif raw_data.tag_class == TagClass.application:
        # The first token is encapsulated in an InitialContextToken.
        if raw_data.tag_number != 0:
            raise ValueError("Expecting a tag number of 0 not %s for InitialContextToken" % raw_data.tag_number)

        initial_context_token = InitialContextToken.unpack(raw_data.b_data)

        # unwrap=True is called from python -m spnego and we don't want to loose any info in the output.
        if unwrap:
            return initial_context_token

        this_mech: typing.Optional[GSSMech]
        try:
            this_mech = GSSMech.from_oid(initial_context_token.this_mech)
        except ValueError:
            this_mech = None

        # We currently only support SPNEGO, or raw Kerberos here.
        if this_mech and (this_mech == GSSMech.spnego or (this_mech.is_kerberos_oid and unwrap)):
            return unpack_token(initial_context_token.inner_context_token, mech=this_mech)

        return b_data

    elif raw_data.tag_class == TagClass.context_specific:
        # This is a raw NegotiationToken that is wrapped in a CHOICE or 0 or 1.
        if raw_data.tag_number == 0:
            return NegTokenInit.unpack(raw_data.b_data)

        elif raw_data.tag_number == 1:
            return NegTokenResp.unpack(raw_data.b_data)

        else:
            raise ValueError("Unknown NegotiationToken CHOICE %d, only expecting 0 or 1" % raw_data.tag_number)

    elif unwrap:
        # Could also be the ASN.1 Sequence of the Kerberos message.
        return KerberosV5Msg.unpack(raw_data)

    else:
        return b_data


# https://www.rfc-editor.org/rfc/rfc4178.html#section-4.2.1 - ContextFlags
class ContextFlags(enum.IntFlag):
    deleg = 0
    mutual = 1
    replay = 2
    sequence = 3
    anon = 4
    conf = 5
    integ = 6

    @classmethod
    def native_labels(cls) -> typing.Dict["ContextFlags", str]:
        return {
            ContextFlags.deleg: "delegFlag",
            ContextFlags.mutual: "mutualFlag",
            ContextFlags.replay: "replayFlag",
            ContextFlags.sequence: "sequenceFlag",
            ContextFlags.anon: "anonFlag",
            ContextFlags.conf: "confFlag",
            ContextFlags.integ: "integFlag",
        }


# https://www.rfc-editor.org/rfc/rfc4178.html#section-4.2.2 - negState
class NegState(enum.IntEnum):
    accept_complete = 0
    accept_incomplete = 1
    reject = 2
    request_mic = 3

    @classmethod
    def native_labels(cls) -> typing.Dict["NegState", str]:
        return {
            NegState.accept_complete: "accept-complete",
            NegState.accept_incomplete: "accept-incomplete",
            NegState.reject: "reject",
            NegState.request_mic: "request-mic",
        }


class InitialContextToken:
    """GSSAPI InitialContextToken object.

    The InitialContextToken is the ASN.1 structure that contains the first GSSAPI token that is sent across the wire.
    The ASN.1 definition for this structure is defined in `RFC 2743 3.1`_::

        MechType ::= OBJECT IDENTIFIER
        -- data structure definitions
        -- callers must be able to distinguish among
        -- InitialContextToken, SubsequentContextToken,
        -- PerMsgToken, and SealedMessage data elements
        -- based on the usage in which they occur

        InitialContextToken ::=
        -- option indication (delegation, etc.) indicated within
        -- mechanism-specific token
        [APPLICATION 0] IMPLICIT SEQUENCE {
            thisMech MechType,
            innerContextToken ANY DEFINED BY thisMech
               -- contents mechanism-specific
               -- ASN.1 structure not required
            }

    Args:
        mech: The OID that defines the structure of the `token`.
        token: The token of the GSSAPI value.

    Attributes:
        this_mech (str): The object identifier that identifies what the inner_context_token is for.
        inner_context_token (bytes): The token value as defined by `this_mech`.

    .. _RFC 2743 3.1:
        https://www.rfc-editor.org/rfc/rfc2743#section-3.1.
    """

    def __init__(self, mech: typing.Union[GSSMech, str], token: bytes) -> None:
        if isinstance(mech, GSSMech):
            mech = mech.value

        self.this_mech = mech
        self.inner_context_token = token

    @property
    def token(self) -> typing.Any:
        mech: typing.Optional[GSSMech]
        try:
            mech = GSSMech.from_oid(self.this_mech)
        except ValueError:
            mech = None

        return unpack_token(self.inner_context_token, mech=mech)

    def pack(self) -> bytes:
        """Packs the InitialContextToken as a byte string."""
        return pack_asn1(
            TagClass.application,
            True,
            0,
            pack_asn1_object_identifier(self.this_mech, tag=True) + self.inner_context_token,
        )

    @staticmethod
    def unpack(b_data: bytes) -> "InitialContextToken":
        """Unpacks the InitialContextToken TLV value."""
        this_mech, inner_context_token = unpack_asn1(b_data)
        mech = unpack_asn1_object_identifier(this_mech)

        return InitialContextToken(mech, inner_context_token)


class NegTokenInit:
    """The NegTokenInit GSSAPI value.

    This is the initial negotiation message token in a GSSAPI exchange. Typically the `NegTokenInit` value is sent
    when sending the first authentication token. The `NegTokenInit2` token is an extension that adds the `negHints`
    field. Unfortunately as the tag number for the `mechListMIC` is the same for `negHints` unpacking the value
    requires some extra checks.

    The ASN.1 definition for the NegTokenInit structure is defined in `RFC 4178 4.2.1`_::

        NegTokenInit ::= SEQUENCE {
            mechTypes       [0] MechTypeList,
            reqFlags        [1] ContextFlags  OPTIONAL,
            -- inherited from RFC 2478 for backward compatibility,
            -- RECOMMENDED to be left out
            mechToken       [2] OCTET STRING  OPTIONAL,
            mechListMIC     [3] OCTET STRING  OPTIONAL,
            ...
        }
        ContextFlags ::= BIT STRING {
            delegFlag       (0),
            mutualFlag      (1),
            replayFlag      (2),
            sequenceFlag    (3),
            anonFlag        (4),
            confFlag        (5),
            integFlag       (6)
        } (SIZE (32))

    The ASN.1 definition for the `NegTokenInit2`_ structure is defined as::

        NegHints ::= SEQUENCE {
            hintName[0] GeneralString OPTIONAL,
            hintAddress[1] OCTET STRING OPTIONAL
        }
        NegTokenInit2 ::= SEQUENCE {
            mechTypes[0] MechTypeList OPTIONAL,
            reqFlags [1] ContextFlags OPTIONAL,
            mechToken [2] OCTET STRING OPTIONAL,
            negHints [3] NegHints OPTIONAL,
            mechListMIC [4] OCTET STRING OPTIONAL,
            ...
        }

    Args:
        mech_types: One or more security mechanisms available for the initiator, in decreasing preference order.
        req_flags: Should be omitted, service options that are requested to establish the context.
        mech_token: Contains the optimistic mechanism token.
        hint_name: Used for the NegTokenInit2 structure only, should be omitted.
        hint_address: Used for the NegTokenINit2 structure only, should be omitted.
        mech_list_mic: The message integrity code (MIC) token.

    Attributes:
        mech_types (List[str]): See args.
        req_flags (ContextFlags): See args.
        mech_token (bytes): See args.
        hint_name (bytes): See args.
        hint_address (bytes): See args.
        mech_list_mic (bytes): See args.

    .. _RFC 4178 4.2.1:
        https://www.rfc-editor.org/rfc/rfc4178.html#section-4.2.1

    .. _NegTokenInit2:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-spng/8e71cf53-e867-4b79-b5b5-38c92be3d472
    """

    def __init__(
        self,
        mech_types: typing.Optional[typing.List[str]] = None,
        req_flags: typing.Optional[ContextFlags] = None,
        mech_token: typing.Optional[bytes] = None,
        hint_name: typing.Optional[bytes] = None,
        hint_address: typing.Optional[bytes] = None,
        mech_list_mic: typing.Optional[bytes] = None,
    ) -> None:
        self.mech_types = mech_types or []
        self.req_flags = req_flags
        self.mech_token = mech_token
        self.hint_name = hint_name
        self.hint_address = hint_address
        self.mech_list_mic = mech_list_mic

    def pack(self) -> bytes:
        """Packs the NegTokenInit as a byte string."""

        def pack_elements(
            value_map: typing.Iterable[typing.Tuple[int, typing.Any, typing.Callable]],
        ) -> typing.List[bytes]:
            elements = []
            for tag, value, pack_func in value_map:
                if value is not None:
                    elements.append(pack_asn1(TagClass.context_specific, True, tag, pack_func(value)))

            return elements

        req_flags = struct.pack("B", self.req_flags) if self.req_flags is not None else None
        base_map: typing.List[typing.Tuple[int, typing.Any, typing.Callable]] = [
            (0, self.mech_types, pack_mech_type_list),
            (1, req_flags, pack_asn1_bit_string),
            (2, self.mech_token, pack_asn1_octet_string),
        ]

        # The placement of the mechListMIC is dependent on whether we are packing a NegTokenInit with or without the
        # negHints field.
        neg_hints = pack_elements(
            [
                (0, self.hint_name, pack_asn1_general_string),
                (1, self.hint_address, pack_asn1_octet_string),
            ]
        )

        if neg_hints:
            base_map.append((3, neg_hints, pack_asn1_sequence))
            base_map.append((4, self.mech_list_mic, pack_asn1_octet_string))

        else:
            base_map.append((3, self.mech_list_mic, pack_asn1_octet_string))

        init_sequence = pack_elements(base_map)

        # The NegTokenInit will always be wrapped in an InitialContextToken -> NegotiationToken - CHOICE 0.
        b_data = pack_asn1_sequence(init_sequence)
        return InitialContextToken(GSSMech.spnego.value, pack_asn1(TagClass.context_specific, True, 0, b_data)).pack()

    @staticmethod
    def unpack(b_data: bytes) -> "NegTokenInit":
        """Unpacks the NegTokenInit TLV value."""
        neg_seq = unpack_asn1_tagged_sequence(unpack_asn1(b_data)[0])

        mech_types = [
            unpack_asn1_object_identifier(m)
            for m in get_sequence_value(neg_seq, 0, "NegTokenInit", "mechTypes", unpack_asn1_sequence) or []
        ]

        req_flags = get_sequence_value(neg_seq, 1, "NegTokenInit", "reqFlags", unpack_asn1_bit_string)
        if req_flags:
            # Can be up to 32 bits in length but RFC 4178 states "Implementations should not expect to receive exactly
            # 32 bits in an encoding of ContextFlags." The spec also documents req flags up to 6 so let's just get the
            # last byte. In reality we shouldn't ever receive this but it's left here for posterity.
            req_flags = ContextFlags(bytearray(req_flags)[-1])

        mech_token = get_sequence_value(neg_seq, 2, "NegTokenInit", "mechToken", unpack_asn1_octet_string)

        hint_name = hint_address = mech_list_mic = None
        if 3 in neg_seq:
            # Microsoft helpfully sends a NegTokenInit2 payload which sets 'negHints [3] NegHints OPTIONAL' and the
            # mechListMIC is actually at the 4th sequence entry. Because the NegTokenInit2 has the same choice in
            # NegotiationToken as NegTokenInit ([0]) we can only differentiate when unpacking based on the class/tags.
            tag_class = neg_seq[3].tag_class
            tag_number = neg_seq[3].tag_number

            if tag_class == TagClass.universal and tag_number == TypeTagNumber.sequence:
                neg_hints = unpack_asn1_tagged_sequence(neg_seq[3].b_data)

                # Windows 2000, 2003, and XP put the SPN encoded as the OEM code page, because there's no sane way
                # to decode this without prior knowledge a GeneralString stays a byte string in Python.
                # https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-spng/211417c4-11ef-46c0-a8fb-f178a51c2088#Appendix_A_5
                hint_name = get_sequence_value(neg_hints, 0, "NegHints", "hintName", unpack_asn1_general_string)
                hint_address = get_sequence_value(neg_hints, 1, "NegHints", "hintAddress", unpack_asn1_octet_string)

            else:
                # Wasn't a sequence, should be mechListMIC.
                mech_list_mic = get_sequence_value(neg_seq, 3, "NegTokenInit", "mechListMIC", unpack_asn1_octet_string)

        if not mech_list_mic:
            mech_list_mic = get_sequence_value(neg_seq, 4, "NegTokenInit2", "mechListMIC", unpack_asn1_octet_string)

        return NegTokenInit(mech_types, req_flags, mech_token, hint_name, hint_address, mech_list_mic)


class NegTokenResp:
    """The NegTokenResp GSSAPI value.

    This is the message token in a GSSAPI exchange that is used for subsequent messages after the `NegTokenInit` has
    been exchanged.

    The ASN.1 definition for the NegTokenResp structure is defined in `RFC 4178 4.2.2`_::

        NegTokenResp ::= SEQUENCE {
            negState       [0] ENUMERATED {
                accept-completed    (0),
                accept-incomplete   (1),
                reject              (2),
                request-mic         (3)
            }                                 OPTIONAL,
            -- REQUIRED in the first reply from the target
            supportedMech   [1] MechType      OPTIONAL,
            -- present only in the first reply from the target
            responseToken   [2] OCTET STRING  OPTIONAL,
            mechListMIC     [3] OCTET STRING  OPTIONAL,
            ...
        }

    Args:
        neg_state: The state of the negotiation.
        supported_mech: Should only be present in the first reply, must be one of the mech(s) offered by the initiator.
        response_token: Contains the token specific to the mechanism selected.
        mech_list_mic: The message integrity code (MIC) token.

    Attributes:
        neg_state (NegState): See args.
        supported_mech (str): See args.
        response_token (bytes): See args.
        mech_list_mic (bytes): See args.

    .. _RFC 4178 4.2.2:
        https://www.rfc-editor.org/rfc/rfc4178.html#section-4.2.2
    """

    def __init__(
        self,
        neg_state: typing.Optional[NegState] = None,
        supported_mech: typing.Optional[str] = None,
        response_token: typing.Optional[bytes] = None,
        mech_list_mic: typing.Optional[bytes] = None,
    ) -> None:
        self.neg_state = neg_state
        self.supported_mech = supported_mech
        self.response_token = response_token
        self.mech_list_mic = mech_list_mic

    def pack(self) -> bytes:
        """Packs the NegTokenResp as a byte string."""
        value_map: typing.List[typing.Tuple[int, typing.Any, typing.Callable[[typing.Any], bytes]]] = [
            (0, self.neg_state, pack_asn1_enumerated),
            (1, self.supported_mech, pack_asn1_object_identifier),
            (2, self.response_token, pack_asn1_octet_string),
            (3, self.mech_list_mic, pack_asn1_octet_string),
        ]
        elements = []
        for tag, value, pack_func in value_map:
            if value is not None:
                elements.append(pack_asn1(TagClass.context_specific, True, tag, pack_func(value)))

        # The NegTokenResp will always be wrapped NegotiationToken - CHOICE 1.
        b_data = pack_asn1_sequence(elements)
        return pack_asn1(TagClass.context_specific, True, 1, b_data)

    @staticmethod
    def unpack(b_data: bytes) -> "NegTokenResp":
        """Unpacks the NegTokenResp TLV value."""
        neg_seq = unpack_asn1_tagged_sequence(unpack_asn1(b_data)[0])

        neg_state = get_sequence_value(neg_seq, 0, "NegTokenResp", "negState", unpack_asn1_enumerated)
        if neg_state is not None:
            neg_state = NegState(neg_state)

        supported_mech = get_sequence_value(neg_seq, 1, "NegTokenResp", "supportedMech", unpack_asn1_object_identifier)
        response_token = get_sequence_value(neg_seq, 2, "NegTokenResp", "responseToken", unpack_asn1_octet_string)
        mech_list_mic = get_sequence_value(neg_seq, 3, "NegTokenResp", "mechListMIC", unpack_asn1_octet_string)

        return NegTokenResp(neg_state, supported_mech, response_token, mech_list_mic)
