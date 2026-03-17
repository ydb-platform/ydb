# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

"""Kerberos Messages

Various Kerberos message structures that is used by 'python -m spnego' to unpack raw ASN.1 values and print out a
pretty structure for end users. This code is not used in the actual spnego authentication processes and is just used
for debugging purposes.
"""

import base64
import collections
import datetime
import enum
import struct
import typing

from spnego._asn1 import (
    ASN1Value,
    TagClass,
    extract_asn1_tlv,
    get_sequence_value,
    unpack_asn1,
    unpack_asn1_bit_string,
    unpack_asn1_general_string,
    unpack_asn1_generalized_time,
    unpack_asn1_integer,
    unpack_asn1_octet_string,
    unpack_asn1_sequence,
    unpack_asn1_tagged_sequence,
)
from spnego._text import to_text


def _enum_labels(
    value: typing.Union[int, str, enum.Enum],
    enum_type: typing.Optional[typing.Type] = None,
) -> typing.Dict[int, str]:
    """Gets the human friendly labels of a known enum and what value they map to."""

    def get_labels(v: typing.Any) -> typing.Dict[int, str]:
        return typing.cast(typing.Dict[int, str], getattr(v, "native_labels", lambda: {})())

    return get_labels(enum_type) if enum_type else get_labels(value)


def parse_enum(
    value: typing.Union[int, str, enum.Enum],
    enum_type: typing.Optional[typing.Type] = None,
) -> str:
    """Parses an IntEnum into a human representative object of that enum."""
    enum_name = "UNKNOWN"
    if isinstance(value, enum.Enum):
        enum_name = value.name

    labels = _enum_labels(value, enum_type)
    value = value.value if isinstance(value, enum.Enum) else value

    for v, name in labels.items():
        if value == v:
            enum_name = name
            break

    return "%s (%s)" % (enum_name, value)


def parse_flags(
    value: typing.Union[int, enum.IntFlag],
    enum_type: typing.Optional[typing.Type] = None,
) -> typing.Dict[str, typing.Any]:
    """Parses an IntFlag into each flag value that is set."""
    raw_value = int(value)
    flags = []

    labels = _enum_labels(value, enum_type)
    value = int(value)

    for v, name in labels.items():
        if value & v == v:
            value &= ~v
            flags.append("%s (%d)" % (name, v))

    if value != 0:
        flags.append("UNKNOWN (%d)" % value)

    return {
        "raw": raw_value,
        "flags": flags,
    }


def parse_kerberos_token(
    token: typing.Union["KerberosV5Msg", "PAData", "PAETypeInfo2", "EncryptedData", "Ticket", "KdcReqBody"],
    secret: typing.Optional[str] = None,
    encoding: typing.Optional[str] = None,
) -> typing.Union[str, typing.Dict[str, typing.Any]]:
    """Parses a KerberosV5Msg object to a dict."""
    text_encoding = encoding if encoding else "utf-8"

    def parse_default(value: typing.Any) -> typing.Any:
        return value

    def parse_datetime(value: datetime.datetime) -> str:
        return value.isoformat()

    def parse_text(value: bytes) -> str:
        return to_text(value, encoding=text_encoding, errors="replace")

    def parse_bytes(value: bytes) -> str:
        return base64.b16encode(value).decode()

    def parse_principal_name(value: PrincipalName) -> typing.Dict[str, typing.Any]:
        return {
            "name-type": parse_enum(value.name_type),
            "name-string": [parse_text(v) for v in value.value],
        }

    def parse_host_address(value: HostAddress) -> typing.Dict[str, typing.Any]:
        return {
            "addr-type": parse_enum(value.addr_type),
            "address": parse_text(value.value),
        }

    def parse_token(value: typing.Any) -> typing.Union[str, typing.Dict[str, typing.Any]]:
        return parse_kerberos_token(value, secret, text_encoding)

    if isinstance(token, bytes):
        return parse_bytes(token)

    msg = {}
    for name, attr_name, attr_type in getattr(token, "PARSE_MAP", {}):
        attr_value = getattr(token, attr_name)

        parse_args = []
        if isinstance(attr_type, tuple):
            parse_args.append(attr_type[1])
            attr_type = attr_type[0]

        parse_func: typing.Callable = {  # type: ignore[assignment] # No idea why
            ParseType.default: parse_default,
            ParseType.enum: parse_enum,
            ParseType.flags: parse_flags,
            ParseType.datetime: parse_datetime,
            ParseType.text: parse_text,
            ParseType.bytes: parse_bytes,
            ParseType.principal_name: parse_principal_name,
            ParseType.host_address: parse_host_address,
            ParseType.token: parse_token,
        }[attr_type]

        if attr_value is None:
            parsed_value = None

        elif isinstance(attr_value, list):
            parsed_value = [parse_func(v, *parse_args) if v is not None else None for v in attr_value]

        else:
            parsed_value = parse_func(attr_value, *parse_args)

        msg[name] = parsed_value

    return msg


def unpack_hostname(value: typing.Union[bytes, ASN1Value]) -> "HostAddress":
    """Unpacks an ASN.1 value to a HostAddress."""
    s = unpack_asn1_tagged_sequence(value)

    name_type = KerberosHostAddressType(get_sequence_value(s, 0, "HostAddress", "addr-type", unpack_asn1_integer))
    name = get_sequence_value(s, 1, "HostAddress", "address", unpack_asn1_octet_string)

    return HostAddress(name_type, name)


def unpack_principal_name(value: typing.Union[bytes, ASN1Value]) -> "PrincipalName":
    """Unpacks an ASN.1 value to a PrincipalName."""
    s = unpack_asn1_tagged_sequence(value)

    name_type = KerberosPrincipalNameType(get_sequence_value(s, 0, "PrincipalName", "name-type", unpack_asn1_integer))
    name = [
        unpack_asn1_general_string(n)
        for n in get_sequence_value(s, 1, "PrincipalName", "name-string", unpack_asn1_sequence)
    ]

    return PrincipalName(name_type, name)


HostAddress = collections.namedtuple("HostAddress", ["addr_type", "value"])
"""Kerberos HostAddress and HostAddresses

A Kerberos Host Address ASN.1 definition is defined in `RFC 4120 5.2.5`_::

    HostAddress     ::= SEQUENCE  {
        addr-type       [0] Int32,
        address         [1] OCTET STRING
    }

Attributes:
    addr_type (KerberosHostAddressType): The type of address that was encoded.
    value (bytes): The address as a byte string.

.. _RFC 4120 5.2.5:
    https://www.rfc-editor.org/rfc/rfc4120#section-5.2.5
"""

PrincipalName = collections.namedtuple("PrincipalName", ["name_type", "value"])
"""Kerberos Realm and PrincipalName

A Kerberos Principal Name ASN.1 definition is defined in `RFC 4120 5.2.2`_::

    PrincipalName   ::= SEQUENCE {
        name-type       [0] Int32,
        name-string     [1] SEQUENCE OF KerberosString
    }

Attributes:
    name_type (KerberosPrincipalNameType): The type of name the was encoded.
    value (List[bytes]): Each component that forms the name.

.. _RFC 4120 5.2.2:
    https://www.rfc-editor.org/rfc/rfc4120#section-5.2.2
"""


# https://www.rfc-editor.org/rfc/rfc4120#section-5.5.1 - ap-options
class KerberosAPOptions(enum.IntFlag):
    mutual_required = 0x00000020
    use_session_key = 0x00000040
    reserved = 0x00000080

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosAPOptions", str]:
        return {
            KerberosAPOptions.mutual_required: "mutual-required",
            KerberosAPOptions.use_session_key: "use-session-key",
            KerberosAPOptions.reserved: "reserved",
        }


# https://www.rfc-editor.org/rfc/rfc4120#section-5.4.1 - KDCOptions
class KerberosKDCOptions(enum.IntFlag):
    reserved = 0x80000000
    forwardable = 0x40000000
    forwarded = 0x20000000
    proxiable = 0x10000000
    proxy = 0x08000000
    allow_postdate = 0x04000000
    postdated = 0x02000000
    unused7 = 0x01000000
    renewable = 0x00800000
    unused9 = 0x00400000
    unused10 = 0x00200000
    opt_hardware_auth = 0x00100000
    unused12 = 0x00080000
    unused13 = 0x00040000
    constrained_delegation = 0x00020000
    canonicalize = 0x00010000
    request_anonymous = 0x00008000
    unused17 = 0x00004000
    unused18 = 0x00002000
    unused19 = 0x00001000
    unused20 = 0x00000800
    unused21 = 0x00000400
    unused22 = 0x00000200
    unused23 = 0x00000100
    unused24 = 0x00000080
    unused25 = 0x00000040
    disable_transited_check = 0x00000020
    renewable_ok = 0x00000010
    enc_tkt_in_skey = 0x00000008
    unused29 = 0x00000004
    renew = 0x00000002
    validate = 0x00000001

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosKDCOptions", str]:
        return {
            KerberosKDCOptions.reserved: "reserved",
            KerberosKDCOptions.forwardable: "forwardable",
            KerberosKDCOptions.forwarded: "forwarded",
            KerberosKDCOptions.proxiable: "proxiable",
            KerberosKDCOptions.proxy: "proxy",
            KerberosKDCOptions.allow_postdate: "allow-postdate",
            KerberosKDCOptions.postdated: "postdated",
            KerberosKDCOptions.unused7: "unused7",
            KerberosKDCOptions.renewable: "renewable",
            KerberosKDCOptions.unused9: "unused9",
            KerberosKDCOptions.unused10: "unused10",
            KerberosKDCOptions.opt_hardware_auth: "opt-hardware-auth",
            KerberosKDCOptions.unused12: "unused12",
            KerberosKDCOptions.unused13: "unused13",
            KerberosKDCOptions.constrained_delegation: "constrained-delegation",
            KerberosKDCOptions.canonicalize: "canonicalize",
            KerberosKDCOptions.request_anonymous: "request-anonymous",
            KerberosKDCOptions.unused17: "unused17",
            KerberosKDCOptions.unused18: "unused18",
            KerberosKDCOptions.unused19: "unused19",
            KerberosKDCOptions.unused20: "unused20",
            KerberosKDCOptions.unused21: "unused21",
            KerberosKDCOptions.unused22: "unused22",
            KerberosKDCOptions.unused23: "unused23",
            KerberosKDCOptions.unused24: "unused24",
            KerberosKDCOptions.unused25: "unused25",
            KerberosKDCOptions.disable_transited_check: "disable-transited-check",
            KerberosKDCOptions.renewable_ok: "renewable-ok",
            KerberosKDCOptions.enc_tkt_in_skey: "enc-tkt-in-skey",
            KerberosKDCOptions.unused29: "unused29",
            KerberosKDCOptions.renew: "renew",
            KerberosKDCOptions.validate: "validate",
        }


# https://ldapwiki.com/wiki/Kerberos%20Encryption%20Types - etypes
class KerberosEncryptionType(enum.IntEnum):
    des_cbc_crc = 0x0001
    des_cbc_md4 = 0x0002
    des_cbc_md5 = 0x0003
    des_cbc_raw = 0x0004
    des3_cbc_raw = 0x0006
    des3_cbc_sha1 = 0x0010
    aes128_cts_hmac_sha1_96 = 0x0011
    aes256_cts_hmac_sha1_96 = 0x0012
    aes128_cts_hmac_sha256_128 = 0x0013
    aes256_cts_hmac_sha384_192 = 0x0014
    rc4_hmac = 0x0017
    rc4_hmac_exp = 0x0018
    camellia128_cts_cmac = 0x0019
    camellia256_cts_cmac = 0x001A

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosEncryptionType", str]:
        return {
            KerberosEncryptionType.des_cbc_crc: "DES_CBC_CRC",
            KerberosEncryptionType.des_cbc_md4: "DES_CBC_MD4",
            KerberosEncryptionType.des_cbc_md5: "DES_CBC_MD5",
            KerberosEncryptionType.des_cbc_raw: "DES_CBC_RAW",
            KerberosEncryptionType.des3_cbc_raw: "DES3_CBC_RAW",
            KerberosEncryptionType.des3_cbc_sha1: "DES3_CBC_SHA1",
            KerberosEncryptionType.aes128_cts_hmac_sha1_96: "AES128_CTS_HMAC_SHA1_96",
            KerberosEncryptionType.aes256_cts_hmac_sha1_96: "AES256_CTS_HMAC_SHA1_96",
            KerberosEncryptionType.aes128_cts_hmac_sha256_128: "AES128_CTS_HMAC_SHA256_128",
            KerberosEncryptionType.aes256_cts_hmac_sha384_192: "AES256_CTS_HMAC_SHA384_192",
            KerberosEncryptionType.rc4_hmac: "RC4_HMAC",
            KerberosEncryptionType.rc4_hmac_exp: "RC4_HMAC_EXP",
            KerberosEncryptionType.camellia128_cts_cmac: "CAMELLIA128_CTS_CMAC",
            KerberosEncryptionType.camellia256_cts_cmac: "CAMELLIA256_CTS_CMAC",
        }


# https://www.rfc-editor.org/rfc/rfc4120#section-7.5.9
class KerberosErrorCode(enum.IntEnum):
    none = 0
    name_exp = 1
    service_exp = 2
    bad_pvno = 3
    c_old_mast_kvno = 4
    s_old_mast_kvno = 5
    c_principal_unknown = 6
    s_principal_unknown = 7
    principal_not_unique = 8
    null_key = 9
    cannot_postdate = 10
    never_valid = 11
    policy = 12
    badoption = 13
    etype_nosupp = 14
    sumtype_nosupp = 15
    padata_type_nosupp = 16
    trtype_nosupp = 17
    client_revoked = 18
    service_revoked = 19
    tgt_revoked = 20
    client_notyet = 21
    service_notyet = 22
    key_expired = 23
    preauth_failed = 24
    preauth_required = 25
    server_nomatch = 26
    must_use_user2user = 27
    path_not_accepted = 28
    kdc_svc_unavailable = 29
    ap_bad_integrity = 31
    ap_txt_expired = 32
    ap_tkt_nyv = 33
    ap_repeat = 34
    ap_not_use = 35
    ap_badmatch = 36
    ap_skew = 37
    ap_badaddr = 38
    ap_badversion = 39
    ap_msg_type = 40
    ap_modified = 41
    ap_badorder = 42
    ap_badkeyver = 44
    ap_nokey = 45
    ap_mut_fail = 46
    ap_baddirection = 47
    ap_method = 48
    ap_badseq = 49
    ap_inapp_cksum = 50
    ap_path_not_accepted = 51
    response_too_big = 52
    generic = 60
    field_toolong = 61
    kdc_client_not_trusted = 62
    kdc_not_trusted = 53
    kdc_invalid_sig = 64
    kdc_key_too_weak = 65
    kdc_certificate_mismatch = 66
    ap_no_tgt = 67
    kdc_wrong_realm = 68
    ap_user_to_user_required = 69
    kdc_cant_verify_certificate = 70
    kdc_invalid_certificate = 71
    kdc_revoked_certificate = 72
    kdc_revocation_status_unknown = 73
    kdc_revocation_status_unavailable = 74
    kdc_client_name_mismatch = 75
    kdc_name_mismatch = 76

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosErrorCode", str]:
        return {
            KerberosErrorCode.none: "KDC_ERR_NONE",
            KerberosErrorCode.name_exp: "KDC_ERR_NAME_EXP",
            KerberosErrorCode.service_exp: "KDC_ERR_SERVICE_EXP",
            KerberosErrorCode.bad_pvno: "KDC_ERR_BAD_PVNO",
            KerberosErrorCode.c_old_mast_kvno: "KDC_ERR_C_OLD_MAST_KVNO",
            KerberosErrorCode.s_old_mast_kvno: "KDC_ERR_S_OLD_MAST_KVNO",
            KerberosErrorCode.c_principal_unknown: "KDC_ERR_C_PRINCIPAL_UNKNOWN",
            KerberosErrorCode.s_principal_unknown: "KDC_ERR_S_PRINCIPAL_UNKNOWN",
            KerberosErrorCode.principal_not_unique: "KDC_ERR_PRINCIPAL_NOT_UNIQUE",
            KerberosErrorCode.null_key: "KDC_ERR_NULL_KEY",
            KerberosErrorCode.cannot_postdate: "KDC_ERR_CANNOT_POSTDATE",
            KerberosErrorCode.never_valid: "KDC_ERR_NEVER_VALID",
            KerberosErrorCode.policy: "KDC_ERR_POLICY",
            KerberosErrorCode.badoption: "KDC_ERR_BADOPTION",
            KerberosErrorCode.etype_nosupp: "KDC_ERR_ETYPE_NOSUPP",
            KerberosErrorCode.sumtype_nosupp: "KDC_ERR_SUMTYPE_NOSUPP",
            KerberosErrorCode.padata_type_nosupp: "KDC_ERR_PADATA_TYPE_NOSUPP",
            KerberosErrorCode.trtype_nosupp: "KDC_ERR_TRTYPE_NOSUPP",
            KerberosErrorCode.client_revoked: "KDC_ERR_CLIENT_REVOKED",
            KerberosErrorCode.service_revoked: "KDC_ERR_SERVICE_REVOKED",
            KerberosErrorCode.tgt_revoked: "KDC_ERR_TGT_REVOKED",
            KerberosErrorCode.client_notyet: "KDC_ERR_CLIENT_NOTYET",
            KerberosErrorCode.service_notyet: "KDC_ERR_SERVICE_NOTYET",
            KerberosErrorCode.key_expired: "KDC_ERR_KEY_EXPIRED",
            KerberosErrorCode.preauth_failed: "KDC_ERR_PREAUTH_FAILED",
            KerberosErrorCode.preauth_required: "KDC_ERR_PREAUTH_REQUIRED",
            KerberosErrorCode.server_nomatch: "KDC_ERR_SERVER_NOMATCH",
            KerberosErrorCode.must_use_user2user: "KDC_ERR_MUST_USE_USER2USER",
            KerberosErrorCode.path_not_accepted: "KDC_ERR_PATH_NOT_ACCEPTED",
            KerberosErrorCode.kdc_svc_unavailable: "KDC_ERR_SVC_UNAVAILABLE",
            KerberosErrorCode.ap_bad_integrity: "KRB_AP_ERR_BAD_INTEGRITY",
            KerberosErrorCode.ap_txt_expired: "KRB_AP_ERR_TKT_EXPIRED",
            KerberosErrorCode.ap_tkt_nyv: "KRB_AP_ERR_TKT_NYV",
            KerberosErrorCode.ap_repeat: "KRB_AP_ERR_REPEAT",
            KerberosErrorCode.ap_not_use: "KRB_AP_ERR_NOT_US",
            KerberosErrorCode.ap_badmatch: "KRB_AP_ERR_BADMATCH",
            KerberosErrorCode.ap_skew: "KRB_AP_ERR_SKEW",
            KerberosErrorCode.ap_badaddr: "KRB_AP_ERR_BADADDR",
            KerberosErrorCode.ap_badversion: "KRB_AP_ERR_BADVERSION",
            KerberosErrorCode.ap_msg_type: "KRB_AP_ERR_MSG_TYPE",
            KerberosErrorCode.ap_modified: "KRB_AP_ERR_MODIFIED",
            KerberosErrorCode.ap_badorder: "KRB_AP_ERR_BADORDER",
            KerberosErrorCode.ap_badkeyver: "KRB_AP_ERR_BADKEYVER",
            KerberosErrorCode.ap_nokey: "KRB_AP_ERR_NOKEY",
            KerberosErrorCode.ap_mut_fail: "KRB_AP_ERR_MUT_FAIL",
            KerberosErrorCode.ap_baddirection: "KRB_AP_ERR_BADDIRECTION",
            KerberosErrorCode.ap_method: "KRB_AP_ERR_METHOD",
            KerberosErrorCode.ap_badseq: "KRB_AP_ERR_BADSEQ",
            KerberosErrorCode.ap_inapp_cksum: "KRB_AP_ERR_INAPP_CKSUM",
            KerberosErrorCode.ap_path_not_accepted: "KRB_AP_PATH_NOT_ACCEPTED",
            KerberosErrorCode.response_too_big: "KRB_ERR_RESPONSE_TOO_BIG",
            KerberosErrorCode.generic: "KRB_ERR_GENERIC",
            KerberosErrorCode.field_toolong: "KRB_ERR_FIELD_TOOLONG",
            KerberosErrorCode.kdc_client_not_trusted: "KDC_ERROR_CLIENT_NOT_TRUSTED",
            KerberosErrorCode.kdc_not_trusted: "KDC_ERROR_KDC_NOT_TRUSTED",
            KerberosErrorCode.kdc_invalid_sig: "KDC_ERROR_INVALID_SIG",
            KerberosErrorCode.kdc_key_too_weak: "KDC_ERR_KEY_TOO_WEAK",
            KerberosErrorCode.kdc_certificate_mismatch: "KDC_ERR_CERTIFICATE_MISMATCH",
            KerberosErrorCode.ap_no_tgt: "KRB_AP_ERR_NO_TGT",
            KerberosErrorCode.kdc_wrong_realm: "KDC_ERR_WRONG_REALM",
            KerberosErrorCode.ap_user_to_user_required: "KRB_AP_ERR_USER_TO_USER_REQUIRED",
            KerberosErrorCode.kdc_cant_verify_certificate: "KDC_ERR_CANT_VERIFY_CERTIFICATE",
            KerberosErrorCode.kdc_invalid_certificate: "KDC_ERR_INVALID_CERTIFICATE",
            KerberosErrorCode.kdc_revoked_certificate: "KDC_ERR_REVOKED_CERTIFICATE",
            KerberosErrorCode.kdc_revocation_status_unknown: "KDC_ERR_REVOCATION_STATUS_UNKNOWN",
            KerberosErrorCode.kdc_revocation_status_unavailable: "KDC_ERR_REVOCATION_STATUS_UNAVAILABLE",
            KerberosErrorCode.kdc_client_name_mismatch: "KDC_ERR_CLIENT_NAME_MISMATCH",
            KerberosErrorCode.kdc_name_mismatch: "KDC_ERR_KDC_NAME_MISMATCH",
        }


# https://www.rfc-editor.org/rfc/rfc4120#section-5.10
class KerberosMessageType(enum.IntEnum):
    unknown = 0
    as_req = 10
    as_rep = 11
    tgs_req = 12
    tgs_rep = 13
    ap_req = 14
    ap_rep = 15
    error = 30

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosMessageType", str]:
        return {
            KerberosMessageType.unknown: "UNKNOWN",
            KerberosMessageType.as_req: "AS-REQ",
            KerberosMessageType.as_rep: "AS-REP",
            KerberosMessageType.tgs_req: "TGS-REQ",
            KerberosMessageType.tgs_rep: "TGS-REP",
            KerberosMessageType.ap_req: "AP-REQ",
            KerberosMessageType.ap_rep: "AP-REP",
            KerberosMessageType.error: "KRB-ERROR",
        }


# https://www.iana.org/assignments/kerberos-parameters/kerberos-parameters.xhtml
class KerberosPADataType(enum.IntEnum):
    tgs_req = 1
    enc_timestamp = 2
    pw_salt = 3
    reserved = 4
    enc_unix_time = 5
    sandia_secureid = 6
    sesame = 7
    osf_dce = 8
    cybersafe_secureid = 9
    afs3_salt = 10
    etype_info = 11
    sam_challenge = 12
    sam_response = 13
    pk_as_req_old = 14
    pk_as_rep_old = 15
    pk_as_req = 16
    pk_as_rep = 17
    pk_ocsp_response = 18
    etype_info2 = 19
    use_specified_kvno = 20
    svr_referral_info = 20
    sam_redirect = 21
    get_from_typed_data = 22
    td_padata = 22
    sam_etype_info = 23
    alt_princ = 24
    server_referral = 25
    sam_challenge2 = 30
    sam_response2 = 31
    extra_tgt = 41
    td_pkinit_cms_certificates = 101
    td_krb_principal = 102
    td_krb_realm = 103
    td_trusted_certifiers = 104
    td_certificate_index = 105
    td_app_defined_error = 106
    td_req_nonce = 107
    td_req_seq = 108
    td_dh_parameters = 109
    td_cms_digest_algorithms = 111
    td_cert_digest_algorithms = 112
    pac_request = 128
    for_user = 128
    for_x509_user = 130
    for_check_dups = 131
    as_checksum = 132
    fx_cookie = 133
    authentication_set = 134
    auth_set_selected = 165
    fx_fast = 136
    fx_error = 137
    encrypted_challenge = 138
    otp_challenge = 141
    otp_request = 142
    otp_confirm = 143
    otp_pin_change = 144
    epak_as_req = 145
    epak_as_rep = 146
    pkinit_kx = 147
    pku2u_name = 148
    enc_pa_rep = 149
    as_freshness = 150
    spake = 151
    kerb_key_list_req = 161
    kerb_key_list_rep = 162
    supported_etypes = 165
    extended_error = 166
    pac_options = 167

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosPADataType", str]:
        return {
            KerberosPADataType.tgs_req: "PA-TGS-REQ",
            KerberosPADataType.enc_timestamp: "PA-ENC-TIMESTAMP",
            KerberosPADataType.pw_salt: "PA-PW-SALT",
            KerberosPADataType.reserved: "reserved",
            KerberosPADataType.enc_unix_time: "PA-ENC-UNIX-TIME",
            KerberosPADataType.sandia_secureid: "PA-SANDIA-SECUREID",
            KerberosPADataType.sesame: "PA-SESAME",
            KerberosPADataType.osf_dce: "PA-OSF-DCE",
            KerberosPADataType.cybersafe_secureid: "PA-CYBERSAFE-SECUREID",
            KerberosPADataType.afs3_salt: "PA-AFS3-SALT",
            KerberosPADataType.etype_info: "PA-ETYPE-INFO",
            KerberosPADataType.sam_challenge: "PA-SAM-CHALLENGE",
            KerberosPADataType.sam_response: "PA-SAM-RESPONSE",
            KerberosPADataType.pk_as_req_old: "PA-PK-AS-REQ_OLD",
            KerberosPADataType.pk_as_rep_old: "PA-PK-AS-REP_OLD",
            KerberosPADataType.pk_as_req: "PA-PK-AS-REQ",
            KerberosPADataType.pk_as_rep: "PA-PK-AS-REP",
            KerberosPADataType.pk_ocsp_response: "PA-PK-OCSP-RESPONSE",
            KerberosPADataType.etype_info2: "PA-ETYPE-INFO2",
            KerberosPADataType.use_specified_kvno: "PA-USE-SPECIFIED-KVNO or PA-SVR-REFERRAL-INFO",
            KerberosPADataType.sam_redirect: "PA-SAM-REDIRECT",
            KerberosPADataType.get_from_typed_data: "PA-GET-FROM-TYPED-DATA",
            KerberosPADataType.td_padata: "TD-PADATA",
            KerberosPADataType.sam_etype_info: "PA-SAM-ETYPE-INFO",
            KerberosPADataType.alt_princ: "PA-ALT-PRINC",
            KerberosPADataType.server_referral: "PA-SERVER-REFERRAL",
            KerberosPADataType.sam_challenge2: "PA-SAM-CHALLENGE2",
            KerberosPADataType.sam_response2: "PA-SAM-RESPONSE2",
            KerberosPADataType.extra_tgt: "PA-EXTRA-TGT",
            KerberosPADataType.td_pkinit_cms_certificates: "TD-PKINIT-CMS-CERTIFICATES",
            KerberosPADataType.td_krb_principal: "TD-KRB-PRINCIPAL",
            KerberosPADataType.td_krb_realm: "TD-KRB-REALM",
            KerberosPADataType.td_trusted_certifiers: "TD-TRUSTED-CERTIFIERS",
            KerberosPADataType.td_certificate_index: "TD-CERTIFICATE-INDEX",
            KerberosPADataType.td_app_defined_error: "TD-APP-DEFINED-ERROR",
            KerberosPADataType.td_req_nonce: "TD-REQ-NONCE",
            KerberosPADataType.td_req_seq: "TD-REQ-SEQ",
            KerberosPADataType.td_dh_parameters: "TD_DH_PARAMETERS",
            KerberosPADataType.td_cms_digest_algorithms: "TD-CMS-DIGEST-ALGORITHMS",
            KerberosPADataType.td_cert_digest_algorithms: "TD-CERT-DIGEST-ALGORITHMS",
            KerberosPADataType.pac_request: "PA-PAC-REQUEST",
            KerberosPADataType.for_user: "PA-FOR_USER",
            KerberosPADataType.for_x509_user: "PA-FOR-X509-USER",
            KerberosPADataType.for_check_dups: "PA-FOR-CHECK_DUPS",
            KerberosPADataType.as_checksum: "PA-AS-CHECKSUM",
            KerberosPADataType.fx_cookie: "PA-FX-COOKIE",
            KerberosPADataType.authentication_set: "PA-AUTHENTICATION-SET",
            KerberosPADataType.auth_set_selected: "PA-AUTH-SET-SELECTED",
            KerberosPADataType.fx_fast: "PA-FX-FAST",
            KerberosPADataType.fx_error: "PA-FX-ERROR",
            KerberosPADataType.encrypted_challenge: "PA-ENCRYPTED-CHALLENGE",
            KerberosPADataType.otp_challenge: "PA-OTP-CHALLENGE",
            KerberosPADataType.otp_request: "PA-OTP-REQUEST",
            KerberosPADataType.otp_confirm: "PA-OTP-CONFIRM",
            KerberosPADataType.otp_pin_change: "PA-OTP-PIN-CHANGE",
            KerberosPADataType.epak_as_req: "PA-EPAK-AS-REQ",
            KerberosPADataType.epak_as_rep: "PA-EPAK-AS-REP",
            KerberosPADataType.pkinit_kx: "PA_PKINIT_KX",
            KerberosPADataType.pku2u_name: "PA_PKU2U_NAME",
            KerberosPADataType.enc_pa_rep: "PA-REQ-ENC-PA-REP",
            KerberosPADataType.as_freshness: "PA_AS_FRESHNESS",
            KerberosPADataType.spake: "PA-SPAKE",
            KerberosPADataType.kerb_key_list_req: "KERB-KEY-LIST-REQ",
            KerberosPADataType.kerb_key_list_rep: "KERB-KEY-LIST-REP",
            KerberosPADataType.supported_etypes: "PA-SUPPORTED-ETYPES",
            KerberosPADataType.extended_error: "PA-EXTENDED_ERROR",
            KerberosPADataType.pac_options: "PA-PAC-OPTIONS",
        }


# https://www.rfc-editor.org/rfc/rfc4120#section-6.2
class KerberosPrincipalNameType(enum.IntEnum):
    unknown = 0
    principal = 1
    srv_inst = 2
    srv_hst = 3
    srv_xhst = 4
    uid = 5
    x500_principal = 6
    smtp_name = 7
    enterprise = 10

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosPrincipalNameType", str]:
        return {
            KerberosPrincipalNameType.unknown: "NT-UNKNOWN",
            KerberosPrincipalNameType.principal: "NT-PRINCIPAL",
            KerberosPrincipalNameType.srv_inst: "NT-SRV-INST",
            KerberosPrincipalNameType.srv_hst: "NT-SRV-HST",
            KerberosPrincipalNameType.srv_xhst: "NT-SRV-XHST",
            KerberosPrincipalNameType.uid: "NT-UID",
            KerberosPrincipalNameType.x500_principal: "NT-X500-PRINCIPAL",
            KerberosPrincipalNameType.smtp_name: "NT-SMTP-NAME",
            KerberosPrincipalNameType.enterprise: "NT-ENTERPRISE",
        }


# https://www.rfc-editor.org/rfc/rfc4120#section-7.5.3
class KerberosHostAddressType(enum.IntEnum):
    ipv4 = 2
    directional = 3
    chaos_net = 5
    xns = 6
    iso = 7
    decnet_phase_iv = 12
    apple_talk_ddp = 16
    netbios = 20
    ipv6 = 26

    @classmethod
    def native_labels(cls) -> typing.Dict["KerberosHostAddressType", str]:
        return {
            KerberosHostAddressType.ipv4: "IPv4",
            KerberosHostAddressType.directional: "Directional",
            KerberosHostAddressType.chaos_net: "ChaosNet",
            KerberosHostAddressType.xns: "XNS",
            KerberosHostAddressType.iso: "ISO",
            KerberosHostAddressType.decnet_phase_iv: "DECNET Phase IV",
            KerberosHostAddressType.apple_talk_ddp: "AppleTalk DDP",
            KerberosHostAddressType.netbios: "NetBios",
            KerberosHostAddressType.ipv6: "IPv6",
        }


class ParseType(enum.IntEnum):
    default = 0
    enum = 1
    flags = 2
    datetime = 3
    text = 4
    bytes = 5
    principal_name = 6
    host_address = 7
    token = 8


class _KerberosMsgType(type):
    __registry: typing.Dict[int, typing.Dict[int, "_KerberosMsgType"]] = {}

    def __init__(
        cls,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        pvno = getattr(cls, "PVNO", 0)

        if pvno not in cls.__registry:
            cls.__registry[pvno] = {}

        msg_type = getattr(cls, "MESSAGE_TYPE", None)
        if msg_type is not None:
            cls.__registry[pvno][msg_type] = cls

    def __call__(
        cls,
        sequence: typing.Dict[int, ASN1Value],
    ) -> "_KerberosMsgType":
        # The KrbAsReq msg starts at 1 not 0, so do a check for that.
        pvno_idx = 0
        if 0 not in sequence:
            pvno_idx = 1

        pvno = unpack_asn1_integer(sequence[pvno_idx])
        message_type = unpack_asn1_integer(sequence[pvno_idx + 1])
        new_cls = cls.__registry[pvno].get(message_type, cls)
        return super(_KerberosMsgType, new_cls).__call__(sequence)


class KerberosV5Msg(metaclass=_KerberosMsgType):

    MESSAGE_TYPE = KerberosMessageType.unknown
    PVNO = 5

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        # This is only used if decoding an unknown Kerberos message type.
        self.sequence = sequence

    @staticmethod
    def unpack(value: typing.Union[ASN1Value, bytes]) -> "KerberosV5Msg":
        msg_sequence = unpack_asn1_tagged_sequence(value)
        return KerberosV5Msg(msg_sequence)


class KrbAsReq(KerberosV5Msg):
    """The KRB_AS_REQ message.

    The KRB_AS_REQ message is used when the client wishes to retrieve a the initial ticket for a service. The
    KRB_TGS_REQ message is identical except for the tag and msg-type is used when retrieving additional tickets for a
    service.

    The ASN.1 definition for the KDC-REQ structure is defined in `RFC 4120 5.4.1`_::

        KDC-REQ         ::= SEQUENCE {
            -- NOTE: first tag is [1], not [0]
            pvno            [1] INTEGER (5) ,
            msg-type        [2] INTEGER (10 -- AS -- | 12 -- TGS --),
            padata          [3] SEQUENCE OF PA-DATA OPTIONAL
                                -- NOTE: not empty --,
            req-body        [4] KDC-REQ-BODY
        }

    Args:
        sequence: The ASN.1 sequence value as a dict to unpack.

    Attributes:
        padata (PAData): The pre-authentication data.
        req_body (KdcReqBody): The body of the request.

    .. _RFC 4120 5.4.1:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.4.1
    """

    MESSAGE_TYPE = KerberosMessageType.as_req

    PARSE_MAP = [
        ("pvno", "PVNO", ParseType.default),
        ("msg-type", "MESSAGE_TYPE", ParseType.enum),
        ("padata", "padata", ParseType.token),
        ("req-body", "req_body", ParseType.token),
    ]

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        def unpack_padata(value: typing.Union[ASN1Value, bytes]) -> typing.List:
            return [PAData.unpack(p) for p in unpack_asn1_sequence(value)]

        self.padata = get_sequence_value(sequence, 3, "KDC-REQ", "pa-data", unpack_padata)
        self.req_body = get_sequence_value(sequence, 4, "KDC-REQ", "req-body", KdcReqBody.unpack)


class KrbAsRep(KerberosV5Msg):
    """The KRB_AS_REP message.

    The KRB_AS_REP message is used for a reply from the KDC to a KRB_AS_REQ message. The KRB_TGS_REP message is
    identical except for the tag and msg-type.

    The ASN.1 definition for the KDC-REP structure is defined in `RFC 4120 5.4.2`_::

        KDC-REP         ::= SEQUENCE {
            pvno            [0] INTEGER (5),
            msg-type        [1] INTEGER (11 -- AS -- | 13 -- TGS --),
            padata          [2] SEQUENCE OF PA-DATA OPTIONAL
                                -- NOTE: not empty --,
            crealm          [3] Realm,
            cname           [4] PrincipalName,
            ticket          [5] Ticket,
            enc-part        [6] EncryptedData
                                -- EncASRepPart or EncTGSRepPart,
                                -- as appropriate
        }

    Args:
        sequence: The ASN.1 sequence value as a dict to unpack.

    Attributes:
        padata (PAData): The pre-authentication data.
        crealm (bytes): The client realm.
        cname (PrincipalName): The client principal name.
        ticket (Ticket): The newly issued ticket.
        enc_part (EncryptedData): The encrypted part of the message.

    .. _RFC 4120 5.4.2:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.4.2
    """

    MESSAGE_TYPE = KerberosMessageType.as_rep

    PARSE_MAP = [
        ("pvno", "PVNO", ParseType.default),
        ("msg-type", "MESSAGE_TYPE", ParseType.enum),
        ("padata", "padata", ParseType.token),
        ("crealm", "crealm", ParseType.text),
        ("cname", "cname", ParseType.principal_name),
        ("ticket", "ticket", ParseType.token),
        ("enc-part", "enc_part", ParseType.token),
    ]

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        def unpack_padata(value: typing.Union[ASN1Value, bytes]) -> typing.List:
            return [PAData.unpack(p) for p in unpack_asn1_sequence(value)]

        self.padata = get_sequence_value(sequence, 2, "KDC-REP", "pa-data", unpack_padata)
        self.crealm = get_sequence_value(sequence, 3, "KDC-REP", "crealm", unpack_asn1_general_string)
        self.cname = get_sequence_value(sequence, 4, "KDC-REP", "cname", unpack_principal_name)
        self.ticket = get_sequence_value(sequence, 5, "KDC-REP", "ticket", Ticket.unpack)
        self.enc_part = get_sequence_value(sequence, 6, "KDC-REP", "enc-part", EncryptedData.unpack)


class KrbTgsReq(KrbAsReq):
    """The KRB_TGS_REQ is the same as KRB_AS_REQ but with a different MESSAGE_TYPE."""

    MESSAGE_TYPE = KerberosMessageType.tgs_req


class KrbTgsRep(KrbAsRep):
    """The KRB_TGS_REP is the same as KRB_AS_REP but with a different MESSAGE_TYPE."""

    MESSAGE_TYPE = KerberosMessageType.tgs_rep


class KrbApReq(KerberosV5Msg):
    """The KRB_AP_REQ message.

    The KRB_AP_REQ message contains is used to authenticate the initiator to an acceptor.

    The ASN.1 definition for the KRB_AP_REQ structure is defined in `RFC 4120 5.5.1`_::

        AP-REQ          ::= [APPLICATION 14] SEQUENCE {
            pvno            [0] INTEGER (5),
            msg-type        [1] INTEGER (14),
            ap-options      [2] APOptions,
            ticket          [3] Ticket,
            authenticator   [4] EncryptedData -- Authenticator
        }

    Args:
        sequence: The ASN.1 sequence value as a dict to unpack.

    Attributes:
        ap_options (KerberosAPOptions): Options related to the AP request.
        ticket (Ticket): The ticket authenticating the client to the server.
        authenticator (EncryptedData): The encrypted authenticator.

    .. _RFC 4120 5.5.1:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.5.1
    """

    MESSAGE_TYPE = KerberosMessageType.ap_req

    PARSE_MAP = [
        ("pvno", "PVNO", ParseType.default),
        ("msg-type", "MESSAGE_TYPE", ParseType.enum),
        ("ap-options", "ap_options", ParseType.flags),
        ("ticket", "ticket", ParseType.token),
        ("authenticator", "authenticator", ParseType.token),
    ]

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        raw_ap_options = get_sequence_value(sequence, 2, "AP-REQ", "ap-options", unpack_asn1_bit_string)
        ap_options = KerberosAPOptions(struct.unpack("<I", raw_ap_options)[0])

        self.ap_options = ap_options
        self.ticket = get_sequence_value(sequence, 3, "AP-REQ", "ticket", Ticket.unpack)
        self.authenticator = get_sequence_value(sequence, 4, "AP-REQ", "authenticator", EncryptedData.unpack)


class KrbApRep(KerberosV5Msg):
    """The KRB_AP_REP message.

    The KRB_AP_REP is a response to an application request `KRB_AP_REQ`.

    The ASN.1 definition for the KRB_AP_REP structure is defined in `RFC 4120 5.5.2`_::

        AP-REP          ::= [APPLICATION 15] SEQUENCE {
            pvno            [0] INTEGER (5),
            msg-type        [1] INTEGER (15),
            enc-part        [2] EncryptedData -- EncAPRepPart
        }

    Args:
        sequence: The ASN.1 sequence value as a dict to unpack.

    Attributes:
        enc_part (EncryptedData): The encrypted authenticator.

    .. _RFC 4120 5.5.2:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.5.2
    """

    MESSAGE_TYPE = KerberosMessageType.ap_rep

    PARSE_MAP = [
        ("pvno", "PVNO", ParseType.default),
        ("msg-type", "MESSAGE_TYPE", ParseType.enum),
        ("enc-part", "enc_part", ParseType.token),
    ]

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        self.enc_part = get_sequence_value(sequence, 2, "AP-REP", "enc-part", EncryptedData.unpack)


class KrbError(KerberosV5Msg):
    """The KRB_ERROR message.

    The KRB_ERROR is a message sent in the occurrence of an error.

    The ASN.1 definition for the KRB_ERROR structure is defined in `RFC 4120 5.9.1`_::

        KRB-ERROR       ::= [APPLICATION 30] SEQUENCE {
            pvno            [0] INTEGER (5),
            msg-type        [1] INTEGER (30),
            ctime           [2] KerberosTime OPTIONAL,
            cusec           [3] Microseconds OPTIONAL,
            stime           [4] KerberosTime,
            susec           [5] Microseconds,
            error-code      [6] Int32,
            crealm          [7] Realm OPTIONAL,
            cname           [8] PrincipalName OPTIONAL,
            realm           [9] Realm -- service realm --,
            sname           [10] PrincipalName -- service name --,
            e-text          [11] KerberosString OPTIONAL,
            e-data          [12] OCTET STRING OPTIONAL
        }

    Args:
        sequence: The ASN.1 sequence value as a dict to unpack.

    Attributes:
        ctime (datetime.datetime): The current time on the client's host.
        cusec (int): The microsecond part of the client's timestamp.
        stime (datetime.datetime): The current time of the server.
        susec (int): The microsecond part of the server's timestamp.
        error_code (KerberosErrorCode): THe error code returned by the kerberos when a request fails.
        crealm (bytes): The realm that issues a ticket.
        cname (PrincipalName): The principal name in the ticket.
        realm (bytes): The service realm.
        sname (PrincipalName): The service name.
        e_text (bytes): Additional text to explain the error code.
        e_data (bytes): Additional data about the error.

    .. _RFC 4120 5.9.1:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.9.1
    """

    MESSAGE_TYPE = KerberosMessageType.error

    PARSE_MAP = [
        ("pvno", "PVNO", ParseType.default),
        ("msg-type", "MESSAGE_TYPE", ParseType.enum),
        ("ctime", "ctime", ParseType.datetime),
        ("cusec", "cusec", ParseType.default),
        ("stime", "stime", ParseType.datetime),
        ("susec", "susec", ParseType.default),
        ("error-code", "error_code", ParseType.enum),
        ("crealm", "crealm", ParseType.text),
        ("cname", "cname", ParseType.principal_name),
        ("realm", "realm", ParseType.text),
        ("sname", "sname", ParseType.principal_name),
        ("e-text", "e_text", ParseType.text),
        ("e-data", "e_data", ParseType.bytes),
    ]

    def __init__(self, sequence: typing.Dict[int, ASN1Value]) -> None:
        self.ctime = get_sequence_value(sequence, 2, "KRB-ERROR", "ctime", unpack_asn1_generalized_time)
        self.cusec = get_sequence_value(sequence, 3, "KRB-ERROR", "cusec", unpack_asn1_integer)
        self.stime = get_sequence_value(sequence, 4, "KRB-ERROR", "stime", unpack_asn1_generalized_time)
        self.susec = get_sequence_value(sequence, 5, "KRB-ERROR", "susec", unpack_asn1_integer)
        self.error_code = KerberosErrorCode(
            get_sequence_value(sequence, 6, "KRB-ERROR", "error-code", unpack_asn1_integer)
        )
        self.crealm = get_sequence_value(sequence, 7, "KRB-ERROR", "crealm", unpack_asn1_general_string)
        self.cname = get_sequence_value(sequence, 8, "KRB-ERROR", "cname", unpack_principal_name)
        self.realm = get_sequence_value(sequence, 9, "KRB-ERROR", "realm", unpack_asn1_general_string)
        self.sname = get_sequence_value(sequence, 10, "KRB-ERROR", "realm", unpack_principal_name)
        self.e_text = get_sequence_value(sequence, 11, "KRB-ERROR", "e-text", unpack_asn1_general_string)
        self.e_data = get_sequence_value(sequence, 12, "KRB-ERROR", "e-data", unpack_asn1_octet_string)


class PAData:
    """Kerberos PA-DATA.

    The ASN.1 definition for the PA-DATA structure is defined in `RFC 4120 5.2.7`_::

        PA-DATA         ::= SEQUENCE {
            -- NOTE: first tag is [1], not [0]
            padata-type     [1] Int32,
            padata-value    [2] OCTET STRING -- might be encoded AP-REQ
        }

    Args:
        data_type: Indicates the type of data the value represents.
        value: The PAData value, usually the DER encoding of another message.

    Attributes:
        data_type (Union[int, KerberosPADataType]): See args.
        b_value (bytes): The raw bytes of padata-value, use `value` to get a structured object of these bytes if
            available.

    .. RFC 4120 5.2.7:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.2.7
    """

    PARSE_MAP = [
        ("padata-type", "data_type", ParseType.enum),
        ("padata-value", "value", ParseType.token),
    ]

    def __init__(self, data_type: typing.Union[int, KerberosPADataType], value: bytes) -> None:
        self.data_type = data_type
        self.b_value = value

    @property
    def value(self) -> typing.Any:
        if self.data_type == KerberosPADataType.tgs_req:
            # Special edge case for this PA type due to how the messages require the data in unpack
            return KrbTgsReq.unpack(unpack_asn1(unpack_asn1(self.b_value)[0].b_data)[0])

        data_type_map = {
            int(KerberosPADataType.enc_timestamp): (EncryptedData.unpack, False),
            int(KerberosPADataType.etype_info2): (PAETypeInfo2.unpack, True),
        }

        if self.data_type in data_type_map:
            unpack_func, is_sequence = data_type_map[int(self.data_type)]
            b_value = unpack_asn1(self.b_value)[0]
            if is_sequence:
                # Is a SEQUENCE OF (list of entries).
                return [unpack_func(v) for v in unpack_asn1_sequence(b_value)]

            else:
                return unpack_func(b_value.b_data)

        else:
            return self.b_value

    @staticmethod
    def unpack(value: typing.Union[ASN1Value, bytes]) -> "PAData":
        sequence = unpack_asn1_tagged_sequence(value)

        def unpack_data_type(value: typing.Union[ASN1Value, bytes]) -> typing.Union[KerberosPADataType, int]:
            int_val = unpack_asn1_integer(value)

            try:
                return KerberosPADataType(int_val)
            except ValueError:
                return int_val

        data_type = get_sequence_value(sequence, 1, "PA-DATA", "padata-type", unpack_data_type)
        pa_value = get_sequence_value(sequence, 2, "PA-DATA", "padata-value", unpack_asn1_octet_string)

        return PAData(data_type, pa_value)


class PAETypeInfo2:
    """Kerberos PA-ETYPE-INFO2 container.

    The ASN.1 definition for the PA-ETYPE-INFO2 structure is defined in `RFC 4120 5.2.7.5`_::

        ETYPE-INFO2-ENTRY       ::= SEQUENCE {
            etype           [0] Int32,
            salt            [1] KerberosString OPTIONAL,
            s2kparams       [2] OCTET STRING OPTIONAL
        }

    Args:
        etype: The etype that defines the cipher used.
        salt: The used in the cipher associated with the cryptosystem.
        s2kparams: Extra params to be interpreted by the cipher associated with the cryptosystem.

    Attributes:
        etype (KerberosEncryptionType): See args.
        salt (Optional[bytes]): See args.
        s2kparams (Optional[bytes]): See args.

    .. RFC 4120 5.2.7.5:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.2.7.5
    """

    PARSE_MAP = [
        ("etype", "etype", ParseType.enum),
        ("salt", "salt", ParseType.bytes),
        ("s2kparams", "s2kparams", ParseType.bytes),
    ]

    def __init__(
        self,
        etype: KerberosEncryptionType,
        salt: typing.Optional[bytes],
        s2kparams: typing.Optional[bytes],
    ) -> None:
        self.etype = etype
        self.salt = salt
        self.s2kparams = s2kparams

    @staticmethod
    def unpack(value: typing.Union[ASN1Value, bytes]) -> "PAETypeInfo2":
        sequence = unpack_asn1_tagged_sequence(value)

        etype = KerberosEncryptionType(get_sequence_value(sequence, 0, "PA-ETYPE-INFO2", "etype", unpack_asn1_integer))
        salt = get_sequence_value(sequence, 1, "ETYPE-INFO2-ENTRY", "salt", unpack_asn1_general_string)
        s2kparams = get_sequence_value(sequence, 2, "ETYPE-INFO2-ENTRY", "s2kparams", unpack_asn1_octet_string)

        return PAETypeInfo2(etype, salt, s2kparams)


class EncryptedData:
    """Kerberos EncryptedData container.

    The ASN.1 definition for the EncryptedData structure is defined in `RFC 4120 5.2.9`_::

        EncryptedData   ::= SEQUENCE {
            etype   [0] Int32 -- EncryptionType --,
            kvno    [1] UInt32 OPTIONAL,
            cipher  [2] OCTET STRING -- ciphertext
        }

    Args:
        etype: The encryption algorithm that was used to encipher the cipher.
        kvno: The version number of the key under which data is encrypted. It is only present in messages encrypted
            under long lasting keys.
        cipher: The enciphered text.

    Attributes:
        etype (KerberosEncryptionType): See args.
        kvno (Optional[int]): See args.
        cipher (bytes): See args.

    .. RFC 4120 5.2.9:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.2.9
    """

    PARSE_MAP = [
        ("etype", "etype", ParseType.enum),
        ("kvno", "kvno", ParseType.default),
        ("cipher", "cipher", ParseType.bytes),
    ]

    def __init__(self, etype: KerberosEncryptionType, kvno: typing.Optional[int], cipher: bytes) -> None:
        self.etype = etype
        self.kvno = kvno
        self.cipher = cipher

    @staticmethod
    def unpack(value: typing.Union[bytes, ASN1Value]) -> "EncryptedData":
        sequence = unpack_asn1_tagged_sequence(value)

        etype = KerberosEncryptionType(get_sequence_value(sequence, 0, "EncryptedData", "etype", unpack_asn1_integer))
        kvno = get_sequence_value(sequence, 1, "EncryptedData", "kvno", unpack_asn1_integer)
        cipher = get_sequence_value(sequence, 2, "EncryptedData", "cipher", unpack_asn1_octet_string)

        return EncryptedData(etype, kvno, cipher)


class Ticket:
    """Kerberos Ticket.

    The ASN.1 definition for the Ticket structure is defined in `RFC 4120 5.3`_::

        Ticket          ::= [APPLICATION 1] SEQUENCE {
            tkt-vno         [0] INTEGER (5),
            realm           [1] Realm,
            sname           [2] PrincipalName,
            enc-part        [3] EncryptedData -- EncTicketPart
        }

    Args:
        tkt_vno: The version number for the ticket format.
        realm: The realm that issued a ticket.
        sname: All the name components of the server's identity.
        enc_part: The encrypted part of the ticket, it is encrypted in the key shared by Kerberos and the end server.

    Attributes:
        tkt_vno (int): See args.
        realm (bytes): See args.
        sname (List[PrincipalName]): See args.
        enc_part (EncryptedData): See args.

    .. _RFC 4120 5.3:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.3
    """

    PARSE_MAP = [
        ("tkt-vno", "tkt_vno", ParseType.default),
        ("realm", "realm", ParseType.text),
        ("sname", "sname", ParseType.principal_name),
        ("enc-part", "enc_part", ParseType.token),
    ]

    def __init__(
        self,
        tkt_vno: int,
        realm: bytes,
        sname: typing.List[PrincipalName],
        enc_part: EncryptedData,
    ) -> None:
        self.tkt_vno = tkt_vno
        self.realm = realm
        self.sname = sname
        self.enc_part = enc_part

    @staticmethod
    def unpack(value: typing.Union[ASN1Value, bytes]) -> "Ticket":
        b_data = extract_asn1_tlv(value, TagClass.application, 1)
        sequence = unpack_asn1_tagged_sequence(unpack_asn1(b_data)[0])

        tkt_vno = get_sequence_value(sequence, 0, "Ticket", "tkt-vno", unpack_asn1_integer)
        realm = get_sequence_value(sequence, 1, "Ticket", "realm", unpack_asn1_general_string)
        sname = get_sequence_value(sequence, 2, "Ticket", "sname", unpack_principal_name)
        enc_part = get_sequence_value(sequence, 3, "Ticket", "enc-part", EncryptedData.unpack)

        return Ticket(tkt_vno, realm, sname, enc_part)


class KdcReqBody:
    """The KRB_AS_REQ message.

    The KRB_AS_REQ message is used when the client wishes to retrieve a the initial ticket for a service. The
    KRB_TGS_REQ message is identical except for the tag and msg-type is used when retrieving additional tickets for a
    service.

    The ASN.1 definition for the KDC-REQ structure is defined in `RFC 4120 5.4.1`_::

        KDC-REQ-BODY    ::= SEQUENCE {
            kdc-options             [0] KDCOptions,
            cname                   [1] PrincipalName OPTIONAL
                                        -- Used only in AS-REQ --,
            realm                   [2] Realm
                                        -- Server's realm
                                        -- Also client's in AS-REQ --,
            sname                   [3] PrincipalName OPTIONAL,
            from                    [4] KerberosTime OPTIONAL,
            till                    [5] KerberosTime,
            rtime                   [6] KerberosTime OPTIONAL,
            nonce                   [7] UInt32,
            etype                   [8] SEQUENCE OF Int32 -- EncryptionType
                                        -- in preference order --,
            addresses               [9] HostAddresses OPTIONAL,
            enc-authorization-data  [10] EncryptedData OPTIONAL
                                        -- AuthorizationData --,
            additional-tickets      [11] SEQUENCE OF Ticket OPTIONAL
                                        -- NOTE: not empty
        }

    Args:
        kdc_options: Flags desired by the client and other behaviour desired.
        cname: The client name.
        realm: The realm part of the server's principal.
        sname: The service name.
        postdated_from: When the requested ticket is to be posted from.
        postdated_till: The expiration date requested by the client.
        rtime: The requested renew-till time.
        nonce: Random number generated by the client.
        etype: The desired encryption algorithm to be used in priority order.
        addresses: Addresses from which the requested ticket is to be valid.
        enc_authorization_data: Encrypted authorization data.
        additional_tickets: Additional tickets to be optionally included in a request.

    Attributes:
        kdc_options (int): See args.
        cname (Optional[PrincipalName]): See args.
        realm (bytes): See args.
        sname (PrincipalName): See args.
        postdated_from (Optional[datetime.datetime]): See args.
        postdated_till (datetime.datetime): See args.
        rtime (Optional[datetime.datetime]): See args.
        nonce (int): See args.
        etype (List[KerberosEncryptionType]): See args.
        addresses (Optional[List[HostAddress]]): See args.
        enc_authorization_data (Optional[EncryptedData]): See args.
        additional_tickets (Optional[Ticket]): See args.

    .. _RFC 4120 5.4.1:
        https://www.rfc-editor.org/rfc/rfc4120#section-5.4.1
    """

    PARSE_MAP = [
        ("kdc-options", "kdc_options", (ParseType.flags, KerberosKDCOptions)),
        ("cname", "cname", ParseType.principal_name),
        ("realm", "realm", ParseType.text),
        ("sname", "sname", ParseType.principal_name),
        ("from", "postdated_from", ParseType.datetime),
        ("till", "postdated_till", ParseType.datetime),
        ("rtime", "rtime", ParseType.datetime),
        ("nonce", "nonce", ParseType.default),
        ("etype", "etype", ParseType.enum),
        ("addresses", "addresses", ParseType.host_address),
        ("enc-authorization-data", "enc_authorization_data", ParseType.token),
        ("additional-tickets", "additional_tickets", ParseType.token),
    ]

    def __init__(
        self,
        kdc_options: int,
        cname: typing.Optional[PrincipalName],
        realm: bytes,
        sname: PrincipalName,
        postdated_from: typing.Optional[datetime.datetime],
        postdated_till: datetime.datetime,
        rtime: typing.Optional[datetime.datetime],
        nonce: int,
        etype: typing.List[KerberosEncryptionType],
        addresses: typing.Optional[typing.List[HostAddress]],
        enc_authorization_data: typing.Optional[EncryptedData],
        additional_tickets: typing.Optional[typing.List[Ticket]],
    ) -> None:
        self.kdc_options = kdc_options
        self.cname = cname
        self.realm = realm
        self.sname = sname
        self.postdated_from = postdated_from
        self.postdated_till = postdated_till
        self.rtime = rtime
        self.nonce = nonce
        self.etype = etype
        self.addresses = addresses
        self.enc_authorization_data = enc_authorization_data
        self.additional_tickets = additional_tickets

    @staticmethod
    def unpack(value: typing.Union[ASN1Value, bytes]) -> "KdcReqBody":
        sequence = unpack_asn1_tagged_sequence(value)

        def unpack_kdc_options(value: typing.Union[ASN1Value, bytes]) -> int:
            b_data = unpack_asn1_bit_string(value)
            return struct.unpack(">I", b_data)[0]

        def unpack_etype(value: typing.Union[ASN1Value, bytes]) -> typing.List[KerberosEncryptionType]:
            return [KerberosEncryptionType(unpack_asn1_integer(e)) for e in unpack_asn1_sequence(value)]

        def unpack_addresses(value: typing.Union[ASN1Value, bytes]) -> typing.List[HostAddress]:
            return [unpack_hostname(h) for h in unpack_asn1_sequence(value)]

        def unpack_ticket(value: typing.Union[ASN1Value, bytes]) -> typing.List[Ticket]:
            return [Ticket.unpack(t) for t in unpack_asn1_sequence(value)]

        kdc_options = get_sequence_value(sequence, 0, "KDC-REQ-BODY", "kdc-options", unpack_kdc_options)
        cname = get_sequence_value(sequence, 1, "KDC-REQ-BODY", "cname", unpack_principal_name)
        realm = get_sequence_value(sequence, 2, "KDC-REQ-BODY", "realm", unpack_asn1_general_string)
        sname = get_sequence_value(sequence, 3, "KDC-REQ-BODY", "sname", unpack_principal_name)
        postdated_from = get_sequence_value(sequence, 4, "KDC-REQ-BODY", "from", unpack_asn1_generalized_time)
        postdated_till = get_sequence_value(sequence, 5, "KDC-REQ-BODY", "till", unpack_asn1_generalized_time)
        rtime = get_sequence_value(sequence, 6, "KDC-REQ-BODY", "rtime", unpack_asn1_generalized_time)
        nonce = get_sequence_value(sequence, 7, "KDC-REQ-BODY", "nonce", unpack_asn1_integer)
        etype = get_sequence_value(sequence, 8, "KDC-REQ-BODY", "etype", unpack_etype)
        addresses = get_sequence_value(sequence, 9, "KDC-REQ-BODY", "addresses", unpack_addresses)
        enc_auth_data = get_sequence_value(sequence, 10, "KDC-REQ-BODY", "enc-authorization-data", EncryptedData.unpack)
        additional_tickets = get_sequence_value(sequence, 11, "KDC-REQ-BODY", "additional-tickets", unpack_ticket)

        return KdcReqBody(
            kdc_options,
            cname,
            realm,
            sname,
            postdated_from,
            postdated_till,
            rtime,
            nonce,
            etype,
            addresses,
            enc_auth_data,
            additional_tickets,
        )
