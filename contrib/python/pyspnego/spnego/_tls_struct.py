# Copyright: (c) 2022, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import typing


def _add_missing_enum_member(
    cls: typing.Type[enum.IntEnum],
    value: object,
    label: str,
) -> typing.Optional[enum.Enum]:
    if not isinstance(value, int):
        return None

    new_member = int.__new__(cls)
    new_member._name_ = label.format(value)
    new_member._value_ = value
    return cls._value2member_map_.setdefault(value, new_member)


class TlsProtocolVersion(enum.IntEnum):
    tls1_0 = 0x0301
    tls1_1 = 0x0302
    tls1_2 = 0x0303
    tls1_3 = 0x0304

    @classmethod
    def native_labels(cls) -> typing.Dict["TlsProtocolVersion", str]:
        return {
            TlsProtocolVersion.tls1_0: "TLS 1.0 (0x0301)",
            TlsProtocolVersion.tls1_1: "TLS 1.1 (0x0302)",
            TlsProtocolVersion.tls1_2: "TLS 1.2 (0x0303)",
            TlsProtocolVersion.tls1_3: "TLS 1.3 (0x0304)",
        }

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown TLS Protocol Version 0x{0:04X}")


class TlsContentType(enum.IntEnum):
    invalid = 0
    change_cipher_spec = 20
    alert = 21
    handshake = 22
    application_data = 23

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown TLS Content Type 0x{0:02X}")


class TlsHandshakeMessageType(enum.IntEnum):
    hello_request = 0
    client_hello = 1
    server_hello = 2
    hello_verify_request = 3
    new_session_ticket = 4
    end_of_early_data = 5
    hello_retry_request = 6
    encrypted_extensions = 8
    request_connection_id = 9
    new_connection_id = 10
    certificate = 11
    server_key_exchange = 12
    certificate_request = 13
    server_hello_done = 14
    certificate_verify = 15
    client_key_exchange = 16
    client_certificate_request = 17
    finished = 20
    certificate_url = 21
    certificate_status = 22
    supplemental_data = 23
    key_update = 24
    compressed_certificate = 25
    ekt_key = 26
    message_hash = 254

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Handshake Message Type 0x{0:02X}")


class TlsCipherSuite(enum.IntEnum):
    TLS_NULL_WITH_NULL_NULL = 0x0000
    TLS_RSA_WITH_NULL_MD5 = 0x0001
    TLS_RSA_WITH_NULL_SHA = 0x0002
    TLS_RSA_EXPORT_WITH_RC4_40_MD5 = 0x0003
    TLS_RSA_WITH_RC4_128_MD5 = 0x0004
    TLS_RSA_WITH_RC4_128_SHA = 0x0005
    TLS_RSA_EXPORT_WITH_RC2_CBC_40_MD5 = 0x0006
    TLS_RSA_WITH_IDEA_CBC_SHA = 0x0007
    TLS_RSA_EXPORT_WITH_DES40_CBC_SHA = 0x0008
    TLS_RSA_WITH_DES_CBC_SHA = 0x0009
    TLS_RSA_WITH_3DES_EDE_CBC_SHA = 0x000A
    TLS_DH_DSS_EXPORT_WITH_DES40_CBC_SHA = 0x000B
    TLS_DH_DSS_WITH_DES_CBC_SHA = 0x000C
    TLS_DH_DSS_WITH_3DES_EDE_CBC_SHA = 0x000D
    TLS_DH_RSA_EXPORT_WITH_DES40_CBC_SHA = 0x000E
    TLS_DH_RSA_WITH_DES_CBC_SHA = 0x000F
    TLS_DH_RSA_WITH_3DES_EDE_CBC_SHA = 0x0010
    TLS_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA = 0x0011
    TLS_DHE_DSS_WITH_DES_CBC_SHA = 0x0012
    TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA = 0x0013
    TLS_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA = 0x0014
    TLS_DHE_RSA_WITH_DES_CBC_SHA = 0x0015
    TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA = 0x0016
    TLS_DH_anon_EXPORT_WITH_RC4_40_MD5 = 0x0017
    TLS_DH_anon_WITH_RC4_128_MD5 = 0x0018
    TLS_DH_anon_EXPORT_WITH_DES40_CBC_SHA = 0x0019
    TLS_DH_anon_WITH_DES_CBC_SHA = 0x001A
    TLS_DH_anon_WITH_3DES_EDE_CBC_SHA = 0x001B
    TLS_KRB5_WITH_DES_CBC_SHA = 0x001E
    TLS_KRB5_WITH_3DES_EDE_CBC_SHA = 0x001F
    TLS_KRB5_WITH_RC4_128_SHA = 0x0020
    TLS_KRB5_WITH_IDEA_CBC_SHA = 0x0021
    TLS_KRB5_WITH_DES_CBC_MD5 = 0x0022
    TLS_KRB5_WITH_3DES_EDE_CBC_MD5 = 0x0023
    TLS_KRB5_WITH_RC4_128_MD5 = 0x0024
    TLS_KRB5_WITH_IDEA_CBC_MD5 = 0x0025
    TLS_KRB5_EXPORT_WITH_DES_CBC_40_SHA = 0x0026
    TLS_KRB5_EXPORT_WITH_RC2_CBC_40_SHA = 0x0027
    TLS_KRB5_EXPORT_WITH_RC4_40_SHA = 0x0028
    TLS_KRB5_EXPORT_WITH_DES_CBC_40_MD5 = 0x0029
    TLS_KRB5_EXPORT_WITH_RC2_CBC_40_MD5 = 0x002A
    TLS_KRB5_EXPORT_WITH_RC4_40_MD5 = 0x002B
    TLS_PSK_WITH_NULL_SHA = 0x002C
    TLS_DHE_PSK_WITH_NULL_SHA = 0x002D
    TLS_RSA_PSK_WITH_NULL_SHA = 0x002E
    TLS_RSA_WITH_AES_128_CBC_SHA = 0x002F
    TLS_DH_DSS_WITH_AES_128_CBC_SHA = 0x0030
    TLS_DH_RSA_WITH_AES_128_CBC_SHA = 0x0031
    TLS_DHE_DSS_WITH_AES_128_CBC_SHA = 0x0032
    TLS_DHE_RSA_WITH_AES_128_CBC_SHA = 0x0033
    TLS_DH_anon_WITH_AES_128_CBC_SHA = 0x0034
    TLS_RSA_WITH_AES_256_CBC_SHA = 0x0035
    TLS_DH_DSS_WITH_AES_256_CBC_SHA = 0x0036
    TLS_DH_RSA_WITH_AES_256_CBC_SHA = 0x0037
    TLS_DHE_DSS_WITH_AES_256_CBC_SHA = 0x0038
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA = 0x0039
    TLS_DH_anon_WITH_AES_256_CBC_SHA = 0x003A
    TLS_RSA_WITH_NULL_SHA256 = 0x003B
    TLS_RSA_WITH_AES_128_CBC_SHA256 = 0x003C
    TLS_RSA_WITH_AES_256_CBC_SHA256 = 0x003D
    TLS_DH_DSS_WITH_AES_128_CBC_SHA256 = 0x003E
    TLS_DH_RSA_WITH_AES_128_CBC_SHA256 = 0x003F
    TLS_DHE_DSS_WITH_AES_128_CBC_SHA256 = 0x0040
    TLS_RSA_WITH_CAMELLIA_128_CBC_SHA = 0x0041
    TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA = 0x0042
    TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA = 0x0043
    TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA = 0x0044
    TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA = 0x0045
    TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA = 0x0046
    TLS_DHE_RSA_WITH_AES_128_CBC_SHA256 = 0x0067
    TLS_DH_DSS_WITH_AES_256_CBC_SHA256 = 0x0068
    TLS_DH_RSA_WITH_AES_256_CBC_SHA256 = 0x0069
    TLS_DHE_DSS_WITH_AES_256_CBC_SHA256 = 0x006A
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA256 = 0x006B
    TLS_DH_anon_WITH_AES_128_CBC_SHA256 = 0x006C
    TLS_DH_anon_WITH_AES_256_CBC_SHA256 = 0x006D
    TLS_RSA_WITH_CAMELLIA_256_CBC_SHA = 0x0084
    TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA = 0x0085
    TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA = 0x0086
    TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA = 0x0087
    TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA = 0x0088
    TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA = 0x0089
    TLS_PSK_WITH_RC4_128_SHA = 0x008A
    TLS_PSK_WITH_3DES_EDE_CBC_SHA = 0x008B
    TLS_PSK_WITH_AES_128_CBC_SHA = 0x008C
    TLS_PSK_WITH_AES_256_CBC_SHA = 0x008D
    TLS_DHE_PSK_WITH_RC4_128_SHA = 0x008E
    TLS_DHE_PSK_WITH_3DES_EDE_CBC_SHA = 0x008F
    TLS_DHE_PSK_WITH_AES_128_CBC_SHA = 0x0090
    TLS_DHE_PSK_WITH_AES_256_CBC_SHA = 0x0091
    TLS_RSA_PSK_WITH_RC4_128_SHA = 0x0092
    TLS_RSA_PSK_WITH_3DES_EDE_CBC_SHA = 0x0093
    TLS_RSA_PSK_WITH_AES_128_CBC_SHA = 0x0094
    TLS_RSA_PSK_WITH_AES_256_CBC_SHA = 0x0095
    TLS_RSA_WITH_SEED_CBC_SHA = 0x0096
    TLS_DH_DSS_WITH_SEED_CBC_SHA = 0x0097
    TLS_DH_RSA_WITH_SEED_CBC_SHA = 0x0098
    TLS_DHE_DSS_WITH_SEED_CBC_SHA = 0x0099
    TLS_DHE_RSA_WITH_SEED_CBC_SHA = 0x009A
    TLS_DH_anon_WITH_SEED_CBC_SHA = 0x009B
    TLS_RSA_WITH_AES_128_GCM_SHA256 = 0x009C
    TLS_RSA_WITH_AES_256_GCM_SHA384 = 0x009D
    TLS_DHE_RSA_WITH_AES_128_GCM_SHA256 = 0x009E
    TLS_DHE_RSA_WITH_AES_256_GCM_SHA384 = 0x009F
    TLS_DH_RSA_WITH_AES_128_GCM_SHA256 = 0x00A0
    TLS_DH_RSA_WITH_AES_256_GCM_SHA384 = 0x00A1
    TLS_DHE_DSS_WITH_AES_128_GCM_SHA256 = 0x00A2
    TLS_DHE_DSS_WITH_AES_256_GCM_SHA384 = 0x00A3
    TLS_DH_DSS_WITH_AES_128_GCM_SHA256 = 0x00A4
    TLS_DH_DSS_WITH_AES_256_GCM_SHA384 = 0x00A5
    TLS_DH_anon_WITH_AES_128_GCM_SHA256 = 0x00A6
    TLS_DH_anon_WITH_AES_256_GCM_SHA384 = 0x00A7
    TLS_PSK_WITH_AES_128_GCM_SHA256 = 0x00A8
    TLS_PSK_WITH_AES_256_GCM_SHA384 = 0x00A9
    TLS_DHE_PSK_WITH_AES_128_GCM_SHA256 = 0x00AA
    TLS_DHE_PSK_WITH_AES_256_GCM_SHA384 = 0x00AB
    TLS_RSA_PSK_WITH_AES_128_GCM_SHA256 = 0x00AC
    TLS_RSA_PSK_WITH_AES_256_GCM_SHA384 = 0x00AD
    TLS_PSK_WITH_AES_128_CBC_SHA256 = 0x00AE
    TLS_PSK_WITH_AES_256_CBC_SHA384 = 0x00AF
    TLS_PSK_WITH_NULL_SHA256 = 0x00B0
    TLS_PSK_WITH_NULL_SHA384 = 0x00B1
    TLS_DHE_PSK_WITH_AES_128_CBC_SHA256 = 0x00B2
    TLS_DHE_PSK_WITH_AES_256_CBC_SHA384 = 0x00B3
    TLS_DHE_PSK_WITH_NULL_SHA256 = 0x00B4
    TLS_DHE_PSK_WITH_NULL_SHA384 = 0x00B5
    TLS_RSA_PSK_WITH_AES_128_CBC_SHA256 = 0x00B6
    TLS_RSA_PSK_WITH_AES_256_CBC_SHA384 = 0x00B7
    TLS_RSA_PSK_WITH_NULL_SHA256 = 0x00B8
    TLS_RSA_PSK_WITH_NULL_SHA384 = 0x00B9
    TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BA
    TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BB
    TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BC
    TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BD
    TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BE
    TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA256 = 0x00BF
    TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C0
    TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C1
    TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C2
    TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C3
    TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C4
    TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA256 = 0x00C5
    TLS_SM4_GCM_SM3 = 0x00C6
    TLS_SM4_CCM_SM3 = 0x00C7
    TLS_EMPTY_RENEGOTIATION_INFO_SCSV = 0x00FF
    TLS_AES_128_GCM_SHA256 = 0x1301
    TLS_AES_256_GCM_SHA384 = 0x1302
    TLS_CHACHA20_POLY1305_SHA256 = 0x1303
    TLS_AES_128_CCM_SHA256 = 0x1304
    TLS_AES_128_CCM_8_SHA256 = 0x1305
    TLS_FALLBACK_SCSV = 0x5600
    TLS_ECDH_ECDSA_WITH_NULL_SHA = 0xC001
    TLS_ECDH_ECDSA_WITH_RC4_128_SHA = 0xC002
    TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA = 0xC003
    TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA = 0xC004
    TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA = 0xC005
    TLS_ECDHE_ECDSA_WITH_NULL_SHA = 0xC006
    TLS_ECDHE_ECDSA_WITH_RC4_128_SHA = 0xC007
    TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA = 0xC008
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA = 0xC009
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA = 0xC00A
    TLS_ECDH_RSA_WITH_NULL_SHA = 0xC00B
    TLS_ECDH_RSA_WITH_RC4_128_SHA = 0xC00C
    TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA = 0xC00D
    TLS_ECDH_RSA_WITH_AES_128_CBC_SHA = 0xC00E
    TLS_ECDH_RSA_WITH_AES_256_CBC_SHA = 0xC00F
    TLS_ECDHE_RSA_WITH_NULL_SHA = 0xC010
    TLS_ECDHE_RSA_WITH_RC4_128_SHA = 0xC011
    TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA = 0xC012
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA = 0xC013
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA = 0xC014
    TLS_ECDH_anon_WITH_NULL_SHA = 0xC015
    TLS_ECDH_anon_WITH_RC4_128_SHA = 0xC016
    TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA = 0xC017
    TLS_ECDH_anon_WITH_AES_128_CBC_SHA = 0xC018
    TLS_ECDH_anon_WITH_AES_256_CBC_SHA = 0xC019
    TLS_SRP_SHA_WITH_3DES_EDE_CBC_SHA = 0xC01A
    TLS_SRP_SHA_RSA_WITH_3DES_EDE_CBC_SHA = 0xC01B
    TLS_SRP_SHA_DSS_WITH_3DES_EDE_CBC_SHA = 0xC01C
    TLS_SRP_SHA_WITH_AES_128_CBC_SHA = 0xC01D
    TLS_SRP_SHA_RSA_WITH_AES_128_CBC_SHA = 0xC01E
    TLS_SRP_SHA_DSS_WITH_AES_128_CBC_SHA = 0xC01F
    TLS_SRP_SHA_WITH_AES_256_CBC_SHA = 0xC020
    TLS_SRP_SHA_RSA_WITH_AES_256_CBC_SHA = 0xC021
    TLS_SRP_SHA_DSS_WITH_AES_256_CBC_SHA = 0xC022
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256 = 0xC023
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384 = 0xC024
    TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256 = 0xC025
    TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384 = 0xC026
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256 = 0xC027
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384 = 0xC028
    TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256 = 0xC029
    TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384 = 0xC02A
    TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 = 0xC02B
    TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 = 0xC02C
    TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256 = 0xC02D
    TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384 = 0xC02E
    TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 = 0xC02F
    TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 = 0xC030
    TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256 = 0xC031
    TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384 = 0xC032
    TLS_ECDHE_PSK_WITH_RC4_128_SHA = 0xC033
    TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA = 0xC034
    TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA = 0xC035
    TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA = 0xC036
    TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256 = 0xC037
    TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384 = 0xC038
    TLS_ECDHE_PSK_WITH_NULL_SHA = 0xC039
    TLS_ECDHE_PSK_WITH_NULL_SHA256 = 0xC03A
    TLS_ECDHE_PSK_WITH_NULL_SHA384 = 0xC03B
    TLS_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC03C
    TLS_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC03D
    TLS_DH_DSS_WITH_ARIA_128_CBC_SHA256 = 0xC03E
    TLS_DH_DSS_WITH_ARIA_256_CBC_SHA384 = 0xC03F
    TLS_DH_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC040
    TLS_DH_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC041
    TLS_DHE_DSS_WITH_ARIA_128_CBC_SHA256 = 0xC042
    TLS_DHE_DSS_WITH_ARIA_256_CBC_SHA384 = 0xC043
    TLS_DHE_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC044
    TLS_DHE_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC045
    TLS_DH_anon_WITH_ARIA_128_CBC_SHA256 = 0xC046
    TLS_DH_anon_WITH_ARIA_256_CBC_SHA384 = 0xC047
    TLS_ECDHE_ECDSA_WITH_ARIA_128_CBC_SHA256 = 0xC048
    TLS_ECDHE_ECDSA_WITH_ARIA_256_CBC_SHA384 = 0xC049
    TLS_ECDH_ECDSA_WITH_ARIA_128_CBC_SHA256 = 0xC04A
    TLS_ECDH_ECDSA_WITH_ARIA_256_CBC_SHA384 = 0xC04B
    TLS_ECDHE_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC04C
    TLS_ECDHE_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC04D
    TLS_ECDH_RSA_WITH_ARIA_128_CBC_SHA256 = 0xC04E
    TLS_ECDH_RSA_WITH_ARIA_256_CBC_SHA384 = 0xC04F
    TLS_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC050
    TLS_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC051
    TLS_DHE_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC052
    TLS_DHE_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC053
    TLS_DH_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC054
    TLS_DH_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC055
    TLS_DHE_DSS_WITH_ARIA_128_GCM_SHA256 = 0xC056
    TLS_DHE_DSS_WITH_ARIA_256_GCM_SHA384 = 0xC057
    TLS_DH_DSS_WITH_ARIA_128_GCM_SHA256 = 0xC058
    TLS_DH_DSS_WITH_ARIA_256_GCM_SHA384 = 0xC059
    TLS_DH_anon_WITH_ARIA_128_GCM_SHA256 = 0xC05A
    TLS_DH_anon_WITH_ARIA_256_GCM_SHA384 = 0xC05B
    TLS_ECDHE_ECDSA_WITH_ARIA_128_GCM_SHA256 = 0xC05C
    TLS_ECDHE_ECDSA_WITH_ARIA_256_GCM_SHA384 = 0xC05D
    TLS_ECDH_ECDSA_WITH_ARIA_128_GCM_SHA256 = 0xC05E
    TLS_ECDH_ECDSA_WITH_ARIA_256_GCM_SHA384 = 0xC05F
    TLS_ECDHE_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC060
    TLS_ECDHE_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC061
    TLS_ECDH_RSA_WITH_ARIA_128_GCM_SHA256 = 0xC062
    TLS_ECDH_RSA_WITH_ARIA_256_GCM_SHA384 = 0xC063
    TLS_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC064
    TLS_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC065
    TLS_DHE_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC066
    TLS_DHE_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC067
    TLS_RSA_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC068
    TLS_RSA_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC069
    TLS_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06A
    TLS_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06B
    TLS_DHE_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06C
    TLS_DHE_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06D
    TLS_RSA_PSK_WITH_ARIA_128_GCM_SHA256 = 0xC06E
    TLS_RSA_PSK_WITH_ARIA_256_GCM_SHA384 = 0xC06F
    TLS_ECDHE_PSK_WITH_ARIA_128_CBC_SHA256 = 0xC070
    TLS_ECDHE_PSK_WITH_ARIA_256_CBC_SHA384 = 0xC071
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC072
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC073
    TLS_ECDH_ECDSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC074
    TLS_ECDH_ECDSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC075
    TLS_ECDHE_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC076
    TLS_ECDHE_RSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC077
    TLS_ECDH_RSA_WITH_CAMELLIA_128_CBC_SHA256 = 0xC078
    TLS_ECDH_RSA_WITH_CAMELLIA_256_CBC_SHA384 = 0xC079
    TLS_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC07A
    TLS_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC07B
    TLS_DHE_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC07C
    TLS_DHE_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC07D
    TLS_DH_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC07E
    TLS_DH_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC07F
    TLS_DHE_DSS_WITH_CAMELLIA_128_GCM_SHA256 = 0xC080
    TLS_DHE_DSS_WITH_CAMELLIA_256_GCM_SHA384 = 0xC081
    TLS_DH_DSS_WITH_CAMELLIA_128_GCM_SHA256 = 0xC082
    TLS_DH_DSS_WITH_CAMELLIA_256_GCM_SHA384 = 0xC083
    TLS_DH_anon_WITH_CAMELLIA_128_GCM_SHA256 = 0xC084
    TLS_DH_anon_WITH_CAMELLIA_256_GCM_SHA384 = 0xC085
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC086
    TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC087
    TLS_ECDH_ECDSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC088
    TLS_ECDH_ECDSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC089
    TLS_ECDHE_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08A
    TLS_ECDHE_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08B
    TLS_ECDH_RSA_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08C
    TLS_ECDH_RSA_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08D
    TLS_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC08E
    TLS_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC08F
    TLS_DHE_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC090
    TLS_DHE_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC091
    TLS_RSA_PSK_WITH_CAMELLIA_128_GCM_SHA256 = 0xC092
    TLS_RSA_PSK_WITH_CAMELLIA_256_GCM_SHA384 = 0xC093
    TLS_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC094
    TLS_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC095
    TLS_DHE_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC096
    TLS_DHE_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC097
    TLS_RSA_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC098
    TLS_RSA_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC099
    TLS_ECDHE_PSK_WITH_CAMELLIA_128_CBC_SHA256 = 0xC09A
    TLS_ECDHE_PSK_WITH_CAMELLIA_256_CBC_SHA384 = 0xC09B
    TLS_RSA_WITH_AES_128_CCM = 0xC09C
    TLS_RSA_WITH_AES_256_CCM = 0xC09D
    TLS_DHE_RSA_WITH_AES_128_CCM = 0xC09E
    TLS_DHE_RSA_WITH_AES_256_CCM = 0xC09F
    TLS_RSA_WITH_AES_128_CCM_8 = 0xC0A0
    TLS_RSA_WITH_AES_256_CCM_8 = 0xC0A1
    TLS_DHE_RSA_WITH_AES_128_CCM_8 = 0xC0A2
    TLS_DHE_RSA_WITH_AES_256_CCM_8 = 0xC0A3
    TLS_PSK_WITH_AES_128_CCM = 0xC0A4
    TLS_PSK_WITH_AES_256_CCM = 0xC0A5
    TLS_DHE_PSK_WITH_AES_128_CCM = 0xC0A6
    TLS_DHE_PSK_WITH_AES_256_CCM = 0xC0A7
    TLS_PSK_WITH_AES_128_CCM_8 = 0xC0A8
    TLS_PSK_WITH_AES_256_CCM_8 = 0xC0A9
    TLS_PSK_DHE_WITH_AES_128_CCM_8 = 0xC0AA
    TLS_PSK_DHE_WITH_AES_256_CCM_8 = 0xC0AB
    TLS_ECDHE_ECDSA_WITH_AES_128_CCM = 0xC0AC
    TLS_ECDHE_ECDSA_WITH_AES_256_CCM = 0xC0AD
    TLS_ECDHE_ECDSA_WITH_AES_128_CCM_8 = 0xC0AE
    TLS_ECDHE_ECDSA_WITH_AES_256_CCM_8 = 0xC0AF
    TLS_ECCPWD_WITH_AES_128_GCM_SHA256 = 0xC0B0
    TLS_ECCPWD_WITH_AES_256_GCM_SHA384 = 0xC0B1
    TLS_ECCPWD_WITH_AES_128_CCM_SHA256 = 0xC0B2
    TLS_ECCPWD_WITH_AES_256_CCM_SHA384 = 0xC0B3
    TLS_SHA256_SHA256 = 0xC0B4
    TLS_SHA384_SHA384 = 0xC0B5
    TLS_GOSTR341112_256_WITH_KUZNYECHIK_CTR_OMAC = 0xC100
    TLS_GOSTR341112_256_WITH_MAGMA_CTR_OMAC = 0xC101
    TLS_GOSTR341112_256_WITH_28147_CNT_IMIT = 0xC102
    TLS_GOSTR341112_256_WITH_KUZNYECHIK_MGM_L = 0xC103
    TLS_GOSTR341112_256_WITH_MAGMA_MGM_L = 0xC104
    TLS_GOSTR341112_256_WITH_KUZNYECHIK_MGM_S = 0xC105
    TLS_GOSTR341112_256_WITH_MAGMA_MGM_S = 0xC106
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCA8
    TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCA9
    TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAA
    TLS_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAB
    TLS_ECDHE_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAC
    TLS_DHE_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAD
    TLS_RSA_PSK_WITH_CHACHA20_POLY1305_SHA256 = 0xCCAE
    TLS_ECDHE_PSK_WITH_AES_128_GCM_SHA256 = 0xD001
    TLS_ECDHE_PSK_WITH_AES_256_GCM_SHA384 = 0xD002
    TLS_ECDHE_PSK_WITH_AES_128_CCM_8_SHA256 = 0xD003
    TLS_ECDHE_PSK_WITH_AES_128_CCM_SHA256 = 0xD005

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Cipher Suite 0x{0:04X}")


class TlsCompressionMethod(enum.IntEnum):
    none = 0
    deflate = 1
    lzs = 64

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Compression Method 0x{0:02X}")


class TlsExtensionType(enum.IntEnum):
    server_name = 0
    max_fragment_length = 1
    client_certificate_url = 2
    trusted_ca_keys = 3
    truncated_hmac = 4
    status_request = 5
    user_mapping = 6
    client_authz = 7
    server_authz = 8
    cert_type = 9
    supported_groups = 10
    ec_point_formats = 11
    srp = 12
    signature_algorithms = 13
    use_srtp = 14
    heartbeat = 15
    application_layer_protocol_negotiation = 16
    status_request_v2 = 17
    signed_certificate_timestamp = 18
    client_certificate_type = 19
    server_certificate_type = 20
    padding = 21
    encrypt_then_mac = 22
    extended_master_secret = 23
    token_binding = 24
    cached_info = 25
    tls_lts = 26
    compress_certificate = 27
    record_size_limit = 28
    pwd_protect = 29
    pwd_clear = 30
    password_salt = 31
    ticket_pinning = 32
    tls_cert_with_extern_psk = 33
    delegated_credentials = 34
    session_ticket = 35
    TLMSP = 36
    TLMSP_proxying = 37
    TLMSP_delegate = 38
    supported_ekt_ciphers = 39
    pre_shared_key = 41
    early_data = 42
    supported_versions = 43
    cookie = 44
    psk_key_exchange_modes = 45
    certificate_authorities = 47
    oid_filters = 48
    post_handshake_auth = 49
    signature_algorithms_cert = 50
    key_share = 51
    transparency_info = 52
    connection_id_deprecated = 53
    connection_id = 54
    external_id_hash = 55
    external_session_id = 56
    quic_transport_parameters = 57
    ticket_request = 58
    dnssec_chain = 69
    renegotiation_info = 65281

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Extension Type 0x{0:04X}")


class TlsServerNameType(enum.IntEnum):
    server_name = 0

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Server Name Type 0x{0:02X}")


class TlsECPointFormat(enum.IntEnum):
    uncompressed = 0
    ansiX962_compressed_prime = 1
    ansiX962_compressed_char2 = 2

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown EC Point Format 0x{0:02X}")


class TlsSupportedGroup(enum.IntEnum):
    reserved = 0
    sect163k1 = 1
    sect163r1 = 2
    sect163r2 = 3
    sect193r1 = 4
    sect193r2 = 5
    sect233k1 = 6
    sect233r1 = 7
    sect239k1 = 8
    sect283k1 = 9
    sect283r1 = 10
    sect409k1 = 11
    sect409r1 = 12
    sect571k1 = 13
    sect571r1 = 14
    secp160k1 = 15
    secp160r1 = 16
    secp160r2 = 17
    secp192k1 = 18
    secp192r1 = 19
    secp224k1 = 20
    secp224r1 = 21
    secp256k1 = 22
    secp256r1 = 23
    secp384r1 = 24
    secp521r1 = 25
    brainpoolP256r1 = 26
    brainpoolP384r1 = 27
    brainpoolP512r1 = 28
    x25519 = 29
    x448 = 30
    brainpoolP256r1tls13 = 31
    brainpoolP384r1tls13 = 32
    brainpoolP512r1tls13 = 33
    GC256A = 34
    GC256B = 35
    GC256C = 36
    GC256D = 37
    GC512A = 38
    GC512B = 39
    GC512C = 40
    curveSM2 = 41
    ffdhe2048 = 256
    ffdhe3072 = 257
    ffdhe4096 = 258
    ffdhe6144 = 259
    ffdhe8192 = 260
    arbitrary_explicit_prime_curves = 65281
    arbitrary_explicit_char2_curves = 65282

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Supported Group 0x{0:04X}")


class TlsSignatureScheme(enum.IntEnum):
    rsa_pkcs1_sha1 = 0x0201
    dsa_sha1 = 0x0202
    ecdsa_sha1 = 0x0203
    sha224_rsa = 0x0301
    dsa_sha224 = 0x0302
    sha224_ecdsa = 0x0303
    rsa_pkcs1_sha256 = 0x0401
    dsa_sha256 = 0x0402
    ecdsa_secp256r1_sha256 = 0x0403
    rsa_pkcs1_sha256_legacy = 0x0420
    rsa_pkcs1_sha384 = 0x0501
    dsa_sha384 = 0x0502
    ecdsa_secp384r1_sha384 = 0x0503
    rsa_pkcs1_sha384_legacy = 0x0520
    rsa_pkcs1_sha512 = 0x0601
    dsa_sha512 = 0x0602
    ecdsa_secp521r1_sha512 = 0x0603
    rsa_pkcs1_sha512_legacy = 0x0620
    eccsi_sha256 = 0x0704
    iso_ibs1 = 0x0705
    iso_ibs2 = 0x0706
    iso_chinese_ibs = 0x0707
    sm2sig_sm3 = 0x0708
    gostr34102012_256a = 0x0709
    gostr34102012_256b = 0x070A
    gostr34102012_256c = 0x070B
    gostr34102012_256d = 0x070C
    gostr34102012_512a = 0x070D
    gostr34102012_512b = 0x070E
    gostr34102012_512c = 0x070F
    rsa_pss_rsae_sha256 = 0x0804
    rsa_pss_rsae_sha384 = 0x0805
    rsa_pss_rsae_sha512 = 0x0806
    ed25519 = 0x0807
    ed448 = 0x0808
    rsa_pss_pss_sha256 = 0x0809
    rsa_pss_pss_sha384 = 0x080A
    rsa_pss_pss_sha512 = 0x080B
    ecdsa_brainpoolP256r1tls13_sha256 = 0x081A
    ecdsa_brainpoolP384r1tls13_sha384 = 0x081B
    ecdsa_brainpoolP512r1tls13_sha512 = 0x081C

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Signature Scheme 0x{0:04X}")


class TlsPskKeyExchangeMode(enum.IntEnum):
    psk_ke = 0
    psk_dhe_ke = 1

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown PSK Key Exchange Mode 0x{0:02X}")


class TlsECCurveType(enum.IntEnum):
    unassigned = 0
    explicit_primve = 1
    explicit_char2 = 2
    named_curve = 3

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown EC Curve Type 0x{0:02X}")


class TlsClientCertificateType(enum.IntEnum):
    rsa_sign = 1
    dss_sign = 2
    rsa_fixed_dh = 3
    dss_fixed_dh = 4
    rsa_ephemeral_dh = 5
    dss_ephemeral_dh = 6
    fortezza_dms = 20
    ecdsa_sign = 64
    rsa_fixed_ecdh = 65
    ecdsa_fixed_ecdh = 66
    gost_sign256 = 67
    gost_sign512 = 68

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        return _add_missing_enum_member(cls, value, "Unknown Client Certificate Type 0x{0:02X}")


class DistinguishedNameType(str, enum.Enum):
    object_class = "2.5.4.0"
    aliased_entry_name = "2.5.4.1"
    knowledge_information = "2.5.4.2"
    common_name = "2.5.4.3"
    surname = "2.5.4.4"
    serial_number = "2.5.4.5"
    country_name = "2.5.4.6"
    locality_name = "2.5.4.7"
    state_or_province_name = "2.5.4.8"
    street_address = "2.5.4.9"
    organizational_name = "2.5.4.10"
    organizational_unit_name = "2.5.4.11"
    id_title = "2.5.4.12"
    description = "2.5.4.13"
    search_guide = "2.5.4.14"
    business_category = "2.5.4.15"
    postal_address = "2.5.4.16"
    postal_code = "2.5.4.17"
    post_office_box = "2.5.4.18"
    physical_delivery_office_name = "2.5.4.19"
    telephone_number = "2.5.4.20"
    telex_number = "2.5.4.21"
    teletex_terminal_identifier = "2.5.4.22"
    facsimile_telephone_number = "2.5.4.23"
    x121_address = "2.5.4.24"
    international_isdn_number = "2.5.4.25"
    registered_address = "2.5.4.26"
    destination_indicator = "2.5.4.27"
    preferred_delivery_method = "2.5.4.28"
    presentation_address = "2.5.4.29"
    supported_application_context = "2.5.4.30"
    member = "2.5.4.31"
    owner = "2.5.4.32"
    role_occupant = "2.5.4.33"
    see_also = "2.5.4.34"
    user_password = "2.5.4.35"
    user_certificate = "2.5.4.36"
    ca_certificate = "2.5.4.37"
    authority_revocation_list = "2.5.4.38"
    certificate_revocation_list = "2.5.4.39"
    cross_certificate_pair = "2.5.4.40"
    id_name = "2.5.4.41"
    given_name = "2.5.4.42"
    initials = "2.5.4.43"
    generation_qualifier = "2.5.4.44"
    unique_identifier = "2.5.4.45"
    dn_qualifier = "2.5.4.46"
    enhanced_search_guide = "2.5.4.47"
    protocol_information = "2.5.4.48"
    distinguished_name = "2.5.4.49"
    unique_member = "2.5.4.50"
    house_identifier = "2.5.4.51"
    supported_algorithms = "2.5.4.52"
    delta_revocation_list = "2.5.4.53"
    attribute_certificate = "2.5.4.58"
    pseudonym = "2.5.4.65"

    @classmethod
    def _missing_(cls, value: object) -> typing.Optional[enum.Enum]:
        new_member = str.__new__(cls)
        new_member._name_ = f"Unknown DN OID Type {value}"
        new_member._value_ = str(value)
        return cls._value2member_map_.setdefault(value, new_member)

    @classmethod
    def native_labels(cls) -> typing.Dict["DistinguishedNameType", str]:
        return {
            DistinguishedNameType.object_class: "id-at-objectClass",
            DistinguishedNameType.aliased_entry_name: "id-at-aliasedEntryName",
            DistinguishedNameType.knowledge_information: "id-at-knowldgeinformation",
            DistinguishedNameType.common_name: "id-at-commonName",
            DistinguishedNameType.surname: "id-at-surname",
            DistinguishedNameType.serial_number: "id-at-serialNumber",
            DistinguishedNameType.country_name: "id-at-countryName",
            DistinguishedNameType.locality_name: "id-at-localityName",
            DistinguishedNameType.state_or_province_name: "id-at-stateOrProvinceName",
            DistinguishedNameType.street_address: "id-at-streetAddress",
            DistinguishedNameType.organizational_name: "id-at-organizationName",
            DistinguishedNameType.organizational_unit_name: "id-at-organizationalUnitName",
            DistinguishedNameType.id_title: "id-at-title",
            DistinguishedNameType.description: "id-at-description",
            DistinguishedNameType.search_guide: "id-at-searchGuide",
            DistinguishedNameType.business_category: "id-at-businessCategory",
            DistinguishedNameType.postal_address: "id-at-postalAddress",
            DistinguishedNameType.postal_code: "id-at-postalCode",
            DistinguishedNameType.post_office_box: "id-at-postOfficeBox",
            DistinguishedNameType.physical_delivery_office_name: "id-at-physicalDeliveryOfficeName",
            DistinguishedNameType.telephone_number: "id-at-telephoneNumber",
            DistinguishedNameType.telex_number: "id-at-telexNumber",
            DistinguishedNameType.teletex_terminal_identifier: "id-at-teletexTerminalIdentifier",
            DistinguishedNameType.facsimile_telephone_number: "id-at-facsimileTelephoneNumber",
            DistinguishedNameType.x121_address: "id-at-x121Address",
            DistinguishedNameType.international_isdn_number: "id-at-internationalISDNNumber",
            DistinguishedNameType.registered_address: "id-at-registeredAddress",
            DistinguishedNameType.destination_indicator: "id-at-destinationIndicator",
            DistinguishedNameType.preferred_delivery_method: "id-at-preferredDeliveryMethod",
            DistinguishedNameType.presentation_address: "id-at-presentationAddress",
            DistinguishedNameType.supported_application_context: "id-at-supportedApplicationContext",
            DistinguishedNameType.member: "id-at-member",
            DistinguishedNameType.owner: "id-at-owner",
            DistinguishedNameType.role_occupant: "id-at-roleOccupant",
            DistinguishedNameType.see_also: "id-at-seeAlso",
            DistinguishedNameType.user_password: "id-at-userPassword",
            DistinguishedNameType.user_certificate: "id-at-userCertificate",
            DistinguishedNameType.ca_certificate: "id-at-cACertificate",
            DistinguishedNameType.authority_revocation_list: "id-at-authorityRevocationList",
            DistinguishedNameType.certificate_revocation_list: "id-at-certificateRevocationList",
            DistinguishedNameType.cross_certificate_pair: "id-at-crossCertificatePair",
            DistinguishedNameType.id_name: "id-at-name",
            DistinguishedNameType.given_name: "id-at-givenName",
            DistinguishedNameType.initials: "id-at-initials",
            DistinguishedNameType.generation_qualifier: "id-at-generationQualifier",
            DistinguishedNameType.unique_identifier: "id-at-uniqueIdentifier",
            DistinguishedNameType.dn_qualifier: "id-at-dnQualifier",
            DistinguishedNameType.enhanced_search_guide: "id-at-enhancedSearchGuide",
            DistinguishedNameType.protocol_information: "id-at-protocolInformation",
            DistinguishedNameType.distinguished_name: "id-at-distinguishedName",
            DistinguishedNameType.unique_member: "id-at-uniqueMember",
            DistinguishedNameType.house_identifier: "id-at-houseIdentifier",
            DistinguishedNameType.supported_algorithms: "id-at-supportedAlgorithms",
            DistinguishedNameType.delta_revocation_list: "id-at-deltaRevocationList",
            DistinguishedNameType.attribute_certificate: "id-at-attributeCertificate",
            DistinguishedNameType.pseudonym: "id-at-pseudonym",
        }
