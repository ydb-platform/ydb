#!/usr/bin/env python
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK

# Copyright: (c) 2020 Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

"""
Script that can be used to parse a Negotiate token and output a human readable structure. You can pass in an actual
SPNEGO token or just a raw Kerberos or NTLM token, the script should be smart enough to detect the structure of the
input.
"""

import argparse
import base64
import json
import os.path
import re
import struct
import sys
import typing

from spnego._asn1 import (
    unpack_asn1,
    unpack_asn1_object_identifier,
    unpack_asn1_sequence,
)
from spnego._context import GSSMech
from spnego._kerberos import (
    KerberosV5Msg,
    parse_enum,
    parse_flags,
    parse_kerberos_token,
)
from spnego._ntlm_raw.crypto import hmac_md5, ntowfv1, ntowfv2, rc4k
from spnego._ntlm_raw.messages import (
    Authenticate,
    AvId,
    Challenge,
    Negotiate,
    NegotiateFlags,
    NTClientChallengeV2,
    TargetInfo,
    Version,
)
from spnego._spnego import InitialContextToken, NegTokenInit, NegTokenResp, unpack_token
from spnego._text import to_bytes
from spnego._tls_struct import (
    DistinguishedNameType,
    TlsCipherSuite,
    TlsClientCertificateType,
    TlsCompressionMethod,
    TlsContentType,
    TlsECCurveType,
    TlsECPointFormat,
    TlsExtensionType,
    TlsHandshakeMessageType,
    TlsProtocolVersion,
    TlsPskKeyExchangeMode,
    TlsServerNameType,
    TlsSignatureScheme,
    TlsSupportedGroup,
)

HAS_ARGCOMPLETE = False
try:
    import argcomplete

    HAS_ARGCOMPLETE = True
except ImportError:  # pragma: nocover
    pass


HAS_YAML = False
try:
    from ruamel import yaml

    HAS_YAML = True
except ImportError:  # pragma: nocover
    pass


def _parse_ntlm_version(
    version: typing.Optional[Version],
) -> typing.Optional[typing.Dict[str, typing.Union[int, str]]]:
    if not version:
        return None

    return {
        "Major": version.major,
        "Minor": version.minor,
        "Build": version.build,
        "Reserved": base64.b16encode(version.reserved).decode(),
        "NTLMRevision": version.revision,
    }


def _parse_ntlm_target_info(
    target_info: typing.Optional[TargetInfo],
) -> typing.Optional[typing.List[typing.Dict[str, typing.Any]]]:
    if target_info is None:
        return None

    text_values = [
        AvId.nb_computer_name,
        AvId.nb_domain_name,
        AvId.dns_computer_name,
        AvId.dns_domain_name,
        AvId.dns_tree_name,
        AvId.target_name,
    ]

    info = []
    for av_id, raw_value in target_info.items():

        if av_id == AvId.eol:
            value = None
        elif av_id in text_values:
            value = raw_value
        elif av_id == AvId.flags:
            value = parse_flags(raw_value)
        elif av_id == AvId.timestamp:
            value = str(raw_value)
        elif av_id == AvId.single_host:
            value = {
                "Size": raw_value.size,
                "Z4": raw_value.z4,
                "CustomData": base64.b16encode(raw_value.custom_data).decode(),
                "MachineId": base64.b16encode(raw_value.machine_id).decode(),
            }
        else:
            value = base64.b16encode(raw_value).decode()

        info.append({"AvId": parse_enum(av_id), "Value": value})

    return info


def _parse_ntlm_negotiate(data: Negotiate) -> typing.Dict[str, typing.Any]:
    b_data = data.pack()

    msg = {
        "NegotiateFlags": parse_flags(data.flags, enum_type=NegotiateFlags),
        "DomainNameFields": {
            "Len": struct.unpack("<H", b_data[16:18])[0],
            "MaxLen": struct.unpack("<H", b_data[18:20])[0],
            "BufferOffset": struct.unpack("<I", b_data[20:24])[0],
        },
        "WorkstationFields": {
            "Len": struct.unpack("<H", b_data[24:26])[0],
            "MaxLen": struct.unpack("<H", b_data[26:28])[0],
            "BufferOffset": struct.unpack("<I", b_data[28:32])[0],
        },
        "Version": _parse_ntlm_version(data.version),
        "Payload": {
            "DomainName": data.domain_name,
            "Workstation": data.workstation,
        },
    }

    return msg


def _parse_ntlm_challenge(data: Challenge) -> typing.Dict[str, typing.Any]:
    b_data = data.pack()

    msg = {
        "TargetNameFields": {
            "Len": struct.unpack("<H", b_data[12:14])[0],
            "MaxLen": struct.unpack("<H", b_data[14:16])[0],
            "BufferOffset": struct.unpack("<I", b_data[16:20])[0],
        },
        "NegotiateFlags": parse_flags(data.flags, enum_type=NegotiateFlags),
        "ServerChallenge": base64.b16encode(b_data[24:32]).decode(),
        "Reserved": base64.b16encode(b_data[32:40]).decode(),
        "TargetInfoFields": {
            "Len": struct.unpack("<H", b_data[40:42])[0],
            "MaxLen": struct.unpack("<H", b_data[42:44])[0],
            "BufferOffset": struct.unpack("<I", b_data[44:48])[0],
        },
        "Version": _parse_ntlm_version(data.version),
        "Payload": {
            "TargetName": data.target_name,
            "TargetInfo": _parse_ntlm_target_info(data.target_info),
        },
    }

    return msg


def _parse_ntlm_authenticate(data: Authenticate, password: typing.Optional[str]) -> typing.Dict[str, typing.Any]:
    b_data = data.pack()

    msg: typing.Dict[str, typing.Any] = {
        "LmChallengeResponseFields": {
            "Len": struct.unpack("<H", b_data[12:14])[0],
            "MaxLen": struct.unpack("<H", b_data[14:16])[0],
            "BufferOffset": struct.unpack("<I", b_data[16:20])[0],
        },
        "NtChallengeResponseFields": {
            "Len": struct.unpack("<H", b_data[20:22])[0],
            "MaxLen": struct.unpack("<H", b_data[22:24])[0],
            "BufferOffset": struct.unpack("<I", b_data[24:28])[0],
        },
        "DomainNameFields": {
            "Len": struct.unpack("<H", b_data[28:30])[0],
            "MaxLen": struct.unpack("<H", b_data[30:32])[0],
            "BufferOffset": struct.unpack("<I", b_data[32:36])[0],
        },
        "UserNameFields": {
            "Len": struct.unpack("<H", b_data[36:38])[0],
            "MaxLen": struct.unpack("<H", b_data[38:40])[0],
            "BufferOffset": struct.unpack("<I", b_data[40:44])[0],
        },
        "WorkstationFields": {
            "Len": struct.unpack("<H", b_data[44:46])[0],
            "MaxLen": struct.unpack("<H", b_data[46:48])[0],
            "BufferOffset": struct.unpack("<I", b_data[48:52])[0],
        },
        "EncryptedRandomSessionKeyFields": {
            "Len": struct.unpack("<H", b_data[52:54])[0],
            "MaxLen": struct.unpack("<H", b_data[54:56])[0],
            "BufferOffset": struct.unpack("<I", b_data[56:60])[0],
        },
        "NegotiateFlags": parse_flags(data.flags, enum_type=NegotiateFlags),
        "Version": _parse_ntlm_version(data.version),
        "MIC": base64.b16encode(data.mic).decode() if data.mic else None,
        "Payload": {
            "LmChallengeResponse": None,
            "NtChallengeResponse": None,
            "DomainName": data.domain_name,
            "UserName": data.user_name,
            "Workstation": data.workstation,
            "EncryptedRandomSessionKey": None,
        },
    }

    key_exchange_key = None
    lm_response_data = data.lm_challenge_response
    nt_response_data = data.nt_challenge_response

    if lm_response_data:
        lm_response: typing.Dict[str, typing.Any] = {
            "ResponseType": None,
            "LMProofStr": None,
        }

        if not nt_response_data or len(nt_response_data) == 24:
            lm_response["ResponseType"] = "LMv1"
            lm_response["LMProofStr"] = base64.b16encode(lm_response_data).decode()

        else:
            lm_response["ResponseType"] = "LMv2"
            lm_response["LMProofStr"] = base64.b16encode(lm_response_data[:16]).decode()
            lm_response["ChallengeFromClient"] = base64.b16encode(lm_response_data[16:]).decode()

        msg["Payload"]["LmChallengeResponse"] = lm_response

    if nt_response_data:
        nt_response: typing.Dict[str, typing.Any] = {
            "ResponseType": None,
            "NTProofStr": None,
        }

        if len(nt_response_data) == 24:
            nt_response["ResponseType"] = "NTLMv1"
            nt_response["NTProofStr"] = base64.b16encode(nt_response_data).decode()

            # TODO: need to get a sane way to include the server challenge for ESS KXKEY.
            # if password and lm_response_data:
            #     session_base_key = hashlib.new('md4', ntowfv1(password)).digest()
            #     lmowf = lmowfv1(password)
            #     if data.flags & NegotiateFlags.extended_session_security == 0:
            #         key_exchange_key = kxkey(data.flags, session_base_key, lmowf, lm_response_data, b"")

        else:
            nt_proof_str = nt_response_data[:16]
            nt_response["ResponseType"] = "NTLMv2"
            nt_response["NTProofStr"] = base64.b16encode(nt_proof_str).decode()

            challenge = NTClientChallengeV2.unpack(nt_response_data[16:])
            b_challenge = nt_response_data[16:]

            nt_response["ClientChallenge"] = {
                "RespType": challenge.resp_type,
                "HiRespType": challenge.hi_resp_type,
                "Reserved1": struct.unpack("<H", b_challenge[2:4])[0],
                "Reserved2": struct.unpack("<I", b_challenge[4:8])[0],
                "TimeStamp": str(challenge.time_stamp),
                "ChallengeFromClient": base64.b16encode(challenge.challenge_from_client).decode(),
                "Reserved3": struct.unpack("<I", b_challenge[24:28])[0],
                "AvPairs": _parse_ntlm_target_info(challenge.av_pairs),
                "Reserved4": struct.unpack("<I", b_challenge[-4:])[0],
            }

            if password:
                response_key_nt = ntowfv2(msg["Payload"]["UserName"], ntowfv1(password), msg["Payload"]["DomainName"])
                key_exchange_key = hmac_md5(response_key_nt, nt_proof_str)

        msg["Payload"]["NtChallengeResponse"] = nt_response

    if data.encrypted_random_session_key:
        msg["Payload"]["EncryptedRandomSessionKey"] = base64.b16encode(data.encrypted_random_session_key).decode()

    if data.flags & NegotiateFlags.key_exch and (data.flags & NegotiateFlags.sign or data.flags & NegotiateFlags.seal):
        session_key = None
        if key_exchange_key:
            session_key = rc4k(key_exchange_key, typing.cast(bytes, data.encrypted_random_session_key))

    else:
        session_key = key_exchange_key

    msg["SessionKey"] = base64.b16encode(session_key).decode() if session_key else "Failed to derive"

    return msg


def _parse_spnego_init(
    data: NegTokenInit,
    secret: typing.Optional[str] = None,
    encoding: typing.Optional[str] = None,
) -> typing.Dict[str, typing.Any]:
    mech_types = [parse_enum(m, enum_type=GSSMech) for m in data.mech_types] if data.mech_types else None

    mech_token = None
    if data.mech_token:
        mech_token = parse_token(data.mech_token, secret=secret, encoding=encoding)

    encoding = encoding or "utf-8"

    msg = {
        "mechTypes": mech_types,
        "reqFlags": parse_flags(data.req_flags) if data.req_flags is not None else None,
        "mechToken": mech_token,
        "mechListMIC": base64.b16encode(data.mech_list_mic).decode() if data.mech_list_mic is not None else None,
    }

    if data.hint_name or data.hint_address:
        # This is a NegTokenInit2 structure.
        msg["negHints"] = {
            "hintName": data.hint_name.decode(encoding) if data.hint_name else None,
            "hintAddress": data.hint_address.decode(encoding) if data.hint_address else None,
        }

    return msg


def _parse_spnego_resp(
    data: NegTokenResp,
    secret: typing.Optional[str] = None,
    encoding: typing.Optional[str] = None,
) -> typing.Dict[str, typing.Any]:
    supported_mech = parse_enum(data.supported_mech, enum_type=GSSMech) if data.supported_mech else None

    response_token = None
    if data.response_token:
        response_token = parse_token(data.response_token, secret=secret, encoding=encoding)

    msg = {
        "negState": parse_enum(data.neg_state) if data.neg_state is not None else None,
        "supportedMech": supported_mech,
        "responseToken": response_token,
        "mechListMIC": base64.b16encode(data.mech_list_mic).decode() if data.mech_list_mic is not None else None,
    }
    return msg


def _parse_tls_handshake_client_hello(
    view: memoryview,
) -> typing.Dict[str, typing.Any]:
    protocol_version = TlsProtocolVersion(struct.unpack(">H", view[:2])[0])
    view = view[2:]

    random = view[:32]
    view = view[32:]

    session_id_len = struct.unpack("B", view[:1])[0]
    view = view[1:]

    session_id = view[:session_id_len]
    view = view[session_id_len:]

    cipher_suites_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]
    cipher_suites_view = view[:cipher_suites_len]
    view = view[cipher_suites_len:]

    cipher_suites = []
    while cipher_suites_view:
        cs = TlsCipherSuite(struct.unpack(">H", cipher_suites_view[:2])[0])
        cipher_suites.append(f"{cs.name} - 0x{cs.value:04X}")
        cipher_suites_view = cipher_suites_view[2:]

    compression_methods_len = struct.unpack("B", view[:1])[0]
    view = view[1:]
    compression_methods_view = view[:compression_methods_len]
    view = view[compression_methods_len:]

    compression_methods = []
    while compression_methods_view:
        cm = TlsCompressionMethod(struct.unpack("B", compression_methods_view[:1])[0])
        compression_methods.append(parse_enum(cm))
        compression_methods_view = compression_methods_view[1:]

    extensions_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]
    extensions_view = view[:extensions_len]
    view = view[extensions_len:]
    extensions = _parse_tls_extensions(extensions_view, True)

    return {
        "ProtocolVersion": parse_enum(protocol_version),
        "Random": base64.b16encode(random).decode(),
        "SessionID": base64.b16encode(session_id).decode(),
        "CipherSuites": cipher_suites,
        "CompressionMethods": compression_methods,
        "Extensions": extensions,
    }


def _parse_tls_handshake_server_hello(
    view: memoryview,
) -> typing.Dict[str, typing.Any]:
    protocol_version = TlsProtocolVersion(struct.unpack(">H", view[:2])[0])
    view = view[2:]

    random = view[:32]
    view = view[32:]

    session_id_len = struct.unpack("B", view[:1])[0]
    view = view[1:]

    session_id = view[:session_id_len]
    view = view[session_id_len:]

    cipher_suite = TlsCipherSuite(struct.unpack(">H", view[:2])[0])
    view = view[2:]

    compression_method = TlsCompressionMethod(struct.unpack("B", view[:1])[0])
    view = view[1:]

    extensions_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]
    extensions_view = view[:extensions_len]
    view = view[extensions_len:]
    extensions = _parse_tls_extensions(extensions_view, False)

    return {
        "ProtocolVersion": parse_enum(protocol_version),
        "Random": base64.b16encode(random).decode(),
        "SessionID": base64.b16encode(session_id).decode(),
        "CipherSuite": f"{cipher_suite.name} - 0x{cipher_suite.value:04X}",
        "CompressionMethod": parse_enum(compression_method),
        "Extensions": extensions,
    }


def _parse_tls_handshake_certificate_request(
    view: memoryview,
) -> typing.Dict[str, typing.Any]:
    cert_types_len = struct.unpack("B", view[:1])[0]
    view = view[1:]
    cert_types_view = view[:cert_types_len]
    view = view[cert_types_len:]

    cert_types = []
    while cert_types_view:
        ct = TlsClientCertificateType(struct.unpack("B", cert_types_view[:1])[0])
        cert_types.append(parse_enum(ct))
        cert_types_view = cert_types_view[1:]

    sig_algos_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]
    sig_algos_view = view[:sig_algos_len]
    view = view[sig_algos_len:]

    sig_algos = []
    while sig_algos_view:
        algo = TlsSignatureScheme(struct.unpack(">H", sig_algos_view[:2])[0])
        sig_algos.append(parse_enum(algo))
        sig_algos_view = sig_algos_view[2:]

    dn_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]
    dn_view = view[:dn_len]
    view = view[dn_len:]

    dns = []
    while dn_view:
        entry_len = struct.unpack(">H", dn_view[:2])[0]
        dn_view = dn_view[2:]
        entry_view = dn_view[:entry_len]
        dn_view = dn_view[entry_len:]

        for dn_entry in unpack_asn1_sequence(entry_view):
            for dn_set in unpack_asn1_sequence(dn_entry):
                dn_data = unpack_asn1(dn_set.b_data)[0]
                dn_oid, dn_str = unpack_asn1_sequence(dn_data)
                oid = DistinguishedNameType(unpack_asn1_object_identifier(dn_oid))

                dns.append(
                    {
                        "OID": parse_enum(oid),
                        "Value": dn_str.b_data.tobytes().decode("utf-8"),
                    }
                )

    return {
        "CertificateTypes": cert_types,
        "SignatureAlgorithms": sig_algos,
        "CertificateAuthorities": dns,
    }


def _parse_tls_handshake_server_key_exchange(
    view: memoryview,
    protocol_version: TlsProtocolVersion,
) -> typing.Dict[str, typing.Any]:
    curve_type = TlsECCurveType(struct.unpack("B", view[:1])[0])
    view = view[1:]

    curve = TlsSupportedGroup(struct.unpack(">H", view[:2])[0])
    view = view[2:]

    pubkey_len = struct.unpack("B", view[:1])[0]
    view = view[1:]

    pubkey = view[:pubkey_len].tobytes()
    view = view[pubkey_len:]

    signature_algo = None
    if protocol_version >= TlsProtocolVersion.tls1_2:
        signature_algo = TlsSignatureScheme(struct.unpack(">H", view[:2])[0])
        view = view[2:]

    signature_len = struct.unpack(">H", view[:2])[0]
    view = view[2:]

    signature = view[:signature_len].tobytes()

    return {
        "CurveType": parse_enum(curve_type),
        "Curve": parse_enum(curve),
        "PublicKey": base64.b16encode(pubkey).decode(),
        "SignatureAlgorithm": parse_enum(signature_algo) if signature_algo else None,
        "Signature": base64.b16encode(signature).decode(),
    }


def _parse_tls_extensions(
    view: memoryview,
    is_client_hello: bool,
) -> typing.List[typing.Dict[str, typing.Any]]:
    extensions = []

    while view:
        ext_type = TlsExtensionType(struct.unpack(">H", view[:2])[0])
        view = view[2:]

        ext_len = struct.unpack(">H", view[:2])[0]
        view = view[2:]

        ext_data = view[:ext_len]
        view = view[ext_len:]

        data: typing.Any = None
        if ext_len == 0:
            data = None

        elif ext_type == TlsExtensionType.server_name:
            data_len = struct.unpack(">H", ext_data[:2])[0]
            data_view = ext_data[2 : 2 + data_len]
            data = []
            while data_view:
                name_type = TlsServerNameType(struct.unpack("B", data_view[:1])[0])
                name_len = struct.unpack(">H", data_view[1:3])[0]
                name = data_view[3 : 3 + name_len].tobytes().decode("utf-8")
                data.append(
                    {
                        "Type": parse_enum(name_type),
                        "Name": name,
                    }
                )
                data_view = data_view[3 + name_len :]

        elif ext_type == TlsExtensionType.ec_point_formats:
            data_len = struct.unpack("B", ext_data[:1])[0]
            data_view = ext_data[1 : 1 + data_len]
            data = [parse_enum(TlsECPointFormat(b)) for b in list(data_view)]

        elif ext_type == TlsExtensionType.supported_groups:
            data_len = struct.unpack(">H", ext_data[:2])[0]
            data_view = ext_data[2 : 2 + data_len]
            data = []
            while data_view:
                data.append(parse_enum(TlsSupportedGroup(struct.unpack(">H", data_view[:2])[0])))
                data_view = data_view[2:]

        elif ext_type == TlsExtensionType.application_layer_protocol_negotiation:
            data_len = struct.unpack(">H", ext_data[:2])[0]
            data_view = ext_data[2 : 2 + data_len]
            data = []
            while data_view:
                alpn_len = struct.unpack("B", data_view[:1])[0]
                data_view = data_view[1:]
                data.append(data_view[:alpn_len].tobytes().decode())
                data_view = data_view[alpn_len:]

        elif ext_type == TlsExtensionType.session_ticket:
            data = base64.b16encode(ext_data.tobytes()).decode()

        elif ext_type == TlsExtensionType.signature_algorithms:
            data_len = struct.unpack(">H", ext_data[:2])[0]
            data_view = ext_data[2 : 2 + data_len]
            data = []
            while data_view:
                data.append(parse_enum(TlsSignatureScheme(struct.unpack(">H", data_view[:2])[0])))
                data_view = data_view[2:]

        elif ext_type == TlsExtensionType.supported_versions:
            if is_client_hello:
                data_len = struct.unpack("B", ext_data[:1])[0]
                data_view = ext_data[1 : 1 + data_len]
                data = []
                while data_view:
                    data.append(parse_enum(TlsProtocolVersion(struct.unpack(">H", data_view[:2])[0])))
                    data_view = data_view[2:]

            else:
                data = parse_enum(TlsProtocolVersion(struct.unpack(">H", ext_data[:2])[0]))

        elif ext_type == TlsExtensionType.psk_key_exchange_modes:
            data_len = struct.unpack("B", ext_data[:1])[0]
            data_view = ext_data[1 : 1 + data_len]
            data = []
            while data_view:
                data.append(parse_enum(TlsPskKeyExchangeMode(struct.unpack("B", data_view[:1])[0])))
                data_view = data_view[1:]

        elif ext_type == TlsExtensionType.key_share:
            if is_client_hello:
                data_len = struct.unpack(">H", ext_data[:2])[0]
                data_view = ext_data[2 : 2 + data_len]
                data = []
                while data_view:
                    key_share_group = TlsSupportedGroup(struct.unpack(">H", data_view[:2])[0])
                    key_exchange_len = struct.unpack(">H", data_view[2:4])[0]
                    key_exchange = data_view[4 : 4 + key_exchange_len].tobytes()

                    data.append(
                        {
                            "Group": parse_enum(key_share_group),
                            "Key": base64.b16encode(key_exchange).decode(),
                        }
                    )
                    data_view = data_view[4 + key_exchange_len :]
            else:
                key_share_group = TlsSupportedGroup(struct.unpack(">H", ext_data[:2])[0])
                key_exchange_len = struct.unpack(">H", ext_data[2:4])[0]
                key_exchange = ext_data[4 : 4 + key_exchange_len].tobytes()
                data = {
                    "Group": parse_enum(key_share_group),
                    "Key": base64.b16encode(key_exchange).decode(),
                }

        formated_data = {
            "ExtensionType": parse_enum(ext_type),
        }
        if data is not None:
            formated_data["Data"] = data
        if data is not None or ext_data:
            formated_data["RawData"] = base64.b16encode(ext_data).decode()
        extensions.append(formated_data)

    return extensions


def main(args: typing.List[str]) -> None:
    """Main program entry point."""
    parsed_args = parse_args(args)

    if parsed_args.token:
        b_data = to_bytes(parsed_args.token)
    else:
        if parsed_args.file:
            file_path = os.path.abspath(os.path.expanduser(os.path.expandvars(parsed_args.file)))
            b_file_path = to_bytes(file_path)
            if not os.path.exists(b_file_path):
                raise ValueError("Cannot find file at path '%s'" % file_path)

            with open(b_file_path, mode="rb") as fd:
                b_data = fd.read()
        else:
            b_data = sys.stdin.buffer.read()

    if re.match(b"^[a-fA-F0-9\\s]+$", b_data):
        # Input data was a hex string.
        b_data = base64.b16decode(re.sub(b"[\\s]", b"", b_data.strip().upper()))
    if re.match(b"^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$", b_data):
        # Input data was a base64 string.
        b_data = base64.b64decode(b_data.strip())

    token_info: typing.Union[typing.List, typing.Dict]
    if re.match(b"[\x00|\x14|\x15|\x16|\x17]\x03[\x01|\x02|\x03]", b_data):
        token_info = parse_tls_token(b_data)
    else:
        token_info = parse_token(b_data, secret=parsed_args.secret, encoding=parsed_args.encoding)

    if parsed_args.output_format == "yaml" and HAS_YAML:
        y = yaml.YAML()
        y.default_flow_style = False
        y.dump(token_info, sys.stdout)
    else:
        print(json.dumps(token_info, indent=4))


def parse_args(args: typing.List[str]) -> argparse.Namespace:
    """Parse and return args."""
    parser = argparse.ArgumentParser(description="Parse Microsoft authentication tokens into a human readable format.")

    data = parser.add_mutually_exclusive_group()

    data.add_argument(
        "-t", "--token", dest="token", help="Raw base64 encoded or hex string token as a command line argument."
    )

    data.add_argument(
        "-f",
        "--file",
        default="",
        dest="file",
        help="Path to file that contains raw bytes, base64, or hex string of token to parse, Defaults to reading from "
        "stdin if neither -t or -f is specified.",
    )

    parser.add_argument(
        "--encoding",
        dest="encoding",
        help="The encoding to use when trying to decode text fields from bytes in tokens that don't have a negotiated "
        "encoding. This defaults to 'windows-1252' for NTLM tokens and 'utf-8' for Kerberos/SPNEGO tokens.",
    )

    parser.add_argument(
        "--format",
        "--output-format",
        choices=["json", "yaml"],
        default="json",
        dest="output_format",
        type=lambda s: s.lower(),
        help="Set the output format of the token, default is (json). Using yaml requires the ruamel.yaml Python "
        "library to be installed pip install pyspnego[yaml].",
    )

    parser.add_argument(
        "--secret",
        "--password",
        dest="secret",
        default=None,
        help="Optional info that is the secret information for a protocol that can be used to decrypt encrypted "
        "fields and/or derive the unique session key in the exchange. This is currently only supported by NTLM "
        "tokens to generate the session key.",
    )

    if HAS_ARGCOMPLETE:
        argcomplete.autocomplete(parser)

    parsed_args = parser.parse_args(args)

    if parsed_args.output_format == "yaml" and not HAS_YAML:
        raise ValueError("Cannot output as yaml as ruamel.yaml is not installed.")

    return parsed_args


def parse_token(
    b_data: bytes,
    secret: typing.Optional[str] = None,
    encoding: typing.Optional[str] = None,
    mech: typing.Optional[typing.Union[str, GSSMech]] = None,
) -> typing.Dict[str, typing.Any]:
    """
    :param b_data: A byte string of the token to parse. This can be a NTLM or GSSAPI (SPNEGO/Kerberos) token.
    :param secret: The secret data used to decrypt fields and/or derive session keys.
    :param encoding: The encoding to use for token fields that represent text. This is only used for fields where there
        is no negotiation for the encoding of that particular field. Defaults to 'windows-1252' for NTLM and 'utf-8'
        for Kerberos.
    :return: A dict containing the parsed token data.
    """
    gss_mech: typing.Optional[GSSMech] = None
    if mech and not isinstance(mech, GSSMech):
        gss_mech = GSSMech.from_oid(mech)

    try:
        token = unpack_token(b_data, mech=gss_mech, unwrap=True, encoding=encoding)
    except Exception as e:
        return {
            "MessageType": "Unknown - Failed to parse see Data for more details.",
            "Data": "Failed to parse token: %s" % str(e),
            "RawData": base64.b16encode(b_data).decode(),
        }

    msg_type = "Unknown"
    data: typing.Union[str, typing.Dict[str, typing.Any]] = "Failed to parse SPNEGO token due to unknown mech type"

    # SPNEGO messages.
    if isinstance(token, InitialContextToken):
        msg_type = "SPNEGO InitialContextToken"
        data = {
            "thisMech": parse_enum(token.this_mech, enum_type=GSSMech),
            "innerContextToken": parse_token(
                token.inner_context_token, mech=token.this_mech, secret=secret, encoding=encoding
            ),
        }

    elif isinstance(token, NegTokenInit):
        data = _parse_spnego_init(token, secret, encoding)
        if "negHints" in data:
            msg_type = "SPNEGO NegTokenInit2"

        else:
            msg_type = "SPNEGO NegTokenInit"

    elif isinstance(token, NegTokenResp):
        msg_type = "SPNEGO NegTokenResp"
        data = _parse_spnego_resp(token, secret, encoding)

    # NTLM messages.
    elif isinstance(token, (Negotiate, Challenge, Authenticate)):
        msg_type = parse_enum(token.MESSAGE_TYPE)

        if isinstance(token, Negotiate):
            data = _parse_ntlm_negotiate(token)

        elif isinstance(token, Challenge):
            data = _parse_ntlm_challenge(token)

        else:
            data = _parse_ntlm_authenticate(token, secret)

    # Kerberos messages.
    elif isinstance(token, KerberosV5Msg):
        msg_type = parse_enum(token.MESSAGE_TYPE)
        data = parse_kerberos_token(token, secret, encoding)

    return {
        "MessageType": msg_type,
        "Data": data,
        "RawData": base64.b16encode(b_data).decode(),
    }


def parse_tls_token(
    b_data: bytes,
) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    :param b_data: A byte string of the TLS token to parse.
    :return: A dict containing the parsed TLS token data.
    """
    view = memoryview(b_data)

    res = []
    while view:
        content_type = TlsContentType(struct.unpack("B", view[:1])[0])
        protocol_version = TlsProtocolVersion(struct.unpack(">H", view[1:3])[0])
        token_length = struct.unpack(">H", view[3:5])[0]
        token_view = view[5 : 5 + token_length]

        data: typing.Optional[typing.List[typing.Dict[str, typing.Any]]] = None
        if content_type == TlsContentType.handshake:
            data = []

            while token_view:
                handshake_type = TlsHandshakeMessageType(struct.unpack("B", token_view[:1])[0])
                message_len = struct.unpack(">L", b"\x00" + token_view[1:4])[0]
                handshake_view = token_view[4 : 4 + message_len]

                handshake_data: typing.Optional[typing.Dict[str, typing.Any]] = None
                if handshake_type == TlsHandshakeMessageType.client_hello:
                    handshake_data = _parse_tls_handshake_client_hello(handshake_view)

                elif handshake_type == TlsHandshakeMessageType.server_hello:
                    handshake_data = _parse_tls_handshake_server_hello(handshake_view)

                elif handshake_type == TlsHandshakeMessageType.certificate:
                    cert_len = struct.unpack(">I", b"\x00" + handshake_view[:3])[0]
                    handshake_data = {
                        "Certificate": base64.b16encode(handshake_view[3 : 3 + cert_len].tobytes()).decode(),
                    }

                elif handshake_type == TlsHandshakeMessageType.certificate_request:
                    handshake_data = _parse_tls_handshake_certificate_request(handshake_view)

                elif handshake_type == TlsHandshakeMessageType.server_key_exchange:
                    handshake_data = _parse_tls_handshake_server_key_exchange(handshake_view, protocol_version)

                elif handshake_type == TlsHandshakeMessageType.client_key_exchange:
                    key_len = struct.unpack("B", handshake_view[:1])[0]
                    handshake_data = {
                        "PublicKey": base64.b16encode(handshake_view[1 : 1 + key_len].tobytes()).decode(),
                    }

                formatted_handshake_data: typing.Dict[str, typing.Any] = {
                    "HandshakeType": parse_enum(handshake_type),
                }
                if handshake_data is not None:
                    formatted_handshake_data["Data"] = handshake_data
                formatted_handshake_data["RawData"] = base64.b16encode(token_view[: 4 + message_len].tobytes()).decode()
                data.append(formatted_handshake_data)
                token_view = token_view[4 + message_len :]

        formatted_data: typing.Dict[str, typing.Any] = {
            "ContentType": parse_enum(content_type),
            "ProtocolVersion": parse_enum(protocol_version),
        }
        if data is not None:
            formatted_data["Data"] = data

        formatted_data["RawData"] = base64.b16encode(view[: 5 + token_length].tobytes()).decode()

        res.append(formatted_data)
        view = view[5 + token_length :]

    return res


if __name__ == "__main__":  # pragma: nocover
    main(sys.argv[1:])
