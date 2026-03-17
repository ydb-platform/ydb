# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import struct
import typing

from spnego._ntlm_raw.crypto import RC4Handle, crc32, hmac_md5, rc4
from spnego._ntlm_raw.messages import NegotiateFlags
from spnego.exceptions import OperationNotAvailableError


def seal(
    flags: int,
    handle: RC4Handle,
    signing_key: bytes,
    seq_num: int,
    b_data: bytes,
    *,
    to_sign: typing.Optional[bytes] = None,
) -> typing.Tuple[bytes, bytes]:
    """Create a sealed NTLM message.

    Creates a sealed NTLM message as documented at `NTLM Message Confidentiality`_.

    Args:
        flags: The negotiated flags between the initiator and acceptor.
        handle: The RC4 handle for the negotiated context.
        signing_key: The key used to sign the message.
        seq_num: The sequence number for the message.
        b_data: The data/message bytes to seal.

    Returns:
        Tuple[bytes, bytes]: The sealed message bytes and the message signature.

    .. _NTLM Message Confidentiality:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/115f9c7d-bc30-4262-ae96-254555c14ea6
    """
    seal_msg = rc4(handle, b_data)
    signature = sign(flags, handle, signing_key, seq_num, to_sign if to_sign else b_data)
    return seal_msg, signature


def sign(
    flags: int,
    handle: RC4Handle,
    signing_key: bytes,
    seq_num: int,
    b_data: bytes,
) -> bytes:
    """Create a NTLM signature.

    Creates a NTLM signature as documented at `NTLM Message Integrity`_ and appends it to the end of the message.

    Args:
        flags: The negotiated flags between the initiator and acceptor.
        handle: The RC4 handle for the negotiated context.
        signing_key: The key used to sign the message.
        seq_num: The sequence number for the signature.
        b_data: The data/message bytes to sign.

    Returns:
        bytes: The data with the signature appended.

    .. _NTLM Message Integrity:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/131b0062-7958-460e-bca5-c7a9f9086652
    """
    if flags & NegotiateFlags.sign == 0:
        if flags & NegotiateFlags.always_sign == 0:
            raise OperationNotAvailableError(context_msg="Signing without integrity.")

        # This is the behaviour seen with SSPI when signing data with NTLMSSP_NEGOTIATE_ALWAYS_SIGN.
        return b"\x01" + b"\x00" * 15

    elif flags & NegotiateFlags.extended_session_security:
        return _mac_with_ess(flags, handle, signing_key, seq_num, b_data)

    else:
        return _mac_without_ess(handle, seq_num, b_data)


def _mac_without_ess(
    handle: RC4Handle,
    seq_num: int,
    b_data: bytes,
) -> bytes:
    """NTLM MAC without Extended Session Security

    Generates the NTLM signature when Extended Session Security has not been negotiated. The structure of the signature
    is documented at `NTLM signature without ESS`_.

    The algorithm as documented by `MAC without ESS`_ is::

        Define MAC(Handle, SigningKey, SeqNum, Message) as
            Set NTLMSSP_MESSAGE_SIGNATURE.Version to 0x00000001
            Set NTLMSSP_MESSAGE_SIGNATURE.Checksum to CRC32(Message)
            Set NTLMSSP_MESSAGE_SIGNATURE.RandomPad RC4(Handle, RandomPad)
            Set NTLMSSP_MESSAGE_SIGNATURE.Checksum to RC4(Handle, NTLMSSP_MESSAGE_SIGNATURE.Checksum)
            Set NTLMSSP_MESSAGE_SIGNATURE.SeqNum to RC4(Handle, 0x00000000)

            If (connection oriented)
                Set NTLMSSP_MESSAGE_SIGNATURE.SeqNum to NTLMSSP_MESSAGE_SIGNATURE.SeqNum XOR SeqNum
                Set SeqNum to SeqNum + 1

            Else
                Set NTLMSSP_MESSAGE_SIGNATURE.SeqNum to NTLMSSP_MESSAGE_SIGNATURE.SeqNum XOR (app supplied SeqNum)

            Endif

            Set NTLMSSP_MESSAGE_SIGNATURE.RandomPad to 0

        EndDefine

    Args:
        handle: The RC4 handle for the negotiated context.
        seq_num: The sequence number for the signature.
        b_data: The data/message bytes to sign.

    Returns:
        bytes: The NTLM signature.

    .. _NTLM signature without ESS:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/83fbd0e7-8ab0-4873-8cbe-795249b46b8a

    .. _MAC without ESS:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/0b1fb6a6-7224-4d5b-af35-fdd45c0791e5
    """
    checksum = crc32(b_data)
    rc4(handle, b"\x00\x00\x00\x00")  # NTLMSSP_MESSAGE_SIGNATURE.RandomPad RC4(Handle, RandomPad)
    checksum = rc4(handle, checksum)

    temp_seq_num = struct.unpack("<I", rc4(handle, b"\x00\x00\x00\x00"))[0]
    b_seq_num = struct.pack("<I", temp_seq_num ^ seq_num)

    return b"\x01\x00\x00\x00" + b"\x00\x00\x00\x00" + checksum + b_seq_num


def _mac_with_ess(flags: int, handle: RC4Handle, signing_key: bytes, seq_num: int, b_data: bytes) -> bytes:
    """NTLM MAC with Extended Session Security

    Generates the NTLM signature when Extended Session Security has been negotiated. The structure of the signature is
    documented at `NTLM signature with ESS`_.

    The algorithm as documented by `MAC with ESS`_ is::

        Define MAC(Handle, SigningKey, SeqNum, Message) as
            Set NTLMSSP_MESSAGE_SIGNATURE.Version to 0x00000001
            Set NTLMSSP_MESSAGE_SIGNATURE.Checksum to HMAC_MD5(SigningKey, ConcatenationOf(SeqNum, Message))[0..7]
            Set NTLMSSP_MESSAGE_SIGNATURE.SeqNum to SeqNum
            Set SeqNum to SeqNum + 1
        EndDefine

        # When NegotiateFlags.key_exch

        Define MAC(Handle, SigningKey, SeqNum, Message) as
            Set NTLMSSP_MESSAGE_SIGNATURE.Version to 0x00000001
            Set NTLMSSP_MESSAGE_SIGNATURE.Checksum to RC4(Handle,
                HMAC_MD5(SigningKey, ConcatenationOf(SeqNum, Message))[0..7])
            Set NTLMSSP_MESSAGE_SIGNATURE.SeqNum to SeqNum
            Set SeqNum to SeqNum + 1
        EndDefine

    Args:
        flags: The negotiated flags between the initiator and acceptor.
        handle: The RC4 handle for the negotiated context.
        signing_key: The key used to sign the message.
        seq_num: The sequence number for the signature.
        b_data: The data/message bytes to sign.

    Returns:
        bytes: The NTLM with ESS signature.

    .. _NTLM signature with ESS:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/2c3b4689-d6f1-4dc6-85c9-0bf01ea34d9f

    .. _MAC with ESS:
        https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/a92716d5-d164-4960-9e15-300f4eef44a8
    """
    b_seq_num = struct.pack("<I", seq_num)

    checksum = hmac_md5(signing_key, b_seq_num + b_data)[:8]
    if flags & NegotiateFlags.key_exch:
        checksum = handle.update(checksum)

    return b"\x01\x00\x00\x00" + checksum + b_seq_num
