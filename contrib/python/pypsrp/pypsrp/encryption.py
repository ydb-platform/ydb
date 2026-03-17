import logging
import re
import struct
import typing

from pypsrp._utils import to_bytes
from pypsrp.exceptions import WinRMError

log = logging.getLogger(__name__)


class WinRMEncryption(object):
    SIXTEEN_KB = 16384
    MIME_BOUNDARY = "--Encrypted Boundary"
    CREDSSP = "application/HTTP-CredSSP-session-encrypted"
    KERBEROS = "application/HTTP-Kerberos-session-encrypted"
    SPNEGO = "application/HTTP-SPNEGO-session-encrypted"

    def __init__(self, context: typing.Any, protocol: str) -> None:
        log.debug("Initialising WinRMEncryption helper for protocol %s" % protocol)
        self.context = context
        self.protocol = protocol

    def wrap_message(self, message: bytes) -> typing.Tuple[str, bytes]:
        log.debug("Wrapping message")
        if self.protocol == self.CREDSSP and len(message) > self.SIXTEEN_KB:
            content_type = "multipart/x-multi-encrypted"
            encrypted_msg = b""
            chunks = [message[i : i + self.SIXTEEN_KB] for i in range(0, len(message), self.SIXTEEN_KB)]
            for chunk in chunks:
                encrypted_chunk = self._wrap_message(chunk)
                encrypted_msg += encrypted_chunk
        else:
            content_type = "multipart/encrypted"
            encrypted_msg = self._wrap_message(message)

        encrypted_msg += to_bytes("%s--\r\n" % self.MIME_BOUNDARY)

        log.debug("Created wrapped message of content type %s" % content_type)
        return content_type, encrypted_msg

    def unwrap_message(self, message: bytes, boundary: str) -> bytes:
        log.debug("Unwrapped message")

        # Talking to Exchange endpoints gives a non-compliant boundary that has a space between the -- {boundary}, not
        # ideal but we just need to handle it.
        parts = re.compile(to_bytes(r"--\s*%s\r\n" % re.escape(boundary))).split(message)
        parts = list(filter(None, parts))

        message = b""
        for i in range(0, len(parts), 2):
            header = parts[i].strip()
            payload = parts[i + 1]

            expected_length = int(header.split(b"Length=")[1])

            # remove the end MIME block if it exists
            payload = re.sub(to_bytes(r"--\s*%s--\r\n$") % to_bytes(boundary), b"", payload)

            wrapped_data = payload.replace(b"\tContent-Type: application/octet-stream\r\n", b"")
            header_length = struct.unpack("<i", wrapped_data[:4])[0]
            header = wrapped_data[4 : 4 + header_length]
            enc_wrapped_data = wrapped_data[4 + header_length :]

            unwrapped_data = self.context.unwrap_winrm(header, enc_wrapped_data)

            actual_length = len(unwrapped_data)

            log.debug("Actual unwrapped length: %d, expected unwrapped length: %d" % (actual_length, expected_length))
            if actual_length != expected_length:
                raise WinRMError(
                    "The encrypted length from the server does "
                    "not match the expected length, decryption "
                    "failed, actual: %d != expected: %d" % (actual_length, expected_length)
                )
            message += unwrapped_data

        return message

    def _wrap_message(self, message: bytes) -> bytes:
        header, wrapped_data, padding_length = self.context.wrap_winrm(message)
        wrapped_data = struct.pack("<i", len(header)) + header + wrapped_data

        msg_length = str(len(message) + padding_length)

        payload = "\r\n".join(
            [
                self.MIME_BOUNDARY,
                "\tContent-Type: %s" % self.protocol,
                "\tOriginalContent: type=application/soap+xml;charset=UTF-8;Length=%s" % msg_length,
                self.MIME_BOUNDARY,
                "\tContent-Type: application/octet-stream",
                "",
            ]
        )
        return to_bytes(payload) + wrapped_data
