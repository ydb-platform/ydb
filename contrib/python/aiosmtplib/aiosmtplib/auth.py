"""
Authentication related methods.
"""

import base64
import hmac

__all__ = (
    "auth_crammd5_verify",
    "auth_login_encode",
    "auth_plain_encode",
    "auth_xoauth2_encode",
)


def _ensure_bytes(value: str | bytes) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value

    return value.encode("utf-8")


def auth_crammd5_verify(
    username: str | bytes,
    password: str | bytes,
    challenge: str | bytes,
    /,
) -> bytes:
    """
    CRAM-MD5 auth uses the password as a shared secret to MD5 the server's
    response, and sends the username combined with that (base64 encoded).
    """
    username_bytes = _ensure_bytes(username)
    password_bytes = _ensure_bytes(password)
    decoded_challenge = base64.b64decode(challenge)

    md5_digest = hmac.new(password_bytes, msg=decoded_challenge, digestmod="md5")
    verification = username_bytes + b" " + md5_digest.hexdigest().encode("ascii")
    encoded_verification = base64.b64encode(verification)

    return encoded_verification


def auth_plain_encode(
    username: str | bytes,
    password: str | bytes,
    /,
) -> bytes:
    """
    PLAIN auth base64 encodes the username and password together.
    """
    username_bytes = _ensure_bytes(username)
    password_bytes = _ensure_bytes(password)

    username_and_password = b"\0" + username_bytes + b"\0" + password_bytes
    encoded = base64.b64encode(username_and_password)

    return encoded


def auth_login_encode(
    username: str | bytes,
    password: str | bytes,
    /,
) -> tuple[bytes, bytes]:
    """
    LOGIN auth base64 encodes the username and password and sends them
    in sequence.
    """
    username_bytes = _ensure_bytes(username)
    password_bytes = _ensure_bytes(password)

    encoded_username = base64.b64encode(username_bytes)
    encoded_password = base64.b64encode(password_bytes)

    return encoded_username, encoded_password


def auth_xoauth2_encode(
    username: str | bytes,
    access_token: str | bytes,
    /,
) -> bytes:
    """
    XOAUTH2 auth encodes the username and OAuth2 access token per
    https://developers.google.com/gmail/imap/xoauth2-protocol

    Format: base64("user=" + username + "\\x01auth=Bearer " + token + "\\x01\\x01")
    """
    username_bytes = _ensure_bytes(username)
    token_bytes = _ensure_bytes(access_token)

    auth_string = (
        b"user=" + username_bytes + b"\x01auth=Bearer " + token_bytes + b"\x01\x01"
    )
    encoded = base64.b64encode(auth_string)

    return encoded
