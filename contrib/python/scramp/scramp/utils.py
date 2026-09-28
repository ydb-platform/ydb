import hmac as hmaca
from base64 import b64decode, b64encode

SERVER_ERROR_INVALID_ENCODING = "invalid-encoding"
SERVER_ERROR_EXTENSIONS_NOT_SUPPORTED = "extensions-not-supported"
SERVER_ERROR_INVALID_PROOF = "invalid-proof"
SERVER_ERROR_CHANNEL_BINDINGS_DONT_MATCH = "channel-bindings-dont-match"
SERVER_ERROR_SERVER_DOES_SUPPORT_CHANNEL_BINDING = "server-does-support-channel-binding"
SERVER_ERROR_CHANNEL_BINDING_NOT_SUPPORTED = "channel-binding-not-supported"
SERVER_ERROR_UNSUPPORTED_CHANNEL_BINDING_TYPE = "unsupported-channel-binding-type"
SERVER_ERROR_UNKNOWN_USER = "unknown-user"
SERVER_ERROR_INVALID_USERNAME_ENCODING = "invalid-username-encoding"
SERVER_ERROR_NO_RESOURCES = "no-resources"
SERVER_ERROR_OTHER_ERROR = "other-error"


class ScramException(Exception):
    def __init__(self, message, server_error=None):
        super().__init__(message)
        self.server_error = server_error

    def __str__(self):
        s_str = "" if self.server_error is None else f": {self.server_error}"
        return super().__str__() + s_str


def hmac(hf, key, msg):
    return hmaca.new(key, msg=msg, digestmod=hf).digest()


def h(hf, msg):
    return hf(msg).digest()


def xor(bytes1, bytes2):
    return bytes(a ^ b for a, b in zip(bytes1, bytes2, strict=True))


def b64enc(binary):
    return b64encode(binary).decode("utf8")


def b64dec(string):
    try:
        return b64decode(string, validate=True)
    except BaseException as e:
        raise ScramException(
            f"Invalid base 64 encoding '{string}'", SERVER_ERROR_INVALID_ENCODING
        ) from e


def uenc(string):
    return string.encode("utf-8")


class IterationCount(int):
    def __new__(cls, value, minimum, maximum):
        if value is None:
            value = minimum
        if value < minimum:
            raise ValueError(f"The value must not be < {minimum}")
        if value > maximum:
            raise ValueError(f"The value must not be > {maximum}")
        return super().__new__(cls, value)
