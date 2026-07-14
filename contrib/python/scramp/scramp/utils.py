import binascii
import hmac as hmaca
from base64 import b64decode, b64encode

from scramp.exceptions import ScramException


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
    except binascii.Error as e:
        raise ScramException("Invalid base64 encoding.") from e


def uenc(string):
    return string.encode("utf-8")
