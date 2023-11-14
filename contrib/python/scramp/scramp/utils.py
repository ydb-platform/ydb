import hmac as hmaca
from base64 import b64decode, b64encode


def hmac(hf, key, msg):
    return hmaca.new(key, msg=msg, digestmod=hf).digest()


def h(hf, msg):
    return hf(msg).digest()


def hi(hf, password, salt, iterations):
    u = ui = hmac(hf, password, salt + b"\x00\x00\x00\x01")
    for i in range(iterations - 1):
        ui = hmac(hf, password, ui)
        u = xor(u, ui)
    return u


def xor(bytes1, bytes2):
    return bytes(a ^ b for a, b in zip(bytes1, bytes2))


def b64enc(binary):
    return b64encode(binary).decode("utf8")


def b64dec(string):
    return b64decode(string)


def uenc(string):
    return string.encode("utf-8")
