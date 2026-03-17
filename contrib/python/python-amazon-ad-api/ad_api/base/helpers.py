from io import BytesIO
from typing import TypeVar

from Crypto.Util.Padding import pad
import hashlib

import base64
from Crypto.Cipher import AES


def fill_query_params(query, *args):
    return query.format(*args)


T = TypeVar("T")


def sp_endpoint(path, method="GET"):
    def decorator(function: T) -> T:
        def wrapper(*args, **kwargs):
            kwargs.update({"path": path, "method": method})
            return function(*args, **kwargs)

        wrapper.__doc__ = function.__doc__
        return wrapper

    return decorator


def encrypt_aes(file_or_bytes_io, key, iv):
    key = base64.b64decode(key)
    iv = base64.b64decode(iv)
    aes = AES.new(key, AES.MODE_CBC, iv)
    try:
        if isinstance(file_or_bytes_io, BytesIO):
            return aes.encrypt(pad(file_or_bytes_io.read(), 16))
        return aes.encrypt(pad(bytes(file_or_bytes_io.read(), encoding="iso-8859-1"), 16))
    except UnicodeEncodeError:
        file_or_bytes_io.seek(0)
        return aes.encrypt(pad(bytes(file_or_bytes_io.read(), encoding="utf-8"), 16))
    except TypeError:
        file_or_bytes_io.seek(0)
        return aes.encrypt(pad(file_or_bytes_io.read(), 16))


def decrypt_aes(content, key, iv):
    key = base64.b64decode(key)
    iv = base64.b64decode(iv)
    decrypter = AES.new(key, AES.MODE_CBC, iv)
    decrypted = decrypter.decrypt(content)
    padding_bytes = decrypted[-1]
    return decrypted[:-padding_bytes]


def create_md5(file):
    hash_md5 = hashlib.md5()
    if isinstance(file, BytesIO):
        for chunk in iter(lambda: file.read(4096), b""):
            hash_md5.update(chunk)
        file.seek(0)
        return hash_md5.hexdigest()
    if isinstance(file, str):
        with open(file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    for chunk in iter(lambda: file.read(4096), b""):
        hash_md5.update(chunk)
    return hash_md5.hexdigest()


def nest_dict(flat: dict()):
    result = {}
    for k, v in flat.items():
        _nest_dict_rec(k, v, result)
    return result


def _nest_dict_rec(k, v, out):
    k, *rest = k.split(".", 1)
    if rest:
        _nest_dict_rec(rest[0], v, out.setdefault(k, {}))
    else:
        out[k] = v
