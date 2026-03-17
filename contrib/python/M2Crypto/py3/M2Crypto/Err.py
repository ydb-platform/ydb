from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL Error API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

from M2Crypto import BIO, m2, util  # noqa
from typing import Optional  # noqa


def get_error() -> Optional[str]:
    err = BIO.MemoryBuffer()
    m2.err_print_errors(err.bio_ptr())
    err_msg = err.read()
    if err_msg:
        return err_msg.decode()


def get_error_code() -> int:
    return m2.err_get_error()


def peek_error_code() -> int:
    return m2.err_peek_error()


def get_error_lib(err: Optional[int]) -> str:
    err_str = m2.err_lib_error_string(err)
    return err_str.decode() if err_str else ''


def get_error_func(err: Optional[int]) -> str:
    err_str = m2.err_func_error_string(err)
    return err_str if err_str else ''


def get_error_reason(err: Optional[int]) -> str:
    err_str = m2.err_reason_error_string(err)
    return err_str if err_str else ''


def get_error_message() -> str:
    return get_error_reason(get_error_code())


def get_x509_verify_error(err: Optional[int]) -> str:
    err_str = m2.x509_get_verify_error(err)
    return err_str if err_str else ''


class SSLError(Exception):
    def __init__(self, err: int, client_addr: util.AddrType) -> None:
        self.err = err
        self.client_addr = client_addr

    def __str__(self) -> str:
        if not isinstance(self.client_addr, str):
            s = self.client_addr.decode()
        else:
            s = self.client_addr
        return "%s: %s: %s" % (
            get_error_func(self.err),
            s,
            get_error_reason(self.err),
        )


class M2CryptoError(Exception):
    pass
