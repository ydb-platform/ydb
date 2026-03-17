from __future__ import absolute_import

"""M2Crypto wrapper for OpenSSL Error API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

from M2Crypto import BIO, m2, util, six  # noqa
from typing import Optional  # noqa


def get_error():
    # type: () -> Optional[str]
    err = BIO.MemoryBuffer()
    m2.err_print_errors(err.bio_ptr())
    err_msg = err.read()
    if err_msg:
        return six.ensure_text(err_msg)


def get_error_code():
    # type: () -> int
    return m2.err_get_error()


def peek_error_code():
    # type: () -> int
    return m2.err_peek_error()


def get_error_lib(err):
    # type: (Optional[int]) -> str
    err_str = m2.err_lib_error_string(err)
    return six.ensure_text(err_str) if err_str else ''


def get_error_func(err):
    # type: (Optional[int]) -> str
    err_str = m2.err_func_error_string(err)
    return six.ensure_text(err_str) if err_str else ''


def get_error_reason(err):
    # type: (Optional[int]) -> str
    err_str = m2.err_reason_error_string(err)
    return six.ensure_text(err_str) if err_str else ''


def get_error_message():
    # type: () -> str
    return six.ensure_text(get_error_reason(get_error_code()))


def get_x509_verify_error(err):
    # type: (Optional[int]) -> str
    err_str = m2.x509_get_verify_error(err)
    return six.ensure_text(err_str) if err_str else ''


class SSLError(Exception):
    def __init__(self, err, client_addr):
        # type: (int, util.AddrType) -> None
        self.err = err
        self.client_addr = client_addr

    def __str__(self):
        # type: () -> str
        if not isinstance(self.client_addr, six.text_type):
            s = self.client_addr.decode('utf8')
        else:
            s = self.client_addr
        return "%s: %s: %s" % (get_error_func(self.err), s,
                               get_error_reason(self.err))


class M2CryptoError(Exception):
    pass
