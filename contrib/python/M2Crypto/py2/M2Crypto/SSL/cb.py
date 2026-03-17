from __future__ import absolute_import

"""SSL callbacks

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

import sys

from M2Crypto import m2
from typing import Any  # noqa

__all__ = ['unknown_issuer', 'ssl_verify_callback_stub', 'ssl_verify_callback',
           'ssl_verify_callback_allow_unknown_ca', 'ssl_info_callback']


def ssl_verify_callback_stub(ssl_ctx_ptr, x509_ptr, errnum, errdepth, ok):
    # Deprecated
    return ok


unknown_issuer = [
    m2.X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT,
    m2.X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY,
    m2.X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE,
    m2.X509_V_ERR_CERT_UNTRUSTED,
]


def ssl_verify_callback(ssl_ctx_ptr, x509_ptr, errnum, errdepth, ok):
    # type: (bytes, bytes, int, int, int) -> int
    # Deprecated

    from M2Crypto.SSL.Context import Context
    ssl_ctx = Context.ctxmap()[int(ssl_ctx_ptr)]
    if errnum in unknown_issuer:
        if ssl_ctx.get_allow_unknown_ca():
            sys.stderr.write("policy: %s: permitted...\n" %
                             (m2.x509_get_verify_error(errnum)))
            sys.stderr.flush()
            ok = 1
    # CRL checking goes here...
    if ok:
        if ssl_ctx.get_verify_depth() >= errdepth:
            ok = 1
        else:
            ok = 0
    return ok


def ssl_verify_callback_allow_unknown_ca(ok, store):
    # type: (int, Any) -> int
    errnum = store.get_error()
    if errnum in unknown_issuer:
        ok = 1
    return ok


# Cribbed from OpenSSL's apps/s_cb.c.
def ssl_info_callback(where, ret, ssl_ptr):
    # type: (int, int, bytes) -> None

    w = where & ~m2.SSL_ST_MASK
    if w & m2.SSL_ST_CONNECT:
        state = "SSL connect"
    elif w & m2.SSL_ST_ACCEPT:
        state = "SSL accept"
    else:
        state = "SSL state unknown"

    if where & m2.SSL_CB_LOOP:
        sys.stderr.write("LOOP: %s: %s\n" %
                         (state, m2.ssl_get_state_v(ssl_ptr)))
        sys.stderr.flush()
        return

    if where & m2.SSL_CB_EXIT:
        if not ret:
            sys.stderr.write("FAILED: %s: %s\n" %
                             (state, m2.ssl_get_state_v(ssl_ptr)))
            sys.stderr.flush()
        else:
            sys.stderr.write("INFO: %s: %s\n" %
                             (state, m2.ssl_get_state_v(ssl_ptr)))
            sys.stderr.flush()
        return

    if where & m2.SSL_CB_ALERT:
        if where & m2.SSL_CB_READ:
            w = 'read'
        else:
            w = 'write'
        sys.stderr.write("ALERT: %s: %s: %s\n" %
                         (w, m2.ssl_get_alert_type_v(ret),
                          m2.ssl_get_alert_desc_v(ret)))
        sys.stderr.flush()
        return
