from __future__ import absolute_import

"""M2Crypto SSL services.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

import socket, os

# M2Crypto
from M2Crypto import _m2crypto as m2


# from M2Crypto.SSL.SSLServer import (
#     ForkingSSLServer,
#     SSLServer,
#     ThreadingSSLServer,
# )


class SSLError(Exception):
    pass


class SSLTimeoutError(SSLError, socket.timeout):
    pass


m2.ssl_init(SSLError, SSLTimeoutError)

# M2Crypto.SSL
from M2Crypto.SSL.Cipher import Cipher, Cipher_Stack
from M2Crypto.SSL.Connection import Connection
from M2Crypto.SSL.Context import Context
from M2Crypto.SSL.SSLServer import SSLServer, ThreadingSSLServer

if os.name != 'nt':
    from M2Crypto.SSL.SSLServer import ForkingSSLServer
from M2Crypto.SSL.timeout import (
    timeout,
    struct_to_timeout,
    struct_size,
)

verify_none: int = m2.SSL_VERIFY_NONE
verify_peer: int = m2.SSL_VERIFY_PEER
verify_fail_if_no_peer_cert: int = m2.SSL_VERIFY_FAIL_IF_NO_PEER_CERT
verify_client_once: int = m2.SSL_VERIFY_CLIENT_ONCE
verify_crl_check_chain: int = m2.VERIFY_CRL_CHECK_CHAIN
verify_crl_check_leaf: int = m2.VERIFY_CRL_CHECK_LEAF

SSL_SENT_SHUTDOWN: int = m2.SSL_SENT_SHUTDOWN
SSL_RECEIVED_SHUTDOWN: int = m2.SSL_RECEIVED_SHUTDOWN

op_all: int = m2.SSL_OP_ALL
op_no_sslv2: int = m2.SSL_OP_NO_SSLv2
