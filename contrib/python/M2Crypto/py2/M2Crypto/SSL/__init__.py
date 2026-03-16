from __future__ import absolute_import

"""M2Crypto SSL services.

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

import socket, os

# M2Crypto
from M2Crypto import _m2crypto as m2


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
from M2Crypto.SSL.timeout import timeout, struct_to_timeout, struct_size

verify_none = m2.SSL_VERIFY_NONE  # type: int
verify_peer = m2.SSL_VERIFY_PEER  # type: int
verify_fail_if_no_peer_cert = m2.SSL_VERIFY_FAIL_IF_NO_PEER_CERT  # type: int
verify_client_once = m2.SSL_VERIFY_CLIENT_ONCE  # type: int
verify_crl_check_chain = m2.VERIFY_CRL_CHECK_CHAIN # type: int
verify_crl_check_leaf = m2.VERIFY_CRL_CHECK_LEAF # type: int

SSL_SENT_SHUTDOWN = m2.SSL_SENT_SHUTDOWN  # type: int
SSL_RECEIVED_SHUTDOWN = m2.SSL_RECEIVED_SHUTDOWN  # type: int

op_all = m2.SSL_OP_ALL  # type: int
op_no_sslv2 = m2.SSL_OP_NO_SSLv2  # type: int
