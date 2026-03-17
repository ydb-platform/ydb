from __future__ import annotations

import logging
from typing import Any
import typing

try:
    import OpenSSL.SSL  # type: ignore # needs fixing
except ImportError:
    OPENSSL_AVAILABLE = False
else:
    OPENSSL_AVAILABLE = True

from . import tds_base

BUFSIZE = 65536


logger = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from pytds.tds_session import _TdsSession


class EncryptedSocket(tds_base.TransportProtocol):
    def __init__(
        self, transport: tds_base.TransportProtocol, tls_conn: OpenSSL.SSL.Connection
    ) -> None:
        super().__init__()
        self._transport = transport
        self._tls_conn = tls_conn

    def gettimeout(self) -> float | None:
        return self._transport.gettimeout()

    def settimeout(self, timeout: float | None) -> None:
        self._transport.settimeout(timeout)

    def sendall(self, data: Any, flags: int = 0) -> None:
        # TLS.Connection does not support bytearrays, need to convert to bytes first
        if isinstance(data, bytearray):
            data = bytes(data)

        self._tls_conn.sendall(data)
        buf = self._tls_conn.bio_read(BUFSIZE)
        self._transport.sendall(buf)

    #   def send(self, data):
    #       while True:
    #           try:
    #               return self._tls_conn.send(data)
    #           except OpenSSL.SSL.WantWriteError:
    #               buf = self._tls_conn.bio_read(BUFSIZE)
    #               self._transport.sendall(buf)

    def recv_into(
        self, buffer: bytearray | memoryview, size: int = 0, flags: int = 0
    ) -> int:
        if size == 0:
            size = len(buffer)
        res = self.recv(size)
        buffer[0 : len(res)] = res
        return len(res)

    def recv(self, bufsize: int, flags: int = 0) -> bytes:
        while True:
            try:
                buf = self._tls_conn.bio_read(bufsize)
            except OpenSSL.SSL.WantReadError:
                pass
            else:
                self._transport.sendall(buf)

            try:
                return self._tls_conn.recv(bufsize)
            except OpenSSL.SSL.WantReadError:
                buf = self._transport.recv(BUFSIZE)
                if buf:
                    self._tls_conn.bio_write(buf)
                else:
                    return b""

    def close(self) -> None:
        self._tls_conn.shutdown()
        self._transport.close()

    def shutdown(self, how: int = 0) -> None:
        self._tls_conn.shutdown()


def verify_cb(conn, cert, err_num, err_depth, ret_code: int) -> bool:
    return ret_code == 1


def is_san_matching(san: str, host_name: str) -> bool:
    for item in san.split(','):
        dnsentry = item.strip().lstrip('DNS:').strip()
        # SANs are usually have form like: DNS:hostname
        if dnsentry == host_name:
            return True
        if (
            dnsentry[0:2] == "*."
        ):  # support for wildcards, but only at the first position
            afterstar_parts = dnsentry[2:]
            afterstar_parts_sname = ".".join(
                host_name.split(".")[1:]
            )  # remove first part of dns name
            if afterstar_parts == afterstar_parts_sname:
                return True
    return False


def validate_host(cert, name: bytes) -> bool:
    """
    Validates host name against certificate

    @param cert: Certificate returned by host
    @param name: Actual host name used for connection
    @return: Returns true if host name matches certificate
    """
    cn = None
    for t, v in cert.get_subject().get_components():
        if t == b"CN":
            cn = v
            break

    if cn == name:
        return True

    # checking SAN
    s_name = name.decode("ascii")
    for i in range(cert.get_extension_count()):
        ext = cert.get_extension(i)
        if ext.get_short_name() == b"subjectAltName":
            s = str(ext)
            if is_san_matching(s, s_name):
                return True

    # TODO check if wildcard is needed in CN as well
    return False


def create_context(cafile: str) -> OpenSSL.SSL.Context:
    ctx = OpenSSL.SSL.Context(OpenSSL.SSL.TLSv1_2_METHOD)
    ctx.set_options(OpenSSL.SSL.OP_NO_SSLv2)
    ctx.set_options(OpenSSL.SSL.OP_NO_SSLv3)
    ctx.set_verify(OpenSSL.SSL.VERIFY_PEER, verify_cb)
    # print("verify depth:", ctx.get_verify_depth())
    # print("verify mode:", ctx.get_verify_mode())
    # print("openssl version:", cryptography.hazmat.backends.openssl.backend.openssl_version_text())
    ctx.load_verify_locations(cafile=cafile)
    return ctx


# https://msdn.microsoft.com/en-us/library/dd357559.aspx
def establish_channel(tds_sock: _TdsSession) -> None:
    w = tds_sock._writer
    r = tds_sock._reader
    login = tds_sock.conn._login
    tls_ctx = login.tls_ctx
    if not tls_ctx:
        raise Exception("login.tls_ctx is not set unexpectedly")

    bhost = login.server_name.encode("ascii")

    conn = OpenSSL.SSL.Connection(tls_ctx)
    conn.set_tlsext_host_name(bhost)
    # change connection to client mode
    conn.set_connect_state()
    logger.info("doing TLS handshake")
    while True:
        try:
            logger.debug("calling do_handshake")
            conn.do_handshake()
        except OpenSSL.SSL.WantReadError:
            logger.debug(
                "got WantReadError, getting data from the write end of the TLS connection buffer"
            )
            try:
                req = conn.bio_read(BUFSIZE)
            except OpenSSL.SSL.WantReadError:
                # PyOpenSSL - https://github.com/pyca/pyopenssl/issues/887
                logger.debug("got WantReadError again, waiting for response...")
            else:
                logger.debug(
                    "sending %d bytes of the handshake data to the server", len(req)
                )
                w.begin_packet(tds_base.PacketType.PRELOGIN)
                w.write(req)
                w.flush()
            logger.debug("receiving response from the server")
            resp_meta = r.begin_response()
            if resp_meta.type != tds_base.PacketType.PRELOGIN:
                raise tds_base.Error(
                    f"Invalid packet type was received from server, expected PRELOGIN(18) got {resp_meta.type}"
                )
            while not r.stream_finished():
                resp = r.recv(4096)
                logger.debug(
                    "adding %d bytes of the response into the TLS connection buffer",
                    len(resp),
                )
                conn.bio_write(resp)
        else:
            logger.info("TLS handshake is complete")
            if login.validate_host:
                if not validate_host(cert=conn.get_peer_certificate(), name=bhost):
                    raise tds_base.Error(
                        "Certificate does not match host name '{}'".format(
                            login.server_name
                        )
                    )
            enc_sock = EncryptedSocket(transport=tds_sock.conn.sock, tls_conn=conn)
            tds_sock.conn.sock = enc_sock
            tds_sock._writer._transport = enc_sock
            tds_sock._reader._transport = enc_sock
            return


def revert_to_clear(tds_sock: _TdsSession) -> None:
    """
    Reverts connection back to non-encrypted mode
    Used when client sent ENCRYPT_OFF flag
    @param tds_sock:
    @return:
    """
    enc_conn = tds_sock.conn.sock
    if isinstance(enc_conn, EncryptedSocket):
        clear_conn = enc_conn._transport
        enc_conn.shutdown()
        tds_sock.conn.sock = clear_conn
        tds_sock._writer._transport = clear_conn
        tds_sock._reader._transport = clear_conn
