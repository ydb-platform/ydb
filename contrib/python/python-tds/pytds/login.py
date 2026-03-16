# vim: set fileencoding=utf8 :
"""
.. module:: login
   :platform: Unix, Windows, MacOSX
   :synopsis: Login classes

.. moduleauthor:: Mikhail Denisenko <denisenkom@gmail.com>
"""
from __future__ import annotations

import base64
import ctypes
import logging
import socket

from pytds.tds_base import AuthProtocol

logger = logging.getLogger(__name__)


class SspiAuth(AuthProtocol):
    """SSPI authentication

    :platform: Windows

    Required parameters are server_name and port or spn

    :keyword user_name: User name, if not provided current security context will be used
    :type user_name: str
    :keyword password: User password, if not provided current security context will be used
    :type password: str
    :keyword server_name: MSSQL server host name
    :type server_name: str
    :keyword port: MSSQL server port
    :type port: int
    :keyword spn: Service name
    :type spn: str
    """

    def __init__(
        self,
        user_name: str = "",
        password: str = "",
        server_name: str = "",
        port: int | None = None,
        spn: str | None = None,
    ) -> None:
        from . import sspi

        # parse username/password informations
        if "\\" in user_name:
            domain, user_name = user_name.split("\\")
        else:
            domain = ""
        if domain and user_name:
            self._identity = sspi.make_winnt_identity(domain, user_name, password)
        else:
            self._identity = None
        # build SPN
        if spn:
            self._sname = spn
        else:
            primary_host_name, _, _ = socket.gethostbyname_ex(server_name)
            self._sname = f"MSSQLSvc/{primary_host_name}:{port}"

        # using Negotiate system will use proper protocol (either NTLM or Kerberos)
        self._cred = sspi.SspiCredentials(
            package="Negotiate", use=sspi.SECPKG_CRED_OUTBOUND, identity=self._identity
        )

        self._flags = (
            sspi.ISC_REQ_CONFIDENTIALITY
            | sspi.ISC_REQ_REPLAY_DETECT
            | sspi.ISC_REQ_CONNECTION
        )
        self._ctx = None

    def create_packet(self) -> bytes:
        from . import sspi

        buf = ctypes.create_string_buffer(4096)
        ctx, status, bufs = self._cred.create_context(
            flags=self._flags,
            byte_ordering="network",
            target_name=self._sname,
            output_buffers=[(sspi.SECBUFFER_TOKEN, buf)],
        )
        self._ctx = ctx
        if status == sspi.Status.SEC_I_COMPLETE_AND_CONTINUE:
            ctx.complete_auth_token(bufs)
        return bufs[0][1]

    def handle_next(self, packet: bytes) -> bytes | None:
        from . import sspi

        if self._ctx:
            buf = ctypes.create_string_buffer(4096)
            status, buffers = self._ctx.next(
                flags=self._flags,
                byte_ordering="network",
                target_name=self._sname,
                input_buffers=[(sspi.SECBUFFER_TOKEN, packet)],
                output_buffers=[(sspi.SECBUFFER_TOKEN, buf)],
            )
            return buffers[0][1]
        else:
            return None

    def close(self) -> None:
        if self._ctx:
            self._ctx.close()


class NtlmAuth(AuthProtocol):
    """
    This class is deprecated since `ntlm-auth` package, on which it depends, is deprecated.
    Instead use :class:`.SpnegoAuth`.

    NTLM authentication, uses Python implementation (ntlm-auth)

    For more information about NTLM authentication see https://github.com/jborean93/ntlm-auth

    :param user_name: User name
    :type user_name: str
    :param password: User password
    :type password: str
    :param ntlm_compatibility: NTLM compatibility level, default is 3(NTLMv2)
    :type ntlm_compatibility: int
    """

    def __init__(self, user_name: str, password: str, ntlm_compatibility: int = 3) -> None:
        self._user_name = user_name
        if "\\" in user_name:
            domain, self._user = user_name.split("\\", 1)
            self._domain = domain.upper()
        else:
            self._domain = "WORKSPACE"
            self._user = user_name
        self._password = password
        self._workstation = socket.gethostname().upper()

        try:
            from ntlm_auth.ntlm import NtlmContext  # type: ignore # fix later
        except ImportError:
            raise ImportError(
                "To use NTLM authentication you need to install ntlm-auth module"
            )

        self._ntlm_context = NtlmContext(
            self._user,
            self._password,
            self._domain,
            self._workstation,
            ntlm_compatibility=ntlm_compatibility,
        )

    def create_packet(self) -> bytes:
        return self._ntlm_context.step()

    def handle_next(self, packet: bytes) -> bytes | None:
        return self._ntlm_context.step(packet)

    def close(self) -> None:
        pass


class SpnegoAuth(AuthProtocol):
    """Authentication using Negotiate protocol, uses implementation provided pyspnego package

    Takes same parameters as spnego.client function.
    """

    def __init__(self, *args, **kwargs) -> None:
        try:
            import spnego
        except ImportError:
            raise ImportError(
                "To use spnego authentication you need to install pyspnego package"
            )
        self._context = spnego.client(*args, **kwargs)

    def create_packet(self) -> bytes:
        result = self._context.step()
        if not result:
            raise RuntimeError("spnego did not create initial packet")
        return result

    def handle_next(self, packet: bytes) -> bytes | None:
        return self._context.step(packet)

    def close(self) -> None:
        pass


class KerberosAuth(AuthProtocol):
    def __init__(self, server_principal: str) -> None:
        try:
            import kerberos  # type: ignore # fix later
        except ImportError:
            import winkerberos as kerberos  # type: ignore # fix later
        self._kerberos = kerberos
        res, context = kerberos.authGSSClientInit(server_principal)
        if res < 0:
            raise RuntimeError(f"authGSSClientInit failed with code {res}")
        logger.info("Initialized GSS context")
        self._context = context

    def create_packet(self) -> bytes:
        res = self._kerberos.authGSSClientStep(self._context, "")
        if res < 0:
            raise RuntimeError(f"authGSSClientStep failed with code {res}")
        data = self._kerberos.authGSSClientResponse(self._context)
        logger.info("created first client GSS packet %s", data)
        return base64.b64decode(data)

    def handle_next(self, packet: bytes) -> bytes | None:
        res = self._kerberos.authGSSClientStep(
            self._context, base64.b64encode(packet).decode("ascii")
        )
        if res < 0:
            raise RuntimeError(f"authGSSClientStep failed with code {res}")
        if res == self._kerberos.AUTH_GSS_COMPLETE:
            logger.info("GSS authentication completed")
            return b""
        else:
            data = self._kerberos.authGSSClientResponse(self._context)
            logger.info("created client GSS packet %s", data)
            return base64.b64decode(data)

    def close(self) -> None:
        pass
