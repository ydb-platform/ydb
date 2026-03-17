# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import base64
import logging
import os
import socket
import typing

from spnego._context import (
    IOV,
    ContextProxy,
    ContextReq,
    IOVUnwrapResult,
    IOVWrapResult,
    SecPkgContextSizes,
    UnwrapResult,
    WinRMWrapResult,
    WrapResult,
    split_username,
)
from spnego._credential import (
    Credential,
    CredentialCache,
    NTLMHash,
    Password,
    unify_credentials,
)
from spnego._ntlm_raw.crypto import (
    RC4Handle,
    compute_response_v1,
    compute_response_v2,
    hmac_md5,
    lmowfv1,
    md5,
    ntowfv1,
    ntowfv2,
    rc4init,
    rc4k,
    sealkey,
    signkey,
)
from spnego._ntlm_raw.messages import (
    Authenticate,
    AvFlags,
    AvId,
    Challenge,
    FileTime,
    Negotiate,
    NegotiateFlags,
    NTClientChallengeV2,
    TargetInfo,
    Version,
)
from spnego._ntlm_raw.security import seal, sign
from spnego._text import to_text
from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import (
    BadBindingsError,
    BadMICError,
    ErrorCode,
    InvalidTokenError,
    NegotiateOptions,
    NoContextError,
    OperationNotAvailableError,
    SpnegoError,
    UnsupportedQop,
)
from spnego.iov import BufferType, IOVResBuffer

log = logging.getLogger(__name__)


def _get_credential_file() -> typing.Optional[str]:
    """Get the path to the NTLM credential store.

    Returns the path to the NTLM credential store specified by the environment variable `NTLM_USER_FILE`.

    Returns:
        Optional[bytes]: The path to the NTLM credential file or None if not set or found.
    """
    user_file_path = os.environ.get("NTLM_USER_FILE", None)
    if not user_file_path:
        return None

    file_path = to_text(user_file_path, encoding="utf-8")
    if os.path.isfile(file_path):
        return file_path

    return None


def _get_credential(
    store: typing.Optional[str],
    domain: typing.Optional[str] = None,
    username: typing.Optional[str] = None,
) -> typing.Tuple[typing.Optional[str], typing.Optional[str], bytes, bytes]:
    """Look up NTLM credentials from the common flat file.

    Retrieves the LM and NT hash for use with authentication or validating a credential from an initiator.

    Each line in the store can be in the Heimdal format `DOMAIN:USER:PASSWORD` like::

        testdom:testuser:Password01
        :testuser@TESTDOM.COM:Password01

    Or it can use the `smbpasswd`_ file format `USERNAME:UID:LM_HASH:NT_HASH:ACCT_FLAGS:TIMESTAMP` like::

        testuser:1000:278623D830DABE161104594F8C2EF12B:C3C6F4FD8A02A6C1268F1A8074B6E7E0:[U]:LCT-1589398321
        TESTDOM\testuser:1000:4588C64B89437893AAD3B435B51404EE:65202355FA01AEF26B89B19E00F52679:[U]:LCT-1589398321
        testuser@TESTDOM.COM:1000:00000000000000000000000000000000:8ADB9B997580D69E69CAA2BBB68F4697:[U]:LCT-1589398321

    While only the `USERNAME`, `LM_HASH`, and `NT_HASH` fields are used, the colons are still required to differentiate
    between the 2 formats. See `ntlm hash generator`_ for ways to generate the `LM_HASH` and `NT_HASH`.

    The username is case insensitive but the format of the domain and user part must match up with the value used as
    the username specified by the caller.

    While each line can use a different format, it is recommended to stick to 1 throughout the file.

    The same env var and format can also be read with gss-ntlmssp.

    Args:
        store: The credential store to lookup the credential from.
        domain: The domain for the user to get the credentials for. Should be `None` for a user in the UPN form.
        username: The username to get the credentials for. If omitted then the first entry in the store is used.

    Returns:
        Tuple[str, str, bytes, bytes]: The domain, username, LM, and NT hash of the user specified.

    .. _smbpasswd:
        https://www.samba.org/samba/docs/current/man-html/smbpasswd.5.html

    .. _ntlm hash generator:
        https://asecuritysite.com/encryption/lmhash
    """
    if not store:
        raise OperationNotAvailableError(
            context_msg="No username or password was specified and the credential cache did not exist or contained no credentials"
        )

    domain = domain or ""

    def store_lines(
        text: str,
    ) -> typing.Iterator[typing.Tuple[str, str, typing.Optional[str], typing.Optional[bytes], typing.Optional[bytes]]]:
        for line in text.splitlines():
            line_split = line.split(":")

            if len(line_split) == 3:
                yield line_split[0], line_split[1], line_split[2], None, None

            elif len(line_split) == 6:
                domain_entry, user_entry = split_username(line_split[0])
                lm_entry = base64.b16decode(line_split[2].upper())
                nt_entry = base64.b16decode(line_split[3].upper())

                yield domain_entry or "", user_entry or "", None, lm_entry, nt_entry

    with open(store, mode="rb") as fd:
        cred_text = fd.read().decode()

        for line_domain, line_user, line_password, lm_hash, nt_hash in store_lines(cred_text):
            if not username or (username.upper() == line_user.upper() and domain.upper() == line_domain.upper()):
                # The Heimdal format uses the password so if the LM or NT hash isn't set generate it ourselves.
                if not lm_hash:
                    lm_hash = lmowfv1(line_password or "")
                if not nt_hash:
                    nt_hash = ntowfv1(line_password or "")

                # Favour the explicit username/password value, otherwise use what was in the credential file.
                if not username:
                    username = line_user

                if not domain:
                    domain = line_domain or None

                return domain, username, lm_hash, nt_hash

        else:
            raise SpnegoError(
                ErrorCode.failure,
                context_msg="Failed to find any matching credential in NTLM_USER_FILE credential store.",
            )


def _get_workstation() -> typing.Optional[str]:
    """Get the current workstation name.

    This gets the current workstation name that respects `NETBIOS_COMPUTER_NAME`. The env var is used by the library
    that gss-ntlmssp calls and makes sure that this Python implementation is a closer in its behaviour.

    Returns:
        Optional[str]: The workstation to supply in the NTLM authentication message or None.
    """
    if "NETBIOS_COMPUTER_NAME" in os.environ:
        workstation = os.environ["NETBIOS_COMPUTER_NAME"]

    else:
        workstation = socket.gethostname().upper()

    # An empty workstation should be None so we don't set it in the message.
    return to_text(workstation) if workstation else None


class _NTLMCredential:
    def __init__(
        self,
        credential: typing.Optional[Credential] = None,
    ) -> None:
        self._raw_username: typing.Optional[str] = None
        self._store: typing.Optional[str]
        if isinstance(credential, Password):
            self._store = "explicit"
            self._raw_username = credential.username
            self.domain, self.username = split_username(credential.username)
            self.lm_hash = lmowfv1(credential.password)
            self.nt_hash = ntowfv1(credential.password)

        elif isinstance(credential, NTLMHash):
            self._store = "explicit"
            self._raw_username = credential.username
            self.domain, self.username = split_username(credential.username)
            self.lm_hash = base64.b16decode(credential.lm_hash.upper()) if credential.lm_hash else b"\x00" * 16
            self.nt_hash = base64.b16decode(credential.nt_hash.upper()) if credential.nt_hash else b"\x00" * 16

        else:
            domain = username = None
            if isinstance(credential, CredentialCache):
                self._raw_username = credential.username
                domain, username = split_username(credential.username)

            self._store = _get_credential_file()
            self.domain, self.username, self.lm_hash, self.nt_hash = _get_credential(self._store, domain, username)


class NTLMProxy(ContextProxy):
    """A context wrapper for a Python managed NTLM context.

    This is a context that can be used on Linux to generate NTLM without any system dependencies.
    """

    def __init__(
        self,
        username: typing.Optional[typing.Union[str, Credential, typing.List[Credential]]] = None,
        password: typing.Optional[str] = None,
        hostname: typing.Optional[str] = None,
        service: typing.Optional[str] = None,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
        context_req: ContextReq = ContextReq.default,
        usage: str = "initiate",
        protocol: str = "ntlm",
        options: NegotiateOptions = NegotiateOptions.none,
        **kwargs: typing.Any,
    ) -> None:
        credentials = unify_credentials(username, password, required_protocol="ntlm")
        super(NTLMProxy, self).__init__(
            credentials, hostname, service, channel_bindings, context_req, usage, protocol, options
        )

        self._complete = False
        self._credential: typing.Optional[_NTLMCredential] = None

        # Set the default flags, these might change depending on the LM_COMPAT_LEVEL set.
        self._context_req = (
            self._context_req
            | NegotiateFlags.key_128
            | NegotiateFlags.key_56
            | NegotiateFlags.key_exch
            | NegotiateFlags.extended_session_security
            | NegotiateFlags.always_sign
            | NegotiateFlags.ntlm
            | NegotiateFlags.lm_key
            | NegotiateFlags.request_target
            | NegotiateFlags.oem
            | NegotiateFlags.unicode
        )

        # gss-ntlmssp uses the env var 'LM_COMPAT_LEVEL' to control the NTLM compatibility level. To try and make our
        # NTLM implementation similar in functionality we will also use that behaviour.
        # https://github.com/gssapi/gss-ntlmssp/blob/e498737a96e8832a2cb9141ab1fe51e129185a48/src/gss_ntlmssp.c#L159-L170
        # See the below policy link for more details on what these mean, for now 3 is the sane behaviour.
        # https://docs.microsoft.com/en-us/windows/security/threat-protection/security-policy-settings/network-security-lan-manager-authentication-level
        lm_compat_level = int(os.environ.get("LM_COMPAT_LEVEL", 3))
        if lm_compat_level < 0 or lm_compat_level > 5:
            raise SpnegoError(
                ErrorCode.failure, context_msg="Invalid LM_COMPAT_LEVEL %d, must be between 0 and 5" % lm_compat_level
            )

        if lm_compat_level == 0:
            self._context_req &= ~NegotiateFlags.extended_session_security

        if self.usage == "initiate":
            self._credential = _NTLMCredential(next(c for c in credentials if "ntlm" in c.supported_protocols))

            self._lm = lm_compat_level < 2
            self._nt_v1 = lm_compat_level < 3
            self._nt_v2 = lm_compat_level > 2

            if lm_compat_level > 1:
                self._context_req &= ~NegotiateFlags.lm_key

        else:
            self._lm = lm_compat_level < 4
            self._nt_v1 = lm_compat_level < 5
            self._nt_v2 = True

            # Make sure that the credential file is set and exists
            if not _get_credential_file():
                raise OperationNotAvailableError(
                    context_msg="NTLM acceptor requires NTLM credential cache to be provided through the env var NTLM_USER_FILE set to a filepath"
                )

        self._temp_negotiate: typing.Optional[Negotiate] = None
        self._temp_challenge: typing.Optional[Challenge] = None
        self._mic_required = False

        # Crypto state for signing and sealing.
        self._session_key: typing.Optional[bytes] = None
        self._sign_key_out: typing.Optional[bytes] = None
        self._sign_key_in: typing.Optional[bytes] = None
        self._handle_out: typing.Optional[RC4Handle] = None
        self._handle_in: typing.Optional[RC4Handle] = None
        self.__seq_num_in = 0
        self.__seq_num_out = 0

    @classmethod
    def available_protocols(cls, options: typing.Optional[NegotiateOptions] = None) -> typing.List[str]:
        return ["ntlm"]

    @classmethod
    def iov_available(cls) -> bool:
        return True

    @property
    def client_principal(self) -> typing.Optional[str]:
        if self.usage == "accept" and self.complete and self._credential:
            domain_part = self._credential.domain + "\\" if self._credential.domain else ""
            return "%s%s" % (domain_part, self._credential.username)

        return None

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def negotiated_protocol(self) -> typing.Optional[str]:
        return "ntlm"

    @property
    def session_key(self) -> bytes:
        return self._session_key or b""

    def new_context(self) -> "NTLMProxy":
        cred: typing.Optional[NTLMHash] = None
        if self._credential and self.usage == "initiate":
            cred = NTLMHash(
                username=self._credential._raw_username or "",
                lm_hash=base64.b16encode(self._credential.lm_hash).decode(),
                nt_hash=base64.b16encode(self._credential.nt_hash).decode(),
            )

        return NTLMProxy(
            username=cred,
            hostname=self._hostname,
            service=self._service,
            channel_bindings=self.channel_bindings,
            context_req=self.context_req,
            usage=self.usage,
            protocol=self.protocol,
            options=self.options,
        )

    def step(
        self,
        in_token: typing.Optional[bytes] = None,
        *,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
    ) -> typing.Optional[bytes]:
        if not self._is_wrapped:
            log.debug("NTLM step input: %s", base64.b64encode(in_token or b"").decode())

        out_token = getattr(self, "_step_%s" % self.usage)(
            in_token=in_token,
            channel_bindings=channel_bindings,
        )

        if not self._is_wrapped:
            log.debug("NTLM step output: %s", base64.b64encode(out_token or b"").decode())

        if self._complete:
            # Clear out any temp data we still have stored.
            self._temp_negotiate = None
            self._temp_challenge = None

            in_usage = "accept" if self.usage == "initiate" else "initiate"
            session_key = self._session_key or b""
            # if tmp_key := os.environ.get("TESTING", None):
            #     session_key = self._session_key = base64.b16decode(tmp_key)

            self._sign_key_out = signkey(self._context_attr, session_key, self.usage)
            self._sign_key_in = signkey(self._context_attr, session_key, in_usage)

            # Found a vague reference in MS-NLMP that states if NTLMv2 authentication was not used then only 1 key is
            # used for sealing. This seems to reference when NTLMSSP_NEGOTIATE_EXTENDED_SESSION_SECURITY is not set and
            # not NTLMv2 messages itself.
            # https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/d1c86e81-eb66-47fd-8a6f-970050121347
            if self._context_attr & NegotiateFlags.extended_session_security:
                self._handle_out = rc4init(sealkey(self._context_attr, session_key, self.usage))
                self._handle_in = rc4init(sealkey(self._context_attr, session_key, in_usage))
            else:
                self._handle_out = self._handle_in = rc4init(sealkey(self._context_attr, session_key, self.usage))

        return out_token

    def _step_initiate(
        self,
        in_token: typing.Optional[bytes] = None,
        *,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
    ) -> bytes:
        if not self._temp_negotiate:
            # SSPI always sends the version even if it's optional. There have
            # been reports that some NTLM servers expect this to produce a
            # valid challenge/authentication token later in the exchange.
            # https://github.com/jborean93/smbprotocol/issues/216
            self._temp_negotiate = Negotiate(self._context_req, version=Version.get_current())
            return self._temp_negotiate.pack()

        in_token = in_token or b""
        challenge = Challenge.unpack(in_token)

        # Lots of mypy failures with self._credential, cast it to non Optional as it will always be set here.
        credential = typing.cast(_NTLMCredential, self._credential)
        auth_kwargs: typing.Dict[str, typing.Any] = {
            "domain_name": credential.domain,
            "username": credential.username,
        }

        if challenge.flags & NegotiateFlags.version:
            auth_kwargs["version"] = Version.get_current()
            auth_kwargs["workstation"] = _get_workstation()

        nt_challenge, lm_challenge, key_exchange_key = self._compute_response(
            challenge,
            credential,
            channel_bindings=channel_bindings or self.channel_bindings,
        )

        if challenge.flags & NegotiateFlags.key_exch:
            # This is only documented on the server side for MS-NLMP but is also valid for the client. The actual
            # session key is the KeyExchangeKey like normal unless sign or seal is negotiated.

            if challenge.flags & NegotiateFlags.sign or challenge.flags & NegotiateFlags.seal:
                self._session_key = os.urandom(16)
                auth_kwargs["encrypted_session_key"] = rc4k(key_exchange_key, self._session_key)

            else:
                self._session_key = key_exchange_key
                auth_kwargs["encrypted_session_key"] = b"\x00"  # Must be set to some value but this can be anything.

        else:
            self._session_key = key_exchange_key

        authenticate = Authenticate(challenge.flags, lm_challenge, nt_challenge, **auth_kwargs)

        if self._mic_required:
            authenticate.mic = self._calculate_mic(
                self._session_key, self._temp_negotiate.pack(), in_token, authenticate.pack()
            )

        self._context_attr = authenticate.flags
        self._complete = True

        return authenticate.pack()

    def _step_accept(
        self,
        in_token: bytes,
        *,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
    ) -> typing.Optional[bytes]:
        if not self._temp_negotiate:
            return self._step_accept_negotiate(in_token)

        else:
            self._step_accept_authenticate(
                in_token,
                channel_bindings=channel_bindings or self.channel_bindings,
            )
            return None

    def _step_accept_negotiate(self, token: bytes) -> bytes:
        """Process the Negotiate message from the initiator."""
        negotiate = Negotiate.unpack(token)

        flags = (
            negotiate.flags
            | NegotiateFlags.request_target
            | NegotiateFlags.ntlm
            | NegotiateFlags.always_sign
            | NegotiateFlags.target_info
            | NegotiateFlags.target_type_server
        )

        # Make sure either UNICODE or OEM is set, not both.
        if flags & NegotiateFlags.unicode:
            flags &= ~NegotiateFlags.oem
        elif flags & NegotiateFlags.oem == 0:
            raise SpnegoError(
                ErrorCode.failure,
                context_msg="Neither NEGOTIATE_OEM or NEGOTIATE_UNICODE flags were "
                "set, cannot derive encoding for text fields",
            )

        if flags & NegotiateFlags.extended_session_security:
            flags &= ~NegotiateFlags.lm_key

        server_challenge = os.urandom(8)
        target_name = to_text(socket.gethostname()).upper()

        target_info = TargetInfo()
        target_info[AvId.nb_computer_name] = target_name
        target_info[AvId.nb_domain_name] = "WORKSTATION"
        target_info[AvId.dns_computer_name] = to_text(socket.getfqdn())
        target_info[AvId.timestamp] = FileTime.now()

        challenge = Challenge(flags, server_challenge, target_name=target_name, target_info=target_info)

        self._temp_negotiate = negotiate
        self._temp_challenge = challenge

        return challenge.pack()

    def _step_accept_authenticate(self, token: bytes, channel_bindings: typing.Optional[GssChannelBindings]) -> None:
        """Process the Authenticate message from the initiator."""
        negotiate = typing.cast(Negotiate, self._temp_negotiate)
        challenge = typing.cast(Challenge, self._temp_challenge)
        server_challenge = challenge.server_challenge
        auth = Authenticate.unpack(token)

        # TODO: Add anonymous user support.
        if not auth.user_name or (
            not auth.nt_challenge_response and (not auth.lm_challenge_response or auth.lm_challenge_response == b"\x00")
        ):
            raise OperationNotAvailableError(context_msg="Anonymous user authentication not implemented")

        username = auth.user_name
        if auth.domain_name:
            username = f"{auth.domain_name}\\{username}"
        self._credential = _NTLMCredential(CredentialCache(username=username))
        expected_mic = None

        if auth.nt_challenge_response and len(auth.nt_challenge_response) > 24:
            nt_hash = ntowfv2(auth.user_name, self._credential.nt_hash, auth.domain_name)

            nt_challenge = NTClientChallengeV2.unpack(auth.nt_challenge_response[16:])
            time = nt_challenge.time_stamp
            client_challenge = nt_challenge.challenge_from_client
            target_info = nt_challenge.av_pairs

            expected_nt, expected_lm, key_exchange_key = compute_response_v2(
                nt_hash, server_challenge, client_challenge, time, target_info
            )

            if channel_bindings:
                if AvId.channel_bindings not in target_info:
                    raise BadBindingsError(
                        context_msg="Acceptor bindings specified but not present in initiator " "response"
                    )

                expected_bindings = target_info[AvId.channel_bindings]
                actual_bindings = md5(channel_bindings.pack())
                if expected_bindings not in [actual_bindings, b"\x00" * 16]:
                    raise BadBindingsError(context_msg="Acceptor bindings do not match initiator bindings")

            if target_info.get(AvId.flags, 0) & AvFlags.mic:
                expected_mic = auth.mic

        else:
            if not self._nt_v1:
                raise InvalidTokenError(context_msg="Acceptor settings are set to reject NTv1 responses")

            elif not auth.nt_challenge_response and not self._lm:
                raise InvalidTokenError(context_msg="Acceptor settings are set to reject LM responses")

            client_challenge = b"\x00" * 8
            if auth.flags & NegotiateFlags.extended_session_security:
                client_challenge = (auth.lm_challenge_response or b"\x00" * 8)[:8]

            expected_nt, expected_lm, key_exchange_key = compute_response_v1(
                auth.flags,
                self._credential.nt_hash,
                self._credential.lm_hash,
                server_challenge,
                client_challenge,
                no_lm_response=not self._lm,
            )

        auth_success = False

        if auth.nt_challenge_response:
            auth_success = auth.nt_challenge_response == expected_nt

        elif auth.lm_challenge_response:
            auth_success = auth.lm_challenge_response == expected_lm

        if not auth_success:
            raise InvalidTokenError(context_msg="Invalid NTLM response from initiator")

        if auth.flags & NegotiateFlags.key_exch and (
            auth.flags & NegotiateFlags.sign or auth.flags & NegotiateFlags.seal
        ):
            self._session_key = rc4k(key_exchange_key, auth.encrypted_random_session_key or b"")

        else:
            self._session_key = key_exchange_key

        if expected_mic:
            auth.mic = b"\x00" * 16
            actual_mic = self._calculate_mic(self.session_key, negotiate.pack(), challenge.pack(), auth.pack())

            if actual_mic != expected_mic:
                raise InvalidTokenError(context_msg="Invalid MIC in NTLM authentication message")

        self._context_attr = auth.flags
        self._complete = True

    def query_message_sizes(self) -> SecPkgContextSizes:
        if not self.complete:
            raise NoContextError(context_msg="Cannot get message sizes until context has been established")

        return SecPkgContextSizes(header=16)

    def wrap(self, data: bytes, encrypt: bool = True, qop: typing.Optional[int] = None) -> WrapResult:
        if qop:
            raise UnsupportedQop(context_msg="Unsupported QoP value %s specified for NTLM" % qop)

        if self.context_attr & ContextReq.integrity == 0 and self.context_attr & ContextReq.confidentiality == 0:
            raise OperationNotAvailableError(context_msg="NTLM wrap without integrity or confidentiality")

        if not self._handle_out or self._sign_key_out is None:
            raise NoContextError(context_msg="Cannot wrap until context has been established")

        msg, signature = seal(
            self._context_attr,
            self._handle_out,
            self._sign_key_out,
            self._seq_num_out,
            data,
        )

        return WrapResult(data=signature + msg, encrypted=True)

    def wrap_iov(
        self,
        iov: typing.Iterable[IOV],
        encrypt: bool = True,
        qop: typing.Optional[int] = None,
    ) -> IOVWrapResult:
        if qop:
            raise UnsupportedQop(context_msg="Unsupported QoP value %s specified for NTLM" % qop)

        if self.context_attr & ContextReq.integrity == 0 and self.context_attr & ContextReq.confidentiality == 0:
            raise OperationNotAvailableError(context_msg="NTLM wrap without integrity or confidentiality")

        if not self._handle_out or self._sign_key_out is None:
            raise NoContextError(context_msg="Cannot wrap until context has been established")

        header_idx = -1
        data_idx = -1
        signature_input = []
        buffers = self._build_iov_list(iov, lambda b: b)
        res: typing.List[IOVResBuffer] = []

        for idx, buffer in enumerate(buffers):
            data: bytes
            if buffer.type == BufferType.header:
                if header_idx != -1:
                    raise InvalidTokenError(context_msg="wrap_iov must only be used with 1 header IOV buffer.")
                header_idx = idx
                data = b""

            elif buffer.type == BufferType.data:
                if data_idx != -1:
                    raise InvalidTokenError(context_msg="wrap_iov must only be used with 1 data IOV buffer.")

                if not isinstance(buffer.data, bytes):
                    raise InvalidTokenError(context_msg=f"wrap_iov IOV data buffer at [{idx}] must be bytes")

                data_idx = idx
                data = buffer.data
                signature_input.append(data)

            elif buffer.type in [BufferType.sign_only, BufferType.data_readonly]:
                if not isinstance(buffer.data, bytes):
                    raise InvalidTokenError(
                        context_msg=f"wrap_iov IOV {buffer.type.name} buffer at [{idx}] must be bytes"
                    )

                data = buffer.data
                signature_input.append(data)

            else:
                raise InvalidTokenError(context_msg=f"wrap_iov unsupported IOV buffer type {buffer.type.name}")

            res.append(IOVResBuffer(buffer.type, data))

        if header_idx == -1:
            raise InvalidTokenError(context_msg="wrap_iov no IOV header buffer present")

        if data_idx == -1:
            raise InvalidTokenError(context_msg="wrap_iov no IOV data buffer present")

        enc_msg, signature = seal(
            self._context_attr,
            self._handle_out,
            self._sign_key_out,
            self._seq_num_out,
            res[data_idx][1] or b"",
            to_sign=b"".join(signature_input),
        )

        res[header_idx] = IOVResBuffer(BufferType.header, signature)
        res[data_idx] = IOVResBuffer(BufferType.data, enc_msg)

        return IOVWrapResult(tuple(res), encrypted=True)

    def wrap_winrm(self, data: bytes) -> WinRMWrapResult:
        enc_data = self.wrap(data).data
        return WinRMWrapResult(header=enc_data[:16], data=enc_data[16:], padding_length=0)

    def unwrap(self, data: bytes) -> UnwrapResult:
        if not self._handle_in:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        signature = data[:16]
        msg = self._handle_in.update(data[16:])
        self.verify(msg, signature)

        return UnwrapResult(data=msg, encrypted=True, qop=0)

    def unwrap_iov(
        self,
        iov: typing.Iterable[IOV],
    ) -> IOVUnwrapResult:
        if self.context_attr & ContextReq.integrity == 0 and self.context_attr & ContextReq.confidentiality == 0:
            raise OperationNotAvailableError(context_msg="NTLM unwrap without integrity or confidentiality")

        if not self._handle_in or self._sign_key_in is None:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        buffers = self._build_iov_list(iov, lambda b: b)

        # Check if it's a stream + data set of buffers and just call unwrap.
        if (
            len(buffers) == 2
            and buffers[0].type == BufferType.stream
            and buffers[1].type == BufferType.data
            and isinstance(buffers[0].data, bytes)
        ):
            unwrap_res = self.unwrap(buffers[0].data)

            return IOVUnwrapResult(
                (
                    IOVResBuffer(BufferType.stream, buffers[0].data),
                    IOVResBuffer(BufferType.data, unwrap_res.data),
                ),
                encrypted=unwrap_res.encrypted,
                qop=unwrap_res.qop,
            )

        header_idx = -1
        data_idx = -1
        data_sig_idx = -1
        signature_input: typing.List[bytes] = []

        res: typing.List[IOVResBuffer] = []

        for idx, buffer in enumerate(buffers):
            data: bytes

            if not isinstance(buffer.data, bytes):
                raise InvalidTokenError(
                    context_msg=f"unwrap_iov IOV {buffer.type.name} buffer at [{idx}] must be bytes"
                )

            data = buffer.data

            if buffer.type == BufferType.header:
                if header_idx != -1:
                    raise InvalidTokenError(context_msg="unwrap_iov must only be used with 1 header IOV buffer.")

                header_idx = idx

            elif buffer.type == BufferType.data:
                if data_idx != -1:
                    raise InvalidTokenError(context_msg="unwrap_iov must only be used with 1 data IOV buffer.")

                data_idx = idx
                data_sig_idx = len(signature_input)
                signature_input.append(b"")  # Replaced after decryption

            elif buffer.type in [BufferType.sign_only, BufferType.data_readonly]:
                signature_input.append(data)

            else:
                raise InvalidTokenError(context_msg=f"unwrap_iov unsupported IOV buffer type {buffer.type.name}")

            res.append(IOVResBuffer(buffer.type, data))

        if header_idx == -1:
            raise InvalidTokenError(context_msg="unwrap_iov no IOV header buffer present")

        if data_idx == -1:
            raise InvalidTokenError(context_msg="unwrap_iov no IOV data buffer present")

        dec = self._handle_in.update(res[data_idx].data or b"")
        res[data_idx] = IOVResBuffer(BufferType.data, dec)
        signature_input[data_sig_idx] = dec

        self.verify(b"".join(signature_input), res[header_idx].data or b"")

        return IOVUnwrapResult(tuple(res), encrypted=True, qop=0)

    def unwrap_winrm(self, header: bytes, data: bytes) -> bytes:
        if not self._handle_in:
            raise NoContextError(context_msg="Cannot unwrap until context has been established")

        msg = self._handle_in.update(data)
        self.verify(msg, header)

        return msg

    def sign(self, data: bytes, qop: typing.Optional[int] = None) -> bytes:
        if qop:
            raise UnsupportedQop(context_msg="Unsupported QoP value %s specified for NTLM" % qop)

        if not self._handle_out or self._sign_key_out is None:
            raise NoContextError(context_msg="Cannot sign until context has been established")

        return sign(
            self._context_attr,
            self._handle_out,
            self._sign_key_out,
            self._seq_num_out,
            data,
        )

    def verify(self, data: bytes, mic: bytes) -> int:
        if not self._handle_in or self._sign_key_in is None:
            raise NoContextError(context_msg="Cannot verify until context has been established")

        expected_sig = sign(
            self._context_attr,
            self._handle_in,
            self._sign_key_in,
            self._seq_num_in,
            data,
        )

        if expected_sig != mic:
            raise BadMICError(context_msg="Invalid Message integrity Check (MIC) detected")

        return 0

    @property
    def _context_attr_map(self) -> typing.List[typing.Tuple[ContextReq, int]]:
        # https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/a4a41f0d-ca27-44bf-ad1d-6f8c3a3796f2
        return [
            (ContextReq.replay_detect, NegotiateFlags.sign),
            (ContextReq.sequence_detect, NegotiateFlags.sign),
            (ContextReq.confidentiality, NegotiateFlags.seal),
            (ContextReq.integrity, NegotiateFlags.sign),
        ]

    @property
    def _requires_mech_list_mic(self) -> bool:
        # If called before the Authenticate message has been created it force the MIC to be present on the message.
        # When called after the Auth message it will return whether the MIC was generated or not.
        if not self._complete:
            self._mic_required = True
            return False

        return self._mic_required

    @property
    def _seq_num_in(self) -> int:
        if self._context_attr & NegotiateFlags.extended_session_security:
            num = self.__seq_num_in
            self.__seq_num_in += 1

        else:
            num = self.__seq_num_out
            self.__seq_num_out += 1

        return num

    @property
    def _seq_num_out(self) -> int:
        num = self.__seq_num_out
        self.__seq_num_out += 1
        return num

    def _calculate_mic(
        self,
        session_key: bytes,
        negotiate: bytes,
        challenge: bytes,
        authenticate: bytes,
    ) -> bytes:
        """Calculates the MIC value for the negotiated context."""
        return hmac_md5(session_key, negotiate + challenge + authenticate)

    def _compute_response(
        self,
        challenge: Challenge,
        credential: _NTLMCredential,
        channel_bindings: typing.Optional[GssChannelBindings],
    ) -> typing.Tuple[bytes, bytes, bytes]:
        """Compute the NT and LM responses and the key exchange key."""
        client_challenge = os.urandom(8)

        if self._nt_v2:
            target_info = challenge.target_info.copy() if challenge.target_info else TargetInfo()

            if AvId.timestamp in target_info:
                time = target_info[AvId.timestamp]
                self._mic_required = True

            else:
                time = FileTime.now()

            # The docs seem to indicate that a 0'd bindings hash means to ignore it but that does not seem to be the
            # case. Instead only add the bindings if they have been specified by the caller.
            if channel_bindings:
                target_info[AvId.channel_bindings] = md5(channel_bindings.pack())
            target_info[AvId.target_name] = self.spn or ""

            if self._mic_required:
                target_info[AvId.flags] = target_info.get(AvId.flags, AvFlags(0)) | AvFlags.mic

            ntv2_hash = ntowfv2(credential.username or "", credential.nt_hash, credential.domain)
            nt_challenge, lm_challenge, key_exchange_key = compute_response_v2(
                ntv2_hash, challenge.server_challenge, client_challenge, time, target_info
            )

            if self._mic_required:
                lm_challenge = b"\x00" * 24

            return nt_challenge, lm_challenge, key_exchange_key

        else:
            return compute_response_v1(
                challenge.flags,
                credential.nt_hash,
                credential.lm_hash,
                challenge.server_challenge,
                client_challenge,
                no_lm_response=not self._lm,
            )

    def _reset_ntlm_crypto_state(self, outgoing: bool = True) -> None:
        direction = "out" if outgoing else "in"
        getattr(self, f"_handle_{direction}").reset()
