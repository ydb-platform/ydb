# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import abc
import dataclasses
import enum
import typing
import warnings

from spnego._credential import Credential
from spnego._text import to_text
from spnego.channel_bindings import GssChannelBindings
from spnego.exceptions import FeatureMissingError, NegotiateOptions, SpnegoError
from spnego.iov import BufferType, IOVBuffer, IOVResBuffer

F = typing.TypeVar("F", bound=typing.Callable[..., typing.Any])
NativeIOV = typing.TypeVar("NativeIOV", bound=typing.Any)
IOV = typing.Union[
    IOVBuffer,
    IOVResBuffer,
    typing.Tuple[typing.Union[BufferType, int], typing.Union[bool, bytes, int]],
    typing.Union[BufferType, int],
    bytes,
]


def split_username(username: typing.Optional[str]) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    """Splits a username and returns the domain component.

    Will split a username in the Netlogon form `DOMAIN\\username` and return the domain and user part as separate
    strings. If the user does not contain the `DOMAIN\\` prefix or is in the `UPN` form then then user stays the same
    and the domain is an empty string.

    Args:
        username: The username to split

    Returns:
        Tuple[Optional[str], Optional[str]]: The domain and username.
    """
    if username is None:
        return None, None

    domain: typing.Optional[str]
    if "\\" in username:
        domain, username = username.split("\\", 1)
    else:
        domain = None

    return to_text(domain, nonstring="passthru"), to_text(username, nonstring="passthru")


def wrap_system_error(error_type: typing.Type, context: typing.Optional[str] = None) -> typing.Callable[[F], F]:
    """Wraps a function that makes a native GSSAPI/SSPI syscall and convert native exceptions to a SpnegoError.

    Wraps a function that can potentially raise a WindowsError or GSSError and converts it to the common SpnegoError
    that is exposed by this library. This is to ensure the context proxy functions raise a common set of errors rather
    than a specific error for the provider. The underlying error is preserved in the SpnegoError if the user wishes to
    inspect that.

    Args:
        error_type: The native error type that need to be wrapped.
        context: An optional context message to add to the error if raised.
    """

    def decorator(func: F) -> F:
        def wrapper(*args: typing.Any, **kwargs: typing.Any) -> F:
            try:
                return func(*args, **kwargs)

            except error_type as native_err:
                raise SpnegoError(base_error=native_err, context_msg=context) from native_err

        return typing.cast(F, wrapper)

    return decorator


class WrapResult(typing.NamedTuple):
    """Result of the `wrap()` function."""

    data: bytes  #: The bytes of the wrapped data.
    encrypted: bool  #: Whether the data was encrypted (True) or not (False).


class IOVWrapResult(typing.NamedTuple):
    """Result of the `wrap_iov()` function."""

    buffers: typing.Tuple[IOVResBuffer, ...]  #: The wrapped IOV buffers.
    encrypted: bool  #: Whether the buffer data was encrypted (True) or not (False).


class WinRMWrapResult(typing.NamedTuple):
    """Result of the `wrap_winrm()` function."""

    header: bytes  #: The header of the wrapped result.
    data: bytes  #: The wrapped data included any padding.
    padding_length: int  #: The length of the bytes added to the data for padding.


class UnwrapResult(typing.NamedTuple):
    """Result of the `unwrap()` function."""

    data: bytes  #: The bytes of the unwrapped data.
    encrypted: bool  #: Whether the input data was encrypted (True) or not (False)
    qop: int  #: The Quality of Protection used for the encrypted data.


class IOVUnwrapResult(typing.NamedTuple):
    """Result of the `unwrap_iov()` function."""

    buffers: typing.Tuple[IOVResBuffer, ...]  #: The unwrapped IOV buffers.
    encrypted: bool  #: Whether the input buffers were encrypted (True) or not (False)
    qop: int  #: The Quality of Protection used for the encrypted buffers.


@dataclasses.dataclass(frozen=True)
class SecPkgContextSizes:
    """Sizes of important structures used for messages.

    This dataclass exposes the sizes of important structures used in message
    support functions like wrap, wrap_iov, sign, etc. Use
    :meth:`ContextReq.query_message_sizes` to retrieve this value for an
    authenticated context.

    Currently only ``header`` is exposed but other sizes may be added in the
    future if needed.

    Attributes:
        header: The size of the header/signature of a wrapped token. This
            corresponds to cbSecurityTrailer in SecPkgContext_Sizes in SSPI and
            the size of the allocated GSS_IOV_BUFFER_TYPE_HEADER IOV buffer.
    """

    header: int


class ContextReq(enum.IntFlag):
    none = 0x00000000

    # GSSAPI|SSPI flags
    delegate = 0x00000001
    mutual_auth = 0x00000002
    replay_detect = 0x00000004
    sequence_detect = 0x00000008
    confidentiality = 0x00000010
    integrity = 0x00000020
    # anonymous = 0x00000040  # TODO: Add support for anonymous auth.
    dce_style = 0x00001000
    identify = 0x00002000
    # Requires newer python-gssapi version to support https://github.com/pythongssapi/python-gssapi/pull/218
    delegate_policy = 0x00080000

    # Special flag that disables integrity/confidentiality on Kerberos/Negotiate
    # This should not be set with integrity or confidentiality.
    no_integrity = 0x10000000

    # mutual_auth | replay_detect | sequence_detect | confidentiality | integrity
    default = 0x00000002 | 0x00000004 | 0x00000008 | 0x00000010 | 0x00000020


class GSSMech(str, enum.Enum):
    ntlm = "1.3.6.1.4.1.311.2.2.10"
    spnego = "1.3.6.1.5.5.2"

    # Kerberos has been put under several OIDs over time, we should only be using 'kerberos'.
    kerberos = "1.2.840.113554.1.2.2"  # The actual Kerberos OID, this should be the one used.
    _ms_kerberos = "1.2.840.48018.1.2.2"
    _kerberos_draft = "1.3.5.1.5.2"
    _iakerb = "1.3.6.1.5.2"

    # Not implemented.
    kerberos_u2u = "1.2.840.113554.1.2.2.3"
    negoex = "1.3.6.1.4.1.311.2.2.30"

    @classmethod
    def native_labels(cls) -> typing.Dict[str, str]:
        return {
            GSSMech.ntlm: "NTLM",
            GSSMech.ntlm.value: "NTLM",
            GSSMech.spnego: "SPNEGO",
            GSSMech.spnego.value: "SPNEGO",
            GSSMech.kerberos: "Kerberos",
            GSSMech.kerberos.value: "Kerberos",
            GSSMech._ms_kerberos: "MS Kerberos",
            GSSMech._ms_kerberos.value: "MS Kerberos",
            GSSMech._kerberos_draft: "Kerberos (draft)",
            GSSMech._kerberos_draft.value: "Kerberos (draft)",
            GSSMech._iakerb: "IAKerberos",
            GSSMech._iakerb.value: "IAKerberos",
            GSSMech.kerberos_u2u: "Kerberos User to User",
            GSSMech.kerberos_u2u.value: "Kerberos User to User",
            GSSMech.negoex: "NEGOEX",
            GSSMech.negoex.value: "NEGOEX",
        }

    @property
    def common_name(self) -> str:
        if self.is_kerberos_oid:
            return "kerberos"

        return self.name

    @property
    def is_kerberos_oid(self) -> bool:
        """Determines if the mech is a Kerberos mech.

        Kerberos has been known under serveral OIDs in the past. This tells the caller whether the OID is one of those
        "known" OIDs.

        Returns:
            bool: Whether the mech is a Kerberos mech (True) or not (False).
        """
        return self in [GSSMech.kerberos, GSSMech._ms_kerberos, GSSMech._kerberos_draft, GSSMech._iakerb]

    @staticmethod
    def from_oid(oid: str) -> "GSSMech":
        """Converts an OID string to a GSSMech value.

        Converts an OID string to a GSSMech value if it is known.

        Args:
            oid: The OID as a string to convert from.

        Raises:
            ValueError: if the OID is not a known GSSMech.
        """
        for mech in GSSMech:
            if mech.value == oid:
                return mech
        else:
            raise ValueError("'%s' is not a valid GSSMech OID" % oid)


class ContextProxy(metaclass=abc.ABCMeta):
    """Base class for a authentication context.

    A base class the defined a common entry point for the various authentication context's that are used in this
    library. For a new context to be added it must implement the abstract functions in this class and translate the
    calls to what is required internally.

    Args:
        credentials: A list of credentials to use for authentication.
        hostname: The principal part of the SPN. This is required for Kerberos auth to build the SPN.
        service: The service part of the SPN. This is required for Kerberos auth to build the SPN.
        channel_bindings: The optional :class:`spnego.channel_bindings.GssChannelBindings` for the context.
        context_req: The :class:`spnego.ContextReq` flags to use when setting up the context.
        usage: The usage of the context, `initiate` for a client and `accept` for a server.
        protocol: The protocol to authenticate with, can be `ntlm`, `kerberos`, or `negotiate`. Not all providers
            support all three protocols as that is handled by :class:`SPNEGOContext`.
        options: The :class:`spnego.NegotiateOptions` that define pyspnego specific options to control the negotiation.

    Attributes:
        usage (str): The usage of the context, `initiate` for a client and `accept` for a server.
        protocol (str): The protocol to set the context up with; `ntlm`, `kerberos`, or `negotiate`.
        spn (str): The service principal name of the service to connect to.
        channel_bindings (spnego.channel_bindings.GssChannelBindings): Optional channel bindings to provide with the
            context.
        options (NegotiateOptions): The user specified negotiation options.
        context_req (ContextReq): The context requirements flags as an int value specific to the context provider.
    """

    def __init__(
        self,
        credentials: typing.List[Credential],
        hostname: typing.Optional[str],
        service: typing.Optional[str],
        channel_bindings: typing.Optional[GssChannelBindings],
        context_req: ContextReq,
        usage: str,
        protocol: str,
        options: NegotiateOptions,
    ) -> None:
        self.usage = usage.lower()
        if self.usage not in ["initiate", "accept"]:
            raise ValueError("Invalid usage '%s', must be initiate or accept" % self.usage)

        self.protocol = protocol.lower()
        if self.protocol not in ["ntlm", "kerberos", "negotiate", "credssp"]:
            raise ValueError("Invalid protocol '%s', must be ntlm, kerberos, negotiate, or credssp" % self.protocol)

        if self.protocol not in self.available_protocols(options=options):
            raise ValueError("Protocol %s is not available" % self.protocol)

        self._hostname = hostname
        self._service = service
        self.spn = None
        if service or hostname:
            self.spn = to_text("%s/%s" % (service if service else "HOST", hostname or "unspecified"))

        self.channel_bindings = channel_bindings
        self.options = NegotiateOptions(options)

        self.context_req = context_req  # Generic context requirements.
        self._context_req = 0  # Provider specific context requirements.
        for generic, provider in self._context_attr_map:
            if context_req & generic:
                self._context_req |= provider

        self._context_attr = 0  # Provider specific context attributes, set by self.step().

        # Whether the context is wrapped inside another context - set by NegotiateProxy.
        self._is_wrapped = False

        if options & NegotiateOptions.wrapping_iov and not self.iov_available():
            raise FeatureMissingError(NegotiateOptions.wrapping_iov)

    @property
    def username(self) -> None:
        warnings.warn("username is deprecated", category=DeprecationWarning)
        return None

    @property
    def password(self) -> None:
        warnings.warn("password is deprecated", category=DeprecationWarning)
        return None

    @classmethod
    def available_protocols(cls, options: typing.Optional[NegotiateOptions] = None) -> typing.List[str]:
        """A list of protocols that the provider can offer.

        Returns a list of protocols the underlying provider can implement. Currently only kerberos, negotiate, or ntlm
        is understood. The protocols that are available for each proxy context depend on the OS platform and what
        libraries are installed. See each proxy's `available_protocols` function for more info.

        Args:
            options: The context requirements of :class:`NegotiationOptions` that state what the client requires.

        Returns:
            List[str]: The list of protocols that the context can use.
        """
        return ["kerberos", "negotiate", "ntlm"]  # pragma: no cover

    @classmethod
    def iov_available(cls) -> bool:
        """Whether the context supports IOV wrapping and unwrapping.

        Will return a bool that states whether the context supports IOV wrapping or unwrapping. The NTLM protocol on
        Linux does not support IOV and some Linux gssapi implementations do not expose the extension headers for this
        function. This gives the caller a sane way to determine whether it can use :meth:`wrap_iov` or
        :meth:`unwrap_iov`.

        Returns:
            bool: Whether the context provider supports IOV wrapping and unwrapping (True) or not (False).
        """
        return True  # pragma: no cover

    @property
    @abc.abstractmethod
    def client_principal(self) -> typing.Optional[str]:
        """The principal that was used authenticated by the acceptor.

        The name of the client principal that was used in the authentication context. This is `None` when
        `usage='initiate'` or the context has not been completed. The format of the principal name is dependent on the
        protocol and underlying library that was used to complete the authentication.

        Returns:
            Optional[str]: The client principal name.
        """
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def complete(self) -> bool:
        """Whether the context has completed the authentication process.

        Will return a bool that states whether the authentication process has completed successfully.

        Returns:
            bool: The authentication process is complete (True) or not (False).
        """
        pass  # pragma: no cover

    @property
    def context_attr(self) -> ContextReq:
        """The context attributes that were negotiated.

        This is the context attributes that were negotiated with the counterpart server. These attributes are only
        valid once the context is fully established.

        Returns:
            ContextReq: The flags that were negotiated.
        """
        attr = 0
        for generic, provider in self._context_attr_map:
            if self._context_attr & provider:
                attr |= generic

        return ContextReq(attr)

    @property
    @abc.abstractmethod
    def negotiated_protocol(self) -> typing.Optional[str]:
        """The name of the negotiated protocol.

        Once the authentication process has compeleted this will return the name of the negotiated context that was
        used. For pure NTLM and Kerberos this will always be `ntlm` or `kerberos` respectively but for SPNEGO this can
        be either of those two.

        Returns:
            Optional[str]: The protocol that was negotiated, can be `ntlm`, `kerberos`, or `negotiate. Will be `None`
                for the acceptor until it receives the first token from the initiator. Once the context is establish
                `negotiate` will change to either `ntlm` or `kerberos` to reflect the protocol that was used by SPNEGO.
        """
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def session_key(self) -> bytes:
        """The derived session key.

        Once the authentication process is complete, this will return the derived session key. It is recommended to not
        use this key for your own encryption processes and is only exposed because some libraries use this key in their
        protocols.

        Returns:
            bytes: The derived session key from the authenticated context.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def new_context(self) -> "ContextProxy":
        """Creates a new security context.

        Creates a new security context based on the current credential and
        options of the current context. This is useful when needing to set up a
        new security context without having to retrieve the credentials again.

        Returns:
            ContextProxy: The new security context.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def query_message_sizes(self) -> SecPkgContextSizes:
        """Gets the important structure sizes for message functions.

        Will get the important sizes for the various message functions used by
        the current authentication context. This must only be called once the
        context has been authenticated.

        Returns:
            SecPkgContextSizes: The sizes for the current context.

        Raises:
            NoContextError: The security context is not ready to be queried.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def step(
        self,
        in_token: typing.Optional[bytes] = None,
        *,
        channel_bindings: typing.Optional[GssChannelBindings] = None,
    ) -> typing.Optional[bytes]:
        """Performs a negotiation step.

        This method performs a negotiation step and processes/generates a token. This token should be then sent to the
        counterpart context to continue the authentication process.

        This should not be called once :meth:`complete` is True as the security context is complete.

        For the initiator this is equivalent to `gss_init_sec_context`_ for GSSAPI and `InitializeSecurityContext`_ for
        SSPI.

        For the acceptor this is equivalent to `gss_accept_sec_context`_ for GSSAPI and `AcceptSecurityContext`_ for
        SSPI.

        Args:
            in_token: The input token to process (or None to process no input token).
            channel_bindings: Optional channel bindings ot use in this step. Will take priority over channel bindings
                set in the context if both are specified.

        Returns:
            Optional[bytes]: The output token (or None if no output token is generated.

        .. _gss_init_sec_context:
            https://tools.ietf.org/html/rfc2744.html#section-5.19

        .. _InitializeSecurityContext:
            https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-initializesecuritycontextw

        .. _gss_accept_sec_context:
            https://tools.ietf.org/html/rfc2744.html#section-5.1

        .. _AcceptSecurityContext:
            https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-acceptsecuritycontext
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def wrap(self, data: bytes, encrypt: bool = True, qop: typing.Optional[int] = None) -> WrapResult:
        """Wrap a message, optionally with encryption.

        This wraps a message, signing it and optionally encrypting it. The :meth:`unwrap` will unwrap a message.

        This is the equivalent to `gss_wrap`_ for GSSAPI and `EncryptMessage`_ for SSPI.

        The SSPI function's `EncryptMessage`_ is called with the following buffers::

            SecBufferDesc(SECBUFFER_VERSION, [
                SecBuffer(SECBUFFER_TOKEN, sizes.cbSecurityTrailer, b""),
                SecBuffer(SECBUFFER_DATA, len(data), data),
                SecBuffer(SECBUFFER_PADDING, sizes.cbBlockSize, b""),
            ])

        Args:
            data: The data to wrap.
            encrypt: Whether to encrypt the data (True) or just wrap it with a MIC (False).
            qop: The desired Quality of Protection (or None to use the default).

        Returns:
            WrapResult: The wrapped result which contains the wrapped message and whether it was encrypted or not.

        .. _gss_wrap:
            https://tools.ietf.org/html/rfc2744.html#section-5.33

        .. _EncryptMessage:
            https://docs.microsoft.com/en-us/windows/win32/secauthn/encryptmessage--general
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def wrap_iov(
        self,
        iov: typing.Iterable[IOV],
        encrypt: bool = True,
        qop: typing.Optional[int] = None,
    ) -> IOVWrapResult:
        """Wrap/Encrypt an IOV buffer.

        This method wraps/encrypts an IOV buffer. The IOV buffers control how the data is to be processed. Because
        IOV wrapping is an extension to GSSAPI and not implemented for NTLM on Linux, this method may not always be
        available to the caller. Check the :meth:`iov_available` property.

        This is the equivalent to `gss_wrap_iov`_ for GSSAPI and `EncryptMessage`_ for SSPI.

        Args:
            iov: A list of :class:`spnego.iov.IOVBuffer` buffers to wrap.
            encrypt: Whether to encrypt the message (True) or just wrap it with a MIC (False).
            qop: The desired Quality of Protection (or None to use the default).

        Returns:
            IOVWrapResult: The wrapped result which contains the wrapped IOVBuffer bytes and whether it was encrypted
                or not.

        .. _gss_wrap_iov:
            http://k5wiki.kerberos.org/wiki/Projects/GSSAPI_DCE

        .. _EncryptMessage:
            https://docs.microsoft.com/en-us/windows/win32/secauthn/encryptmessage--general
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def wrap_winrm(self, data: bytes) -> WinRMWrapResult:
        """Wrap/Encrypt data for use with WinRM.

        This method wraps/encrypts bytes for use with WinRM message encryption.

        Args:
            data: The data to wrap.

        Returns:
            WinRMWrapResult: The wrapped result for use with WinRM message encryption.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def unwrap(self, data: bytes) -> UnwrapResult:
        """Unwrap a message.

        This unwraps a message created by :meth:`wrap`.

        This is the equivalent to `gss_unwrap`_ for GSSAPI and `DecryptMessage`_ for SSPI.

        The SSPI function's `DecryptMessage`_ is called with the following buffers::

            SecBufferDesc(SECBUFFER_VERSION, [
                SecBuffer(SECBUFFER_STREAM, len(data), data),
                SecBuffer(SECBUFFER_DATA, 0, b""),
            ])

        Args:
            data: The data to unwrap.

        Returns:
            UnwrapResult: The unwrapped message, whether it was encrypted, and the QoP used.

        .. _gss_unwrap:
            https://tools.ietf.org/html/rfc2744.html#section-5.31

        .. _DecryptMessage:
            https://docs.microsoft.com/en-us/windows/win32/secauthn/decryptmessage--general
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def unwrap_iov(
        self,
        iov: typing.Iterable[IOV],
    ) -> IOVUnwrapResult:
        """Unwrap/Decrypt an IOV buffer.

        This method unwraps/decrypts an IOV buffer. The IOV buffers control how the data is to be processed. Because
        IOV wrapping is an extension to GSSAPI and not implemented for NTLM on Linux, this method may not always be
        available to the caller. Check the :meth:`iov_available` property.

        This is the equivalent to `gss_unwrap_iov`_ for GSSAPI and `DecryptMessage`_ for SSPI.

        Args:
            iov: A list of :class:`spnego.iov.IOVBuffer` buffers to unwrap.

        Returns:
            IOVUnwrapResult: The unwrapped buffer bytes, whether it was encrypted, and the QoP used.

        .. _gss_unwrap_iov:
            http://k5wiki.kerberos.org/wiki/Projects/GSSAPI_DCE

        .. _DecryptMessage:
            https://docs.microsoft.com/en-us/windows/win32/secauthn/decryptmessage--general
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def unwrap_winrm(self, header: bytes, data: bytes) -> bytes:
        """Unwrap/Decrypt a WinRM message.

        This method unwraps/decrypts a WinRM message. It handles the complexities of unwrapping the data and dealing
        with the various system library calls.

        Args:
            header: The header portion of the WinRM wrapped result.
            data: The data portion of the WinRM wrapped result.

        Returns:
            bytes: The unwrapped message.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def sign(self, data: bytes, qop: typing.Optional[int] = None) -> bytes:
        """Generates a signature/MIC for a message.

        This method generates a MIC for the given data. This is unlike wrap which bundles the MIC and the message
        together. The :meth:`verify` method can be used to verify a MIC.

        This is the equivalent to `gss_get_mic`_ for GSSAPI and `MakeSignature`_ for SSPI.

        Args:
            data: The data to generate the MIC for.
            qop: The desired Quality of Protection (or None to use the default).

        Returns:
            bytes: The MIC for the data requested.

        .. _gss_get_mic:
            https://tools.ietf.org/html/rfc2744.html#section-5.15

        .. _MakeSignature:
            https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-makesignature
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def verify(self, data: bytes, mic: bytes) -> int:
        """Verify the signature/MIC for a message.

        Will verify that the given MIC matches the given data. If the MIC does not match the given data, an exception
        will be raised. The :meth:`sign` method can be used to sign data.

        This is the equivalent to `gss_verify_mic`_ for GSSAPI and `VerifySignature`_ for SSPI.

        Args:
            data: The data to verify against the MIC.
            mic: The MIC to verify against the data.

        Returns:
            int: The QoP (Quality of Protection) used.

        .. _gss_verify_mic:
            https://tools.ietf.org/html/rfc2744.html#section-5.32

        .. _VerifySignature:
            https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-verifysignature
        """
        pass  # pragma: no cover

    # Internal properties/functions not for public use.

    @property
    @abc.abstractmethod
    def _context_attr_map(self) -> typing.List[typing.Tuple[ContextReq, int]]:
        """Map the generic ContextReq into the provider specific flags.

        Will return a list of tuples that give the provider specific flag value for the generic ContextReq that is
        exposed to end users.

        Returns:
            List[Tuple[ContextReq, int], ...]: A list of tuples where tuple[0] is the ContextReq flag and tuple[1] is
                the relevant provider specific flag for our common one.
        """
        pass  # pragma: no cover

    def get_extra_info(
        self,
        name: str,
        default: typing.Any = None,
    ) -> typing.Any:
        """Return information about the security context.

        Returns extra information about the security context that is not defined
        as part of the standard :class:`ContextProxy` attributes or properties.
        By default there is no context specific information and it's up to the
        sub classes to implement their own.

        These names can be queried for a CredSSP context.

            client_credential:
                Used on an `acceptor` CredSSP context and contains the delegated
                credential sent by the client to the server. This is only
                available once the context is complete otherwise the default
                value is returned. The types returned can be
                :class:`TSPasswordCreds`, :class:`TSSmartCardCreds`, or
                :class:`TSRemoteGuardCreds`.

            sslcontext:
                The :class:`ssl.SSLContext` instance used for the CredSSP
                context.

            ssl_object:
                The :class:`ssl.SSLObject` instance used for the CredSSP
                context.

            auth_stage - added in 0.5.0:
                A string representing that sub authentication stage being
                performed in the CredSSP authentication stepping. The value
                here is meant to be a human friendly representation and not
                something to be relied upon.

            protocol_version - added in 0.5.0:
                The CredSSP protocol version that was negotiated between the
                initiator and acceptor. This is the minimum version number
                offered by both parties once the Negotiate authentication stage
                is complete.

        Args:
            name: The name/id of the information to retrieve.
            default: The default value to return if the information is not
                available on the current context proxy.

        Args:
            name: The name/id of the information to retrieve.
            default: The default value to return if the information is not
                available on the current context proxy.

        Returns:
            The information requested or the default value specified if the
            information isn't found.
        """
        return default

    @property
    def _requires_mech_list_mic(self) -> bool:
        """Determine if the SPNEGO mechListMIC is required for the sec context.

        When Microsoft hosts deal with NTLM through SPNEGO it always wants the mechListMIC to be present when the NTLM
        authentication message contains a MIC. This goes against RFC 4178 as a mechListMIC shouldn't be required if
        NTLM was the preferred mech from the initiator but we can't do anything about that now. Because we exclusively
        use SSPI on Windows hosts, which does all the work for us, this function only matter for Linux hosts when this
        library manually creates the SPNEGO token.

        The function performs 2 operations. When called before the NTLM authentication message has been created it
        tells the gss-ntlmssp mech that it's ok to generate the MIC. When the authentication message has been created
        it returns a bool stating whether the MIC was present in the auth message and subsequently whether we need to
        include the mechListMIC in the SPNEGO token.

        See `mech_required_mechlistMIC in MIT KRB5`_ for more information about how MIT KRB5 deals with this.

        Returns:
            bool: Whether the SPNEGO mechListMIC needs to be generated or not.

        .. _mech_requires_mechlistMIC:
            https://github.com/krb5/krb5/blob/b2fe66fed560ae28917a4acae6f6c0f020156353/src/lib/gssapi/spnego/spnego_mech.c#L493
        """
        return False  # pragma: no cover

    def _build_iov_list(
        self, iov: typing.Iterable[IOV], native_convert: typing.Callable[[IOVBuffer], NativeIOV]
    ) -> typing.List[NativeIOV]:
        """Creates a list of IOV buffers for the native provider needed."""
        provider_iov: typing.List[NativeIOV] = []

        for entry in iov:
            data: typing.Optional[typing.Union[bytes, int, bool]]
            if isinstance(entry, tuple):
                if len(entry) != 2:
                    raise ValueError("IOV entry tuple must contain 2 values, the type and data, see IOVBuffer.")

                if not isinstance(entry[0], int):
                    raise ValueError("IOV entry[0] must specify the BufferType as an int")
                buffer_type = entry[0]

                if entry[1] is not None and not isinstance(entry[1], (bytes, int, bool)):
                    raise ValueError(
                        "IOV entry[1] must specify the buffer bytes, length of the buffer, or whether "
                        "it is auto allocated."
                    )
                data = entry[1] if entry[1] is not None else b""

            elif isinstance(entry, int):
                buffer_type = entry
                data = None

            elif isinstance(entry, bytes):
                buffer_type = BufferType.data
                data = entry

            else:
                raise ValueError("IOV entry must be a IOVBuffer tuple, int, or bytes")

            iov_buffer = IOVBuffer(type=BufferType(buffer_type), data=data)
            provider_iov.append(native_convert(iov_buffer))

        return provider_iov

    def _reset_ntlm_crypto_state(self, outgoing: bool = True) -> None:
        """Reset the NTLM crypto handles after signing/verifying the SPNEGO mechListMIC.

        `MS-SPNG`_ documents that after signing or verifying the mechListMIC, the RC4 key state needs to be the same
        for the mechListMIC and for the first message signed/sealed by the application. Because we use SSPI on Windows
        hosts which does all the work for us this function only matters for Linux hosts.

        Args:
            outgoing: Whether to reset the outgoing or incoming RC4 key state.

        .. _MS-SPNG:
            https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-spng/b87587b3-9d72-4027-8131-b76b5368115f
        """
        pass  # pragma: no cover
