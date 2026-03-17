# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import typing

GSSError: typing.Type[Exception]
try:
    from gssapi.raw import GSSError  # type: ignore
except ImportError as e:
    GSSError = Exception

WinError: typing.Type[Exception]
try:
    WinError = WindowsError  # type: ignore
except NameError:
    WinError = Exception


class NativeError(Exception):
    """Stub for a native error that can be used to generate a SpnegoError from a known platform native code."""

    def __init__(self, msg: str, **kwargs: typing.Any) -> None:
        self.msg = msg
        for key in ["maj_code", "winerror"]:
            if key in kwargs:
                setattr(self, key, kwargs[key])


class NegotiateOptions(enum.IntFlag):
    """Flags that the caller can use to control the negotiation behaviour.

    A list of features as bit flags that the caller can specify when creating the security context. These flags can
    be used on both Windows or Linux but are a no-op on Windows as it should always have the same features available.
    On Linux the features it can implement depend on a wide range of factors like the system libraries/headers that
    are installed, what GSSAPI implementation is present, and what Python libraries are available.

    This is a pretty advanced feature and is mostly a way to control the kerberos to ntlm fallback behaviour on Linux.
    The `use_*` options when combined with `protocol='credssp'` control the underlying auth proxy provider that is used
    in the CredSSP authentication process and not the parent context proxy the user interacts with.

    These are the currently implemented feature flags:

    use_sspi:
        Ensures the context proxy used is :class:`spnego._sspi.SSPIProxy`.

    use_gssapi:
        Ensures the context proxy used is :class:`spnego._gss.GSSAPIProxy`.

    use_negotiate:
        Ensures the context proxy used is :class:`spnego._negotiate.NegotiateProxy`.

    use_ntlm:
        Ensures the context proxy used is :class:`spnego._ntlm.NTLMProxy`.

    negotiate_kerberos:
        Will make sure that Kerberos is at least available to try for authentication when using the `negotiate`
        protocol. If Kerberos cannot be used due to the Python gssapi library not being installed then it will raise a
        :class:`spnego.exceptions.FeatureMissingError`. If Kerberos was available but it cannot get a credential or
        create a context then it will just fallback to NTLM auth. If you wish to only use Kerberos with no NTLM
        fallback, set `protocol='kerberos'` when creating the security context.

    session_key:
        Ensure that the authenticated context will be able to return the session key that was negotiated between the
        client and the server. Older versions of `gss-ntlmssp`_ do not expose the functions required to retrieve this
        info so when this feature flag is set then the NTLM fallback process will use a builtin NTLM process and not
        `gss-ntlmssp`_ if the latter is too old to retrieve the session key. Cannot be used in combination with
        `protocol='credssp'` as CredSSP does not provide a session key.

    wrapping_iov:
        The GSSAPI IOV methods are extensions to the Kerberos spec and not implemented or exposed on all platforms,
        macOS is a popular example. If the caller requires the wrap_iov and unwrap_iov methods this will ensure it
        fails fast before the auth has been set up. Unfortunately there is no fallback for this as if the headers
        aren't present for GSSAPI then we can't do anything to fix that. This won't fail if `negotiate` was used and
        NTLM was the chosen protocol as that happens post negotiation.

    wrapping_winrm:
        To created a wrapped WinRM message the IOV extensions are required when using Kerberos auth. Setting this flag
        will skip Kerberos when `protocol='negotiate'` if the IOV headers aren't present and just fallback to NTLM.

    .. _gss-ntlmssp:
        https://github.com/gssapi/gss-ntlmssp
    """

    none = 0x00000000

    # Force a specific provider
    use_sspi = 0x00000001
    use_gssapi = 0x00000002
    use_negotiate = 0x00000004
    use_ntlm = 0x00000008

    negotiate_kerberos = 0x00000010
    session_key = 0x00000020
    wrapping_iov = 0x00000040
    wrapping_winrm = 0x00000080


class FeatureMissingError(Exception):
    @property
    def feature_id(self) -> NegotiateOptions:
        return self.args[0]

    @property
    def message(self) -> str:
        msg = {
            NegotiateOptions.negotiate_kerberos: "The Python gssapi library is not installed so Kerberos cannot be "
            "negotiated.",
            NegotiateOptions.wrapping_iov: "The system is missing the GSSAPI IOV extension headers or CredSSP is "
            "being requested, cannot utilize wrap_iov and unwrap_iov",
            NegotiateOptions.wrapping_winrm: "The system is missing the GSSAPI IOV extension headers required for "
            "WinRM encryption with Kerberos.",
            NegotiateOptions.session_key: "The protocol selected does not support getting the session key.",
        }.get(self.feature_id, "Unknown option flag: %d" % self.feature_id)

        return msg

    def __str__(self) -> str:
        return self.message


class ErrorCode(enum.IntEnum):
    """Common error codes for SPNEGO operations.

    Mostly a copy of the `GSS major error codes`_ with the names made more pythonic. Not all codes have a corresponding
    SpnegoError class as they are reserved for the codes that apply to both GSSAPI and SSPI.

    .. _GSS major error codes:
        https://docs.oracle.com/cd/E19683-01/816-1331/reference-4/index.html
    """

    bad_mech = 1  # BadMechanismError
    bad_name = 2  # BadNameError
    # bad_nametype = 3  # No equivalent in SSPI, shouldn't happen in our code as well.
    bad_bindings = 4  # BadBindings
    # bad_status = 5  # Only used in gss_display_status which we don't care about.
    bad_mic = 6  # BadMICError
    no_cred = 7  # NoCredentialError
    no_context = 8  # NoContextError
    invalid_token = 9  # InvalidTokenError
    invalid_credential = 10  # InvalidCredentialError
    credentials_expired = 11  # CredentialsExpiredError
    context_expired = 12  # ContextExpiredError
    failure = 13  # This is a generic error with the error coming from the minor code, uses SpnegoError directly.
    bad_qop = 14  # UnsupportedQop
    # unauthorized = 15  # Shouldn't happen in our code, we don't do any authorization functions.
    unavailable = 16  # OperationNotAvailableError
    # duplicate_element = 17  # Shouldn't happen in our code.
    # name_not_mn = 18  # Shouldn't happen in our code.


# Implementation is inspired by the python-gssapi project https://github.com/pythongssapi/python-gssapi.
# https://github.com/pythongssapi/python-gssapi/blob/826c02de1c1885896924bf342c60087f369c6b1a/gssapi/raw/misc.pyx#L180
class _SpnegoErrorRegistry(type):
    __registry: typing.Dict[int, typing.Type] = {}
    __gssapi_map: typing.Dict[int, int] = {}
    __sspi_map: typing.Dict[int, int] = {}

    def __init__(
        cls,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        # Load up the registry with the instantiated class so we can look it up when creating a SpnegoError.
        error_code = getattr(cls, "ERROR_CODE", None)

        if error_code is not None and error_code not in cls.__registry:
            cls.__registry[error_code] = cls

        # Map the system error codes to the common spnego error code.
        for system_attr, mapping in [("_GSSAPI_CODE", cls.__gssapi_map), ("_SSPI_CODE", cls.__sspi_map)]:
            codes = getattr(cls, system_attr, None)

            if codes is None:
                continue

            if not isinstance(codes, (list, tuple)):
                codes = [codes]

            for c in codes:
                mapping[c] = error_code or 0

    def __call__(
        cls,
        error_code: typing.Optional[int] = None,
        base_error: typing.Optional[Exception] = None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "_SpnegoErrorRegistry":
        error_code = error_code if error_code is not None else getattr(cls, "ERROR_CODE", None)

        if error_code is None:
            if not base_error:
                raise ValueError("%s requires either an error_code or base_error" % cls.__name__)

            # GSSError
            maj_code = getattr(base_error, "maj_code", None)
            # WindowsError
            winerror = getattr(base_error, "winerror", None)

            if maj_code is not None:
                error_code = cls.__gssapi_map.get(maj_code, None)

            elif winerror is not None:
                error_code = cls.__sspi_map.get(winerror, None)

            else:
                raise ValueError(
                    "base_error of type '%s' is not supported, must be a gssapi.exceptions.GSSError or "
                    "WindowsError" % type(base_error).__name__
                )

        new_cls = cls.__registry.get(error_code or 0, cls)
        return super(_SpnegoErrorRegistry, new_cls).__call__(
            error_code,  # type: ignore[arg-type] # I cannot understand this, seems to be bug?
            base_error,
            *args,
            **kwargs,
        )


class SpnegoError(Exception, metaclass=_SpnegoErrorRegistry):
    """Common error for SPNEGO exception.

    Creates an common error record for SPNEGO errors raised by pyspnego. This error record can wrap system level error
    records raised by GSSAPI or SSPI and wrap them into a common error record across the various platforms. While this
    reflects the GSSAPI major codes that can be raised, it is up to the GSSAPI platform to conform to those error
    codes. Some platforms like MIT krb5 always report `GSS_S_FAILURE` and use the minor code to report the actual
    error message.

    Args:
        error_code: The ErrorCode for the error, this must be set if base_error is not set.
        base_error: The system level error from SSPI or GSSAPI, this must be set if error_code is not set.
        context_msg: Optional message to provide more context around the error.

    Attributes:
        base_error (Optional[Union[GSSError, WinError]]): The system level error if one was provided.
    """

    # Classes the subclass this type need to provide the following class attribute:
    #
    # ERROR_CODE = common ErrorCode value for the exception
    # _BASE_MESSAGE = common string that explains the error code in the absence of the system error message.
    #
    # The following attributes are used to map specific system error codes to the common ErrorCode error.
    # _GSSAPI_CODE = The GSSAPI major_code from GSSError to map to the common error code
    # _SSPI_CODE = The winerror value from an WindowsError to map to the common error code

    def __init__(
        self,
        error_code: typing.Optional[typing.Union[int, ErrorCode]] = None,
        base_error: typing.Optional[Exception] = None,
        context_msg: typing.Optional[str] = None,
    ) -> None:
        self.base_error = base_error
        self._error_code = error_code
        self._context_message = context_msg

        super(SpnegoError, self).__init__(self.message)

    @property
    def nt_status(self) -> int:
        """The Windows NT Status code that represents this error."""
        codes = getattr(self, "_SSPI_CODE", self._error_code) or 0xFFFFFFFF
        return codes[0] if isinstance(codes, (list, tuple)) else codes

    @property
    def message(self) -> str:
        error_code = self._error_code if self._error_code is not None else 0xFFFFFFFF

        if self.base_error:
            base_message = str(self.base_error)

        else:
            base_message = getattr(self, "_BASE_MESSAGE", "Unknown error code")

        msg = "SpnegoError (%d): %s" % (error_code, base_message)
        if self._context_message:
            msg += ", Context: %s" % self._context_message

        return msg


class BadMechanismError(SpnegoError):
    ERROR_CODE = ErrorCode.bad_mech

    _BASE_MESSAGE = "An unsupported mechanism was requested"
    _GSSAPI_CODE = 65536  # GSS_S_BAD_MECH
    _SSPI_CODE = -2146893051  # SEC_E_SECPKG_NOT_FOUND


class BadNameError(SpnegoError):
    ERROR_CODE = ErrorCode.bad_name

    _BASE_MESSAGE = "An invalid name was supplied"
    _GSSAPI_CODE = 1310722  # GSS_S_BAD_NAME
    _SSPI_CODE = -2146893053  # SEC_E_TARGET_UNKNOWN


class BadBindingsError(SpnegoError):
    ERROR_CODE = ErrorCode.bad_bindings

    _BASE_MESSAGE = "Invalid channel bindings"
    _GSSAPI_CODE = 262144  # GSS_BAD_BINDINGS
    _SSPI_CODE = -2146892986  # SEC_E_BAD_BINDINGS


class BadMICError(SpnegoError):
    ERROR_CODE = ErrorCode.bad_mic

    _BASE_MESSAGE = "A token had an invalid Message Integrity Check (MIC)"
    _GSSAPI_CODE = 3932166  # GSS_BAD_MIC
    _SSPI_CODE = -2146893041  # SEC_E_MESSAGE_ALTERED


class NoCredentialError(SpnegoError):
    ERROR_CODE = ErrorCode.no_cred

    _BASE_MESSAGE = "No credentials were supplied, or the credentials were unavailable or inaccessible"
    _GSSAPI_CODE = 458752  # GSS_NO_CRED
    _SSPI_CODE = -2146893042  # SEC_E_NO_CREDENTIALS


class NoContextError(SpnegoError):
    ERROR_CODE = ErrorCode.no_context

    _BASE_MESSAGE = "No context has been established, or invalid handled passed in"
    _GSSAPI_CODE = 524288  # GSS_S_NO_CONTEXT
    _SSPI_CODE = -2146893055  # SEC_E_INVALID_HANDLE


class InvalidTokenError(SpnegoError):
    ERROR_CODE = ErrorCode.invalid_token

    _BASE_MESSAGE = "A token was invalid, or the logon was denied"
    _GSSAPI_CODE = 589824  # GSS_S_DEFECTIVE_TOKEN
    _SSPI_CODE = [-2146893044, -2146893048]  # SEC_E_LOGON_DENIED, SEC_E_INVALID_TOKEN


class InvalidCredentialError(SpnegoError):
    ERROR_CODE = ErrorCode.invalid_credential

    _BASE_MESSAGE = "A credential was invalid"
    _GSSAPI_CODE = 655360  # GSS_S_DEFECTIVE_CREDENTIAL
    _SSPI_CODE = -1073741715  # STATUS_LOGON_FAILURE


class CredentialsExpiredError(SpnegoError):
    ERROR_CODE = ErrorCode.credentials_expired

    _BASE_MESSAGE = "The referenced credentials have expired"
    _GSSAPI_CODE = 720896  # * GSS_S_CREDENTIALS_EXPIRED
    _SSPI_CODE = -1073741711  # STATUS_PASSWORD_EXPIRED


class ContextExpiredError(SpnegoError):
    ERROR_CODE = ErrorCode.context_expired

    _BASE_MESSAGE = "Security context has expired"
    _GSSAPI_CODE = 786432  # GSS_S_CONTEXT_EXPIRED
    _SSPI_CODE = -2146893033  # SEC_E_CONTEXT_EXPIRED


class UnsupportedQop(SpnegoError):
    ERROR_CODE = ErrorCode.bad_qop

    _BASE_MESSAGE = "The quality-of-protection requested could not be provided"
    _GSSAPI_CODE = 917504  # GSS_S_BAD_QOP
    _SSPI_CODE = -2146893046  # SEC_E_QOP_NOT_SUPPORTED


class OperationNotAvailableError(SpnegoError):
    ERROR_CODE = ErrorCode.unavailable

    _BASE_MESSAGE = "Operation not supported or available"
    _GSSAPI_CODE = 1048576  # GSS_S_UNAVAILABLE
    _SSPI_CODE = -2146893054  # SEC_E_UNSUPPORTED_FUNCTION
