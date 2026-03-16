from __future__ import annotations


class WinRMError(Exception):
    """ "Generic WinRM error"""

    code = 500


class WSManFaultError(WinRMError):
    """WSMan Fault Error.

    Exception that is raised when receiving a WSMan fault message. It
    contains the raw response as well as the fault details parsed from the
    response.

    The wsman_fault_code is returned by the Microsoft WSMan server rather than
    the WSMan protocol error code strings. The wmierror_code can contain more
    fatal service error codes returned as a MSFT_WmiError object, for example
    quota violations.

    @param int code: The HTTP status code of the response.
    @param str message: The error message.
    @param str response: The raw WSMan response text.
    @param str reason: The WSMan fault reason.
    @param string fault_code: The WSMan fault code.
    @param string fault_subcode: The WSMan fault subcode.
    @param int wsman_fault_code: The MS WSManFault specific code.
    @param int wmierror_code: The MS WMI error code.
    """

    def __init__(
        self,
        code: int,
        message: str,
        response: str,
        reason: str,
        fault_code: str | None = None,
        fault_subcode: str | None = None,
        wsman_fault_code: int | None = None,
        wmierror_code: int | None = None,
    ) -> None:
        self.code = code
        self.response = response
        self.fault_code = fault_code
        self.fault_subcode = fault_subcode
        self.reason = reason
        self.wsman_fault_code = wsman_fault_code
        self.wmierror_code = wmierror_code

        # Using the dict repr is for backwards compatibility.
        fault_data = {
            "transport_message": message,
            "http_status_code": code,
        }
        if wsman_fault_code is not None:
            fault_data["wsmanfault_code"] = wsman_fault_code

        if fault_code is not None:
            fault_data["fault_code"] = fault_code

        if fault_subcode is not None:
            fault_data["fault_subcode"] = fault_subcode

        super().__init__("{0} (extended fault data: {1})".format(reason, fault_data))


class WinRMTransportError(Exception):
    """WinRM errors specific to transport-level problems (unexpected HTTP error codes, etc)"""

    @property
    def protocol(self) -> str:
        return self.args[0]

    @property
    def code(self) -> int:
        return self.args[1]

    @property
    def message(self) -> str:
        return "Bad HTTP response returned from server. Code {0}".format(self.code)

    @property
    def response_text(self) -> str:
        return self.args[2]

    def __str__(self) -> str:
        return self.message


class WinRMOperationTimeoutError(Exception):
    """
    Raised when a WinRM-level operation timeout (not a connection-level timeout) has occurred. This is
    considered a normal error that should be retried transparently by the client when waiting for output from
    a long-running process.
    """

    code = 500


class AuthenticationError(WinRMError):
    """Authorization Error"""

    code = 401


class BasicAuthDisabledError(AuthenticationError):
    message = "WinRM/HTTP Basic authentication is not enabled on remote host"


class InvalidCredentialsError(AuthenticationError):
    pass
