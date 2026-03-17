from asyncio import TimeoutError


__all__ = (
    "SMTPAuthenticationError",
    "SMTPConnectError",
    "SMTPDataError",
    "SMTPException",
    "SMTPHeloError",
    "SMTPNotSupported",
    "SMTPRecipientRefused",
    "SMTPRecipientsRefused",
    "SMTPResponseException",
    "SMTPSenderRefused",
    "SMTPServerDisconnected",
    "SMTPTimeoutError",
    "SMTPConnectTimeoutError",
    "SMTPReadTimeoutError",
    "SMTPConnectResponseError",
)


class SMTPException(Exception):
    """
    Base class for all SMTP exceptions.
    """

    def __init__(self, message: str, /) -> None:
        self.message = message
        self.args = (message,)


class SMTPServerDisconnected(SMTPException, ConnectionError):
    """
    The connection was lost unexpectedly, or a command was run that requires
    a connection.
    """


class SMTPConnectError(SMTPException, ConnectionError):
    """
    An error occurred while connecting to the SMTP server.
    """


class SMTPTimeoutError(SMTPException, TimeoutError):
    """
    A timeout occurred while performing a network operation.
    """


class SMTPConnectTimeoutError(SMTPTimeoutError, SMTPConnectError):
    """
    A timeout occurred while connecting to the SMTP server.
    """


class SMTPReadTimeoutError(SMTPTimeoutError):
    """
    A timeout occurred while waiting for a response from the SMTP server.
    """


class SMTPNotSupported(SMTPException):
    """
    A command or argument sent to the SMTP server is not supported.
    """


class SMTPResponseException(SMTPException):
    """
    Base class for all server responses with error codes.
    """

    def __init__(self, code: int, message: str, /) -> None:
        self.code = code
        self.message = message
        self.args = (code, message)


class SMTPConnectResponseError(SMTPResponseException, SMTPConnectError):
    """
    The SMTP server returned an invalid response code after connecting.
    """


class SMTPHeloError(SMTPResponseException):
    """
    Server refused HELO or EHLO.
    """


class SMTPDataError(SMTPResponseException):
    """
    Server refused DATA content.
    """


class SMTPAuthenticationError(SMTPResponseException):
    """
    Server refused our AUTH request; may be caused by invalid credentials.
    """


class SMTPSenderRefused(SMTPResponseException):
    """
    SMTP server refused the message sender.
    """

    def __init__(self, code: int, message: str, sender: str, /) -> None:
        self.code = code
        self.message = message
        self.sender = sender
        self.args = (code, message, sender)


class SMTPRecipientRefused(SMTPResponseException):
    """
    SMTP server refused a message recipient.
    """

    def __init__(self, code: int, message: str, recipient: str, /) -> None:
        self.code = code
        self.message = message
        self.recipient = recipient
        self.args = (code, message, recipient)


class SMTPRecipientsRefused(SMTPException):
    """
    SMTP server refused multiple recipients.
    """

    def __init__(self, recipients: list[SMTPRecipientRefused], /) -> None:
        self.recipients = recipients
        self.args = (recipients,)
