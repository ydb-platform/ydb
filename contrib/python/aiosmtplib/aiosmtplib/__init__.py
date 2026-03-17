"""
aiosmtplib
==========

An asyncio SMTP client.

Originally based on smtplib from the Python 3 standard library by:
The Dragon De Monsyne <dragondm@integral.org>

Author: Cole Maclean <hi@colemaclean.dev>
"""

from .api import send
from .errors import (
    SMTPAuthenticationError,
    SMTPConnectError,
    SMTPConnectTimeoutError,
    SMTPDataError,
    SMTPException,
    SMTPHeloError,
    SMTPNotSupported,
    SMTPReadTimeoutError,
    SMTPRecipientRefused,
    SMTPRecipientsRefused,
    SMTPResponseException,
    SMTPSenderRefused,
    SMTPServerDisconnected,
    SMTPTimeoutError,
    SMTPConnectResponseError,
)
from .response import SMTPResponse
from .smtp import SMTP
from .typing import SMTPStatus, SMTPTokenGenerator


__title__ = "aiosmtplib"
__version__ = "5.1.0"
__author__ = "Cole Maclean"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Cole Maclean"
__all__ = (
    "send",
    "SMTP",
    "SMTPResponse",
    "SMTPStatus",
    "SMTPTokenGenerator",
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
