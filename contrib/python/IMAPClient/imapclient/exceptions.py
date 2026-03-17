import imaplib

# Base class allowing to catch any IMAPClient related exceptions
# To ensure backward compatibility, we "rename" the imaplib general
# exception class, so we can catch its exceptions without having to
# deal with it in IMAPClient codebase

IMAPClientError = imaplib.IMAP4.error
IMAPClientAbortError = imaplib.IMAP4.abort
IMAPClientReadOnlyError = imaplib.IMAP4.readonly


class CapabilityError(IMAPClientError):
    """
    The command tried by the user needs a capability not installed
    on the IMAP server
    """


class LoginError(IMAPClientError):
    """
    A connection has been established with the server but an error
    occurred during the authentication.
    """


class IllegalStateError(IMAPClientError):
    """
    The command tried needs a different state to be executed. This
    means the user is not logged in or the command needs a folder to
    be selected.
    """


class InvalidCriteriaError(IMAPClientError):
    """
    A command using a search criteria failed, probably due to a syntax
    error in the criteria string.
    """


class ProtocolError(IMAPClientError):
    """The server replied with a response that violates the IMAP protocol."""
