#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABC


class RPCError(Exception, ABC):
    """Base class for all exceptions thrown by :py:mod:`tinyrpc`."""
    def error_respond(self):
        """Converts the error to an error response object.

        :returns: An error response instance or ``None`` if the protocol decides to drop the error silently.
        :rtype: :py:class:`~tinyrpc.protocols.RPCErrorResponse`
        """
        raise NotImplementedError()


class BadRequestError(RPCError, ABC):
    """Base class for all errors that caused the processing of a request to
    abort before a request object could be instantiated."""


class BadReplyError(RPCError, ABC):
    """Base class for all errors that caused processing of a reply to abort
    before it could be turned in a response object."""


class InvalidRequestError(BadRequestError, ABC):
    """A request made was malformed (i.e. violated the specification) and could
    not be parsed."""


class InvalidReplyError(BadReplyError, ABC):
    """A reply received was malformed (i.e. violated the specification) and
    could not be parsed into a response."""


class UnexpectedIDError (InvalidReplyError, ABC):
    """A reply received contained an invalid unique identifier."""


class MethodNotFoundError(RPCError, ABC):
    """The desired method was not found."""


class InvalidParamsError(RPCError, ABC):
    """The provided parameters do not match those of the desired method."""


class ServerError(RPCError, ABC):
    """An internal error in the RPC system occurred."""

class TimeoutError(Exception):
    """No reply received within the timeout period."""
