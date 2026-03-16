#!/usr/bin/env python
# -*- coding: utf-8 -*-

class RPCError(Exception):
    """Base class for all excetions thrown by :py:mod:`tinyrpc`."""


class BadRequestError(RPCError):
    """Base class for all errors that caused the processing of a request to
    abort before a request object could be instantiated."""

    def error_respond(self):
        """Create :py:class:`~tinyrpc.RPCErrorResponse` to respond the error.

        :return: A error responce instance or ``None``, if the protocol decides
                 to drop the error silently."""
        raise RuntimeError('Not implemented')


class BadReplyError(RPCError):
    """Base class for all errors that caused processing of a reply to abort
    before it could be turned in a response object."""


class InvalidRequestError(BadRequestError):
    """A request made was malformed (i.e. violated the specification) and could
    not be parsed."""


class InvalidReplyError(BadReplyError):
    """A reply received was malformed (i.e. violated the specification) and
    could not be parsed into a response."""


class MethodNotFoundError(RPCError):
    """The desired method was not found."""


class ServerError(RPCError):
    """An internal error in the RPC system occured."""
