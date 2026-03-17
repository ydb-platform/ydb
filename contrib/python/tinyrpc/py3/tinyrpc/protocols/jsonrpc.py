#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""JSON RPC 2.0 Protocol implementation.

This module can use the jsonext_ package to make it easier to convert to JSON.
In order to use jsonext import it before importing tinyrpc.
Tinyrpc will detect the presence of jsonext and use it automatically.

.. _jsonext: https://pypi.org/project/jsonext

"""

import json
import sys
from tinyrpc.exc import UnexpectedIDError
from typing import Dict, Any, Union, Optional, List, Tuple, Callable, Generator

from . import default_id_generator
from .. import (
    RPCBatchProtocol, RPCRequest, RPCResponse, RPCErrorResponse,
    InvalidRequestError, MethodNotFoundError, InvalidReplyError, RPCError,
    RPCBatchRequest, RPCBatchResponse, InvalidParamsError,
)

if 'jsonext' in sys.modules:
    # jsonext was imported before this file, assume the intent is that
    # it is used in place of the regular json encoder.
    import jsonext

    json_dumps = jsonext.dumps
else:
    json_dumps = json.dumps


class FixedErrorMessageMixin(object):
    """Combines JSON RPC exceptions with the generic RPC exceptions.

    Constructs the exception using the provided parameters as well as
    properties of the JSON RPC Exception.

    JSON RPC exceptions declare two attributes:

    .. py:attribute:: jsonrpc_error_code

        This is an error code conforming to the JSON RPC `error codes`_ convention.

        :type: :py:class:`int`

    .. py:attribute:: message

        This is a textual representation of the error code.

        :type: :py:class:`str`

    :param list args: Positional arguments for the constructor.
        When present it overrules the :py:attr:`message` attribute.
    :param dict kwargs: Keyword arguments for the constructor.
        If the ``data`` parameter is found in ``kwargs`` its contents are
        used as the *data* property of the JSON RPC Error object.

    :py:class:`FixedErrorMessageMixin` is the basis for adding your own
    exceptions to the predefined ones.
    Here is a version of the reverse string example that dislikes palindromes:

    .. code-block:: python

        class PalindromeError(FixedErrorMessageMixin, Exception)
            jsonrpc_error_code = 99
            message = "Ah, that's cheating"

        @public
        def reverse_string(s):
            r = s[::-1]
            if r == s:
                raise PalindromeError(data=s)
            return r

    >>> client.reverse('rotator')

    Will return an error object to the client looking like:

    .. code-block:: json

        {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": 99,
                "message": "Ah, that's cheating",
                "data": "rotator"
            }
        }

    .. _error codes: https://www.jsonrpc.org/specification#error_object
    """
    def __init__(self, *args, **kwargs) -> None:
        if not args:
            args = [self.message]
        self.request_id = kwargs.pop('request_id', None)
        if 'data' in kwargs:
            self.data = kwargs.pop('data')
        super(FixedErrorMessageMixin, self).__init__(*args, **kwargs)

    def error_respond(self) -> 'JSONRPCErrorResponse':
        """Converts the error to an error response object.

        :return: An error response object ready to be serialized and sent to the client.
        :rtype: :py:class:`JSONRPCErrorResponse`
        """
        response = JSONRPCErrorResponse()

        response.error = self.message
        response.unique_id = self.request_id
        response._jsonrpc_error_code = self.jsonrpc_error_code
        if hasattr(self, 'data'):
            response.data = self.data
        return response


class JSONRPCParseError(FixedErrorMessageMixin, InvalidRequestError):
    """The request cannot be decoded or is malformed."""
    jsonrpc_error_code = -32700
    message = 'Parse error'


class JSONRPCInvalidRequestError(FixedErrorMessageMixin, InvalidRequestError):
    """The request contents are not valid for JSON RPC 2.0"""
    jsonrpc_error_code = -32600
    message = 'Invalid Request'


class JSONRPCMethodNotFoundError(FixedErrorMessageMixin, MethodNotFoundError):
    """The requested method name is not found in the registry."""
    jsonrpc_error_code = -32601
    message = 'Method not found'


class JSONRPCInvalidParamsError(FixedErrorMessageMixin, InvalidRequestError):
    """The provided parameters are not appropriate for the function called."""
    jsonrpc_error_code = -32602
    message = 'Invalid params'


class JSONRPCInternalError(FixedErrorMessageMixin, InvalidRequestError):
    """Unspecified error, not in the called function."""
    jsonrpc_error_code = -32603
    message = 'Internal error'


class JSONRPCServerError(FixedErrorMessageMixin, InvalidRequestError):
    """Unspecified error, this message originates from the called function."""
    jsonrpc_error_code = -32000
    message = ''


class JSONRPCError(FixedErrorMessageMixin, RPCError):
    """Reconstructs (to some extend) the server-side exception.

    The client creates this exception by providing it with the ``error``
    attribute of the JSON error response object returned by the server.

    :param dict error: This dict contains the error specification:

        * code (int): the numeric error code.
        * message (str): the error description.
        * data (any): if present, the data attribute of the error
    """
    def __init__(
            self, error: Union['JSONRPCErrorResponse', Dict[str, Any]]
    ) -> None:
        if isinstance(error, JSONRPCErrorResponse):
            super(JSONRPCError, self).__init__(error.error)
            self.message = error.error
            self._jsonrpc_error_code = error._jsonrpc_error_code
            if hasattr(error, 'data'):
                self.data = error.data
        else:
            super(JSONRPCError, self).__init__(error.message)
            self.message = error['message']
            self._jsonrpc_error_code = error['code']
            if 'data' in error:
                self.data = error['data']


class JSONRPCSuccessResponse(RPCResponse):
    """Collects the attributes of a successful response message.

    Contains the fields of a normal (i.e. a non-error) response message.

    .. py:attribute:: unique_id

        Correlation ID to match request and response.
        A JSON RPC response *must* have a defined matching id attribute.
        ``None`` is not a valid value for a successful response.

        :type: str or int

    .. py:attribute:: result

        Contains the result of the RPC call.

        :type: Any type that can be serialized by the protocol.
    """
    def _to_dict(self):
        return {
            'jsonrpc': JSONRPCProtocol.JSON_RPC_VERSION,
            'id': self.unique_id,
            'result': self.result
        }

    def serialize(self) -> bytes:
        """Returns a serialization of the response.

        Converts the response into a bytes object that can be passed to and by the transport layer.

        :return: The serialized encoded response object.
        :rtype: bytes
        """
        return json_dumps(self._to_dict()).encode()


class JSONRPCErrorResponse(RPCErrorResponse):
    """Collects the attributes of an error response message.

    Contains the fields of an error response message.

    .. py:attribute:: unique_id

        Correlation ID to match request and response.
        ``None`` is a valid ID when the error cannot be matched to a particular request.

        :type: str or int or None

    .. py:attribute:: error

        The error message. A string describing the error condition.

        :type: str

    .. py:attribute:: data

        This field may contain any JSON encodable datum that the server may want
        to return the client.

        It may contain additional information about the error condition, a partial result
        or whatever. Its presence and value are entirely optional.

        :type: Any type that can be serialized by the protocol.

    .. py:attribute:: _jsonrpc_error_code

        The numeric error code.

        The value is usually predefined by one of the JSON protocol exceptions.
        It can be set by the developer when defining application specific exceptions.
        See :py:class:`FixedErrorMessageMixin` for an example on how to do this.

        Note that the value of this field *must* comply with the defined values in
        the standard_.

    .. _standard: https://www.jsonrpc.org/specification#error_object
    """
    def _to_dict(self):
        msg = {
            'jsonrpc': JSONRPCProtocol.JSON_RPC_VERSION,
            'id': self.unique_id,
            'error': {
                'message': str(self.error),
                'code': self._jsonrpc_error_code
            }
        }
        if hasattr(self, 'data'):
            msg['error']['data'] = self.data
        return msg

    def serialize(self) -> bytes:
        """Returns a serialization of the error.

        Converts the response into a bytes object that can be passed to and by the transport layer.

        :return: The serialized encoded error object.
        :rtype: bytes
        """
        return json_dumps(self._to_dict()).encode()


def _get_code_message_and_data(error: Union[Exception, str]
                               ) -> Tuple[int, str, Any]:
    assert isinstance(error, (Exception, str))
    data = None
    if isinstance(error, Exception):
        if hasattr(error, 'jsonrpc_error_code'):
            code = error.jsonrpc_error_code
            msg = str(error)
            try:
                data = error.data
            except AttributeError:
                pass
        elif isinstance(error, InvalidRequestError):
            code = JSONRPCInvalidRequestError.jsonrpc_error_code
            msg = JSONRPCInvalidRequestError.message
        elif isinstance(error, MethodNotFoundError):
            code = JSONRPCMethodNotFoundError.jsonrpc_error_code
            msg = JSONRPCMethodNotFoundError.message
        elif isinstance(error, InvalidParamsError):
            code = JSONRPCInvalidParamsError.jsonrpc_error_code
            msg = JSONRPCInvalidParamsError.message
        else:
            # allow exception message to propagate
            code = JSONRPCServerError.jsonrpc_error_code
            if len(error.args) == 2:
                msg = str(error.args[0])
                data = error.args[1]
            else:
                msg = str(error)
    else:
        code = -32000
        msg = error

    return code, msg, data


class JSONRPCRequest(RPCRequest):
    """Defines a JSON RPC request."""
    def __init__(self):
        super().__init__()
        self.one_way = False
        """Request or Notification.

        :type: bool

        This flag indicates if the client expects to receive a reply (request: ``one_way = False``)
        or not (notification: ``one_way = True``).

        Note that according to the specification it is possible for the server to return an error response.
        For example if the request becomes unreadable and the server is not able to determine that it is
        in fact a notification an error should be returned. However, once the server had verified that the
        request is a notification no reply (not even an error) should be returned.
        """

        self.unique_id = None
        """Correlation ID used to match request and response.

        :type: int or str

        Generated by the client, the server copies it from request to corresponding response.
        """

        self.method = None
        """The name of the RPC function to be called.

        :type: str

        The :py:attr:`method` attribute uses the name of the function as it is known by the public.
        The :py:class:`~tinyrpc.dispatch.RPCDispatcher` allows the use of public aliases in the
        ``@public`` decorators.
        These are the names used in the :py:attr:`method` attribute.
        """

        self.args = []
        """The positional arguments of the method call.

        :type: list

        The contents of this list are the positional parameters for the :py:attr:`method` called.
        It is eventually called as ``method(*args)``.
        """

        self.kwargs = {}
        """The keyword arguments of the method call.

        :type: dict

        The contents of this dict are the keyword parameters for the :py:attr:`method` called.
        It is eventually called as ``method(**kwargs)``.
        """
    def error_respond(self, error: Union[Exception, str]
                      ) -> Optional['JSONRPCErrorResponse']:
        """Create an error response to this request.

        When processing the request produces an error condition this method can be used to
        create the error response object.

        :param error: Specifies what error occurred.
        :type error: Exception or str
        :returns: An error response object that can be serialized and sent to the client.
        :rtype: ;py:class:`JSONRPCErrorResponse`
        """
        if self.unique_id is None:
            return None

        response = JSONRPCErrorResponse()
        response.unique_id = None if self.one_way else self.unique_id

        code, msg, data = _get_code_message_and_data(error)

        response.error = msg
        response._jsonrpc_error_code = code
        if data:
            response.data = data
        return response

    def respond(self, result: Any) -> Optional['JSONRPCSuccessResponse']:
        """Create a response to this request.

        When processing the request completed successfully this method can be used to
        create a response object.

        :param result: The result of the invoked method.
        :type result: Anything that can be encoded by JSON.
        :returns: A response object that can be serialized and sent to the client.
        :rtype: :py:class:`JSONRPCSuccessResponse`
        """
        if self.one_way or self.unique_id is None:
            return None

        response = JSONRPCSuccessResponse()

        response.result = result
        response.unique_id = self.unique_id

        return response

    def _to_dict(self):
        jdata = {
            'jsonrpc': JSONRPCProtocol.JSON_RPC_VERSION,
            'method': self.method,
        }
        if self.args:
            jdata['params'] = self.args
        if self.kwargs:
            jdata['params'] = self.kwargs
        if not self.one_way and hasattr(
                self, 'unique_id') and self.unique_id is not None:
            jdata['id'] = self.unique_id
        return jdata

    def serialize(self) -> bytes:
        """Returns a serialization of the request.

        Converts the request into a bytes object that can be sent to the server.

        :return: The serialized encoded request object.
        :rtype: bytes
        """
        return json_dumps(self._to_dict()).encode()


class JSONRPCBatchRequest(RPCBatchRequest):
    """Defines a JSON RPC batch request."""
    def create_batch_response(self) -> Optional['JSONRPCBatchResponse']:
        """Produces a batch response object if a response is expected.

        :return: A batch response if needed
        :rtype: :py:class:`JSONRPCBatchResponse`
        """
        if self._expects_response():
            return JSONRPCBatchResponse()

    def _expects_response(self):
        for request in self:
            if isinstance(request, Exception):
                return True
            if not request.one_way and request.unique_id is not None:
                return True

        return False

    def serialize(self) -> bytes:
        """Returns a serialization of the request.

        Converts the request into a bytes object that can be passed to and by the transport layer.

        :return: A bytes object to be passed on to a transport.
        :rtype: bytes
        """
        return json_dumps([req._to_dict() for req in self]).encode()


class JSONRPCBatchResponse(RPCBatchResponse):
    """Multiple responses from a batch request. See
    :py:class:`JSONRPCBatchRequest` on how to handle.

    Items in a batch response need to be
    :py:class:`JSONRPCResponse` instances or None, meaning no reply should be
    generated for the request.
    """
    def serialize(self) -> bytes:
        """Returns a serialization of the batch response.

        Converts the response into a bytes object that can be passed to and by the transport layer.

        :return: A bytes object to be passed on to a transport.
        :rtype: bytes
        """
        return json_dumps([
            resp._to_dict() for resp in self if resp is not None
        ]).encode()


class JSONRPCProtocol(RPCBatchProtocol):
    """JSONRPC protocol implementation."""

    JSON_RPC_VERSION = "2.0"
    """Currently, only version 2.0 is supported."""

    _ALLOWED_REPLY_KEYS = sorted(['id', 'jsonrpc', 'error', 'result'])
    _ALLOWED_REQUEST_KEYS = sorted(['id', 'jsonrpc', 'method', 'params'])

    def __init__(
            self,
            id_generator: Optional[Generator[object, None, None]] = None,
            *args,
            **kwargs
    ) -> None:
        super(JSONRPCProtocol, self).__init__(*args, **kwargs)
        self._id_generator = id_generator or default_id_generator()
        self._pending_replies = []

    def _get_unique_id(self) -> object:
        return next(self._id_generator)

    def request_factory(self) -> 'JSONRPCRequest':
        """Factory for request objects.

        Allows derived classes to use requests derived from :py:class:`JSONRPCRequest`.

        :rtype: :py:class:`JSONRPCRequest`
        """
        return JSONRPCRequest()

    def create_batch_request(
            self,
            requests: Union['JSONRPCRequest', List['JSONRPCRequest']] = None
    ) -> 'JSONRPCBatchRequest':
        """Create a new :py:class:`JSONRPCBatchRequest` object.

        Called by the client when constructing a request.

        :param requests: A list of requests.
        :type requests: :py:class:`list` or :py:class:`JSONRPCRequest`
        :return: A new request instance.
        :rtype: :py:class:`JSONRPCBatchRequest`
        """
        return JSONRPCBatchRequest(requests or [])

    def create_request(
            self,
            method: str,
            args: List[Any] = None,
            kwargs: Dict[str, Any] = None,
            one_way: bool = False
    ) -> 'JSONRPCRequest':
        """Creates a new :py:class:`JSONRPCRequest` object.

        Called by the client when constructing a request.
        JSON RPC allows either the ``args`` or ``kwargs`` argument to be set.

        :param str method: The method name to invoke.
        :param list args: The positional arguments to call the method with.
        :param dict kwargs: The keyword arguments to call the method with.
        :param bool one_way: The request is an update, i.e. it does not expect a reply.
        :return: A new request instance
        :rtype: :py:class:`JSONRPCRequest`
        :raises InvalidRequestError: when ``args`` and ``kwargs`` are both defined.
        """
        if args and kwargs:
            raise InvalidRequestError(
                'Does not support args and kwargs at '
                'the same time'
            )

        request = self.request_factory()
        request.one_way = one_way

        if not one_way:
            request.unique_id = self._get_unique_id()
            self._pending_replies.append(request.unique_id)

        request.method = method
        if args is not None:
            request.args = args
        if kwargs is not None:
            request.kwargs = kwargs

        return request

    def parse_reply(
            self, data: bytes
    ) -> Union['JSONRPCSuccessResponse', 'JSONRPCErrorResponse', 'JSONRPCBatchResponse']:
        """De-serializes and validates a response.

        Called by the client to reconstruct the serialized :py:class:`JSONRPCResponse`.

        :param bytes data: The data stream received by the transport layer containing the
            serialized request.
        :return: A reconstructed response.
        :rtype: :py:class:`JSONRPCSuccessResponse` or :py:class:`JSONRPCErrorResponse`
        :raises InvalidReplyError: if the response is not valid JSON or does not conform
            to the standard.
        """
        if isinstance(data, bytes):
            data = data.decode()

        try:
            rep = json.loads(data)
        except Exception as e:
            raise InvalidReplyError(e)

        if isinstance(rep, list):
            # batch request
            replies = JSONRPCBatchResponse()
            for subrep in rep:
                try:
                    replies.append(self._parse_subreply(subrep))
                except RPCError as e:
                    replies.append(e)
                except Exception as e:
                    replies.append(InvalidReplyError(e))

            if not replies:
                raise InvalidReplyError("Empty batch response received.")
            return replies
        else:
            return self._parse_subreply(rep)

    def _parse_subreply(self, rep):
        for k in rep.keys():
            if k not in self._ALLOWED_REPLY_KEYS:
                raise InvalidReplyError('Key not allowed: %s' % k)

        if 'jsonrpc' not in rep:
            raise InvalidReplyError('Missing jsonrpc (version) in response.')

        if rep['jsonrpc'] != self.JSON_RPC_VERSION:
            raise InvalidReplyError('Wrong JSONRPC version')

        if 'id' not in rep:
            raise InvalidReplyError('Missing id in response')

        if ('error' in rep) and ('result' in rep):
            raise InvalidReplyError(
                'Reply must contain exactly one of result and error.'
            )

        if 'error' in rep:
            response = JSONRPCErrorResponse()
            error = rep['error']
            response.error = error["message"]
            response._jsonrpc_error_code = error["code"]
            if "data" in error:
                response.data = error["data"]
        else:
            response = JSONRPCSuccessResponse()
            response.result = rep.get('result', None)

        response.unique_id = rep['id']
        if response.unique_id not in self._pending_replies:
            raise UnexpectedIDError(
                'Reply id does not correspond to any sent requests.'
            )
        else:
            self._pending_replies.remove(response.unique_id)

        return response

    def parse_request(self, data: bytes
                      ) -> Union['JSONRPCRequest', 'JSONRPCBatchRequest']:
        """De-serializes and validates a request.

        Called by the server to reconstruct the serialized :py:class:`JSONRPCRequest`.

        :param bytes data: The data stream received by the transport layer containing the
            serialized request.
        :return: A reconstructed request.
        :rtype: :py:class:`JSONRPCRequest`
        :raises JSONRPCParseError: if the ``data`` cannot be parsed as valid JSON.
        :raises JSONRPCInvalidRequestError: if the request does not comply with the standard.
        """
        if isinstance(data, bytes):
            data = data.decode()

        try:
            req = json.loads(data)
        except Exception as e:
            raise JSONRPCParseError()

        if isinstance(req, list):
            # batch request
            requests = JSONRPCBatchRequest()
            for subreq in req:
                try:
                    requests.append(self._parse_subrequest(subreq))
                except RPCError as e:
                    requests.append(e)
                except Exception as e:
                    requests.append(JSONRPCInvalidRequestError(request_id=subreq.get("id")))

            if not requests:
                raise JSONRPCInvalidRequestError()
            return requests
        else:
            return self._parse_subrequest(req)

    def _parse_subrequest(self, req):
        if not isinstance(req, dict):
            raise JSONRPCInvalidRequestError()

        for k in req.keys():
            if k not in self._ALLOWED_REQUEST_KEYS:
                raise JSONRPCInvalidRequestError(request_id=req.get("id"))

        if req.get('jsonrpc', None) != self.JSON_RPC_VERSION:
            raise JSONRPCInvalidRequestError(request_id=req.get("id"))

        if not isinstance(req['method'], str):
            raise JSONRPCInvalidRequestError(request_id=req.get("id"))

        request = self.request_factory()

        request.method = req['method']
        request.one_way = 'id' not in req
        if not request.one_way:
            request.unique_id = req['id']

        params = req.get('params', None)
        if params is not None:
            if isinstance(params, list):
                request.args = req['params']
            elif isinstance(params, dict):
                request.kwargs = req['params']
            else:
                raise JSONRPCInvalidParamsError(request_id=req.get("id"))

        return request

    def raise_error(
            self, error: Union['JSONRPCErrorResponse', Dict[str, Any]]
    ) -> 'JSONRPCError':
        """Recreates the exception.

        Creates a :py:class:`~tinyrpc.protocols.jsonrpc.JSONRPCError` instance
        and raises it.
        This allows the error, message and data attributes of the original
        exception to propagate into the client code.

        The :py:attr:`~tinyrpc.protocols.RPCProtocol.raises_error` flag controls if the exception object is
        raised or returned.

        :returns: the exception object if it is not allowed to raise it.
        :raises JSONRPCError: when the exception can be raised.
            The exception object will contain ``message``, ``code`` and optionally a
            ``data`` property.
        """
        exc = JSONRPCError(error)
        if self.raises_errors:
            raise exc
        return exc

    def _caller(
            self, method: Callable, args: List[Any], kwargs: Dict[str, Any]
    ) -> Any:
        # Custom dispatcher called by RPCDispatcher._dispatch().
        # Override this when you need to call the method with additional parameters for example.
        return method(*args, **kwargs)
