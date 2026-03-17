#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import json
import sys

import six

from .. import (
    RPCBatchProtocol, RPCRequest, RPCResponse, RPCErrorResponse,
    InvalidRequestError, MethodNotFoundError, InvalidReplyError, RPCError,
    RPCBatchRequest, RPCBatchResponse
)

if 'jsonext' in sys.modules:
    # jsonext was imported before this file, assume the intent is that
    # it is used in place of the regular json encoder.
    import jsonext

    json_dumps = jsonext.dumps
else:
    json_dumps = json.dumps


class FixedErrorMessageMixin(object):
    def __init__(self, *args, **kwargs):
        if not args:
            args = [self.message]
        if 'data' in kwargs:
            self.data = kwargs.pop('data')
        super(FixedErrorMessageMixin, self).__init__(*args, **kwargs)

    def error_respond(self):
        response = JSONRPCErrorResponse()

        response.error = self.message
        response.unique_id = None
        response._jsonrpc_error_code = self.jsonrpc_error_code
        if hasattr(self, 'data'):
            response.data = self.data
        return response


class JSONRPCParseError(FixedErrorMessageMixin, InvalidRequestError):
    jsonrpc_error_code = -32700
    message = 'Parse error'


class JSONRPCInvalidRequestError(FixedErrorMessageMixin, InvalidRequestError):
    jsonrpc_error_code = -32600
    message = 'Invalid Request'


class JSONRPCMethodNotFoundError(FixedErrorMessageMixin, MethodNotFoundError):
    jsonrpc_error_code = -32601
    message = 'Method not found'


class JSONRPCInvalidParamsError(FixedErrorMessageMixin, InvalidRequestError):
    jsonrpc_error_code = -32602
    message = 'Invalid params'


class JSONRPCInternalError(FixedErrorMessageMixin, InvalidRequestError):
    jsonrpc_error_code = -32603
    message = 'Internal error'


class JSONRPCServerError(FixedErrorMessageMixin, InvalidRequestError):
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
    def __init__(self, error):
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
    def _to_dict(self):
        return {
            'jsonrpc': JSONRPCProtocol.JSON_RPC_VERSION,
            'id': self.unique_id,
            'result': self.result
        }

    def serialize(self):
        return json_dumps(self._to_dict())


class JSONRPCErrorResponse(RPCErrorResponse):
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

    def serialize(self):
        return json_dumps(self._to_dict())


def _get_code_message_and_data(error):
    assert isinstance(error, (Exception, six.string_types))
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
    def error_respond(self, error):
        if self.unique_id is None:
            return None

        response = JSONRPCErrorResponse()

        code, msg, data = _get_code_message_and_data(error)

        response.error = msg
        response.unique_id = self.unique_id
        response._jsonrpc_error_code = code
        if data:
            response.data = data
        return response

    def respond(self, result):
        if self.unique_id is None:
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
        if hasattr(self, 'unique_id') and self.unique_id is not None:
            jdata['id'] = self.unique_id
        return jdata

    def serialize(self):
        return json_dumps(self._to_dict())


class JSONRPCBatchRequest(RPCBatchRequest):
    def create_batch_response(self):
        if self._expects_response():
            return JSONRPCBatchResponse()

    def _expects_response(self):
        for request in self:
            if isinstance(request, Exception):
                return True
            if request.unique_id != None:
                return True

        return False

    def serialize(self):
        return json_dumps([req._to_dict() for req in self])


class JSONRPCBatchResponse(RPCBatchResponse):
    def serialize(self):
        return json_dumps([resp._to_dict() for resp in self if resp != None])


class JSONRPCProtocol(RPCBatchProtocol):
    """JSONRPC protocol implementation.

    Currently, only version 2.0 is supported."""

    JSON_RPC_VERSION = "2.0"
    _ALLOWED_REPLY_KEYS = sorted(['id', 'jsonrpc', 'error', 'result'])
    _ALLOWED_REQUEST_KEYS = sorted(['id', 'jsonrpc', 'method', 'params'])

    def __init__(self, *args, **kwargs):
        super(JSONRPCProtocol, self).__init__(*args, **kwargs)
        self._id_counter = 0

    def _get_unique_id(self):
        self._id_counter += 1
        return self._id_counter

    def request_factory(self):
        return JSONRPCRequest()

    def create_batch_request(self, requests=None):
        return JSONRPCBatchRequest(requests or [])

    def create_request(self, method, args=None, kwargs=None, one_way=False):
        if args and kwargs:
            raise InvalidRequestError('Does not support args and kwargs at '
                                      'the same time')

        request = self.request_factory()

        if not one_way:
            request.unique_id = self._get_unique_id()

        request.method = method
        request.args = args
        request.kwargs = kwargs

        return request

    def parse_reply(self, data):
        if six.PY3 and isinstance(data, bytes):
            # zmq won't accept unicode strings, and this is the other
            # end; decoding non-unicode strings back into unicode
            data = data.decode()

        try:
            rep = json.loads(data)
        except Exception as e:
            raise InvalidReplyError(e)

        for k in six.iterkeys(rep):
            if not k in self._ALLOWED_REPLY_KEYS:
                raise InvalidReplyError('Key not allowed: %s' % k)

        if 'jsonrpc' not in rep:
            raise InvalidReplyError('Missing jsonrpc (version) in response.')

        if rep['jsonrpc'] != self.JSON_RPC_VERSION:
            raise InvalidReplyError('Wrong JSONRPC version')

        if 'id' not in rep:
            raise InvalidReplyError('Missing id in response')

        if ('error' in rep) == ('result' in rep):
            raise InvalidReplyError(
                'Reply must contain exactly one of result and error.'
            )

        if 'error' in rep:
            response = JSONRPCErrorResponse()
            error = rep['error']
            response.error = error['message']
            response._jsonrpc_error_code = error['code']
            if 'data' in error:
                response.data = error['data']
        else:
            response = JSONRPCSuccessResponse()
            response.result = rep.get('result', None)

        response.unique_id = rep['id']

        return response

    def parse_request(self, data):
        if six.PY3 and isinstance(data, bytes):
            # zmq won't accept unicode strings, and this is the other
            # end; decoding non-unicode strings back into unicode
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
                    requests.append(JSONRPCInvalidRequestError())

            if not requests:
                raise JSONRPCInvalidRequestError()
            return requests
        else:
            return self._parse_subrequest(req)

    def raise_error(self, error):
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

    def _parse_subrequest(self, req):
        if not isinstance(req, dict):
            raise JSONRPCInvalidRequestError()

        for k in six.iterkeys(req):
            if not k in self._ALLOWED_REQUEST_KEYS:
                raise JSONRPCInvalidRequestError()

        if req.get('jsonrpc', None) != self.JSON_RPC_VERSION:
            raise JSONRPCInvalidRequestError()

        if not isinstance(req['method'], six.string_types):
            raise JSONRPCInvalidRequestError()

        request = self.request_factory()

        request.method = str(req['method'])
        request.unique_id = req.get('id', None)

        params = req.get('params', None)
        if params is not None:
            if isinstance(params, list):
                request.args = req['params']
            elif isinstance(params, dict):
                request.kwargs = req['params']
            else:
                raise JSONRPCInvalidParamsError()

        return request

    def _caller(self, method, args, kwargs):
        # custom dispatcher called by RPCDispatcher._dispatch()
        # when provided with the address of a custom dispatcher.
        # Used to generate a customized error message when the
        # function signature doesn't match the parameter list.
        try:
            inspect.getcallargs(method, *args, **kwargs)
        except TypeError:
            raise JSONRPCInvalidParamsError()
        else:
            return method(*args, **kwargs)
