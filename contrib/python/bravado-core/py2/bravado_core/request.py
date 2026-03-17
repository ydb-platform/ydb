# -*- coding: utf-8 -*-
from six import itervalues

from bravado_core.operation import log
from bravado_core.param import unmarshal_param
from bravado_core.validate import validate_security_object


class IncomingRequest(object):
    """
    Common interface for server side request objects.

    Subclasses are responsible for providing attrs for __required_attrs__.
    """
    __required_attrs__ = [
        'path',     # dict of URL path parameters
        'query',    # dict of parameters from the query string
        'form',     # dict of form parameters from a POST
        'headers',  # dict of request headers
        # TODO: may need to make this more flexible based on actual usage and/or need for a file like object # noqa
        'files',    # dict of filename to content
    ]

    def __getattr__(self, name):
        """
        When an attempt to access a required attribute that doesn't exist
        is made, let the caller know that the type is non-compliant in its
        attempt to be `RequestList`. This is in place of the usual throwing
        of an AttributeError.

        Reminder: __getattr___ is only called when it has already been
                  determined that this object does not have the given attr.

        :raises: NotImplementedError when the subclass has not provided access
                to a required attribute.
        """
        if name in self.__required_attrs__:
            raise NotImplementedError(
                'This IncomingRequest type {0} forgot to implement an attr '
                'for `{1}`'.format(type(self), name),
            )
        raise AttributeError(
            "'{0}' object has no attribute '{1}'".format(type(self), name),
        )

    def json(self, **kwargs):
        """
        :return: request content in a json-like form
        :rtype: int, float, double, string, unicode, list, dict
        """
        raise NotImplementedError("Implement json() in {0}".format(type(self)))


# Deprecated
RequestLike = IncomingRequest


def unmarshal_request(request, op):
    """Unmarshal Swagger request parameters from the passed in request like
    object.

    :type request: :class: `bravado_core.request.IncomingRequest`.
    :type op: :class:`bravado_core.operation.Operation`
    :returns: dict where (key, value) = (param_name, param_value)
    """
    request_data = {}
    for param in itervalues(op.params):
        param_value = unmarshal_param(param, request)
        request_data[param.name] = param_value

    if op.swagger_spec.config['validate_requests']:
        validate_security_object(op, request_data)

    log.debug('Swagger request_data: %s', request_data)
    return request_data
