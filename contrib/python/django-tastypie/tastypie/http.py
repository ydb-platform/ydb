"""
The various HTTP responses for use in returning proper HTTP codes.
"""
from django.http import HttpResponse


class HttpCreated(HttpResponse):
    status_code = 201

    def __init__(self, *args, **kwargs):
        location = kwargs.pop('location', '')

        super(HttpCreated, self).__init__(*args, **kwargs)
        self['Location'] = location


class HttpAccepted(HttpResponse):
    status_code = 202


class HttpNoContent(HttpResponse):
    status_code = 204

    def __init__(self, *args, **kwargs):
        super(HttpNoContent, self).__init__(*args, **kwargs)
        del self['Content-Type']


class HttpMultipleChoices(HttpResponse):
    status_code = 300


class HttpSeeOther(HttpResponse):
    status_code = 303


class HttpNotModified(HttpResponse):
    status_code = 304


class HttpBadRequest(HttpResponse):
    status_code = 400


class HttpUnauthorized(HttpResponse):
    status_code = 401


class HttpForbidden(HttpResponse):
    status_code = 403


class HttpNotFound(HttpResponse):
    status_code = 404


class HttpMethodNotAllowed(HttpResponse):
    status_code = 405


class HttpNotAcceptable(HttpResponse):
    status_code = 406


class HttpConflict(HttpResponse):
    status_code = 409


class HttpGone(HttpResponse):
    status_code = 410


class HttpUnsupportedMediaType(HttpResponse):
    status_code = 415


class HttpUnprocessableEntity(HttpResponse):
    status_code = 422


class HttpTooManyRequests(HttpResponse):
    status_code = 429


class HttpApplicationError(HttpResponse):
    status_code = 500


class HttpNotImplemented(HttpResponse):
    status_code = 501
