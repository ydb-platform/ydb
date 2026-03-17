#!/usr/bin/env python
# -*- encoding: utf-8 -*-


class CaldavError(Exception):
    """Group all Caldav Exceptions inside this Exception."""
    pass


class AuthorizationError(CaldavError):
    """
    The client encountered an HTTP 403 error and is passing it on
    to the user. The url property will contain the url in question,
    the reason property will contain the excuse the server sent.
    """
    url = None
    reason = "PHP at work[tm]"

    def __str__(self):
        return "AuthorizationError at '%s', reason '%s'" % \
            (self.url, self.reason)


class PropsetError(CaldavError):
    pass


class PropfindError(CaldavError):
    pass


class ReportError(CaldavError):
    pass


class MkcolError(CaldavError):
    pass


class MkcalendarError(CaldavError):
    pass


class PutError(CaldavError):
    pass


class DeleteError(CaldavError):
    pass


class NotFoundError(CaldavError):
    pass


exception_by_method = {}
for method in ('delete', 'put', 'mkcalendar', 'mkcol', 'report', 'propset',
               'propfind'):
    exception_by_method[method] = \
        locals()[method[0].upper() + method[1:] + 'Error']
