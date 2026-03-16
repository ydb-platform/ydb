# coding: utf-8

from __future__ import unicode_literals

from .base import BaseProvider


class Provider(BaseProvider):
    required_kwargs = ['request']

    def user(self, request):
        if getattr(request, 'yauser', None) is None:
            return None
        if not request.yauser.is_authenticated():
            return None
        yauser = request.yauser
        return {
            'uid': yauser.uid,
            'email': yauser.default_email,
        }

    def auth(self, request):
        if getattr(request, 'yauser', None) is None:
            return {'mechanism': None}
        if not request.yauser.is_authenticated():
            return {'mechanism': None}
        if not request.yauser.authenticated_by:
            return {'mechanism': 'unknown - request.yauser.authenticated_by is None'}
        ctx = {
            'mechanism': request.yauser.authenticated_by.mechanism_name
        }
        if request.yauser.authenticated_by.mechanism_name == 'oauth':
            ctx['application'] = request.yauser.blackbox_result.oauth.client_name
        if request.yauser.authenticated_by.mechanism_name == 'tvm':
            ctx['application'] = request.yauser.service_ticket.src
        return ctx
