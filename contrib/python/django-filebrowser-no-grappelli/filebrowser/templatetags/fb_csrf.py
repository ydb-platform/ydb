# coding: utf-8

from django.template import Library, Node
from django.utils.safestring import mark_safe


register = Library()


class CsrfTokenNode(Node):
    def render(self, context):
        csrf_token = context.get('csrf_token', None)
        if csrf_token:
            if csrf_token == 'NOTPROVIDED':
                return mark_safe(u"")
            else:
                return mark_safe(u"<div style='display:none'><input type='hidden' name='csrfmiddlewaretoken' value='%s' /></div>" % (csrf_token))
        else:
            # It's very probable that the token is missing because of
            # misconfiguration, so we raise a warning
            from django.conf import settings
            if settings.DEBUG:
                import warnings
                warnings.warn("A {% csrf_token %} was used in a template, but the context did not provide the value. This is usually caused by not using RequestContext.")
            return u''


def fb_csrf_token(parser, token):
    return CsrfTokenNode()
register.tag(fb_csrf_token)
