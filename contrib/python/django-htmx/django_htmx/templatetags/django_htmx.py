from __future__ import annotations

from django.template import Context, Library

from django_htmx.jinja import django_htmx_script as base_django_htmx_script
from django_htmx.jinja import htmx_script as base_htmx_script

register = Library()


@register.simple_tag(takes_context=True)
def htmx_script(context: Context, minified: bool = True) -> str:
    return base_htmx_script(minified=minified, nonce=context.get("csp_nonce"))


@register.simple_tag(takes_context=True)
def django_htmx_script(context: Context) -> str:
    return base_django_htmx_script(nonce=context.get("csp_nonce"))
