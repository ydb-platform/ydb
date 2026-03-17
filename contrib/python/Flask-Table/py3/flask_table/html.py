# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from functools import partial

from flask import Markup


def element(element, attrs=None, content='',
            escape_attrs=True, escape_content=True):
    return '<{element}{formatted_attrs}>{content}</{element}>'.format(
        element=element,
        formatted_attrs=_format_attrs(attrs or {}, escape_attrs),
        content=_format_content(content, escape_content),
    )


def _format_attrs(attrs, escape_attrs=True):
    out = []
    for name, value in sorted(attrs.items()):
        if escape_attrs:
            name = Markup.escape(name)
            value = Markup.escape(value)
        out.append(' {name}="{value}"'.format(name=name, value=value))
    return ''.join(out)


def _format_content(content, escape_content=True):
    if isinstance(content, (list, tuple)):
        content = ''.join(content)
    if escape_content:
        return Markup.escape(content)
    return content
