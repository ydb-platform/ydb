# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.conf import settings
import six
from rest_framework.parsers import BaseParser, ParseError
from rest_framework.renderers import JSONRenderer
import ujson


__author__ = 'y.gavenchuk aka murminathor'
__all__ = ['UJSONParser', ]


class UJSONParser(BaseParser):
    """
    Parses JSON-serialized data by ujson parser.
    """

    media_type = 'application/json'
    renderer_class = JSONRenderer

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Parses the incoming bytestream as JSON and returns the resulting data.
        """
        parser_context = parser_context or {}
        encoding = parser_context.get('encoding', settings.DEFAULT_CHARSET)

        try:
            data = stream.read().decode(encoding)
            return ujson.loads(data)
        except ValueError as exc:
            raise ParseError('JSON parse error - %s' % six.text_type(exc))
