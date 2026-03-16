# coding: utf-8

from __future__ import unicode_literals

from ylog.context import log_context
from cached_property import cached_property

from .creator import Creator


class HttpRequestContext(log_context):
    def __init__(self, request, **kwargs):
        kwargs['request'] = request
        super(HttpRequestContext, self).__init__(**self.creator.do(**kwargs))

    @cached_property
    def creator(self):
        return Creator()
