# Copyright The OpenTracing Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tornado
from wrapt import wrap_function_wrapper as wrap_function, ObjectProxy

from . import application, handlers, httpclient


def init_tracing():
    _patch_tornado()
    _patch_tornado_client()


def init_client_tracing(tracer=None, start_span_cb=None):
    if hasattr(tracer, '_tracer'):
        tracer = tracer._tracer

    if start_span_cb is not None and not callable(start_span_cb):
        raise ValueError('start_span_cb')

    _patch_tornado_client(tracer, start_span_cb)


def _patch_tornado():
    # patch only once
    if getattr(tornado, '__opentracing_patch', False) is True:
        return

    setattr(tornado, '__opentracing_patch', True)

    wrap_function('tornado.web', 'Application.__init__',
                  application.tracer_config)

    wrap_function('tornado.web', 'RequestHandler._execute',
                  handlers.execute)
    wrap_function('tornado.web', 'RequestHandler.on_finish',
                  handlers.on_finish)
    wrap_function('tornado.web', 'RequestHandler.log_exception',
                  handlers.log_exception)


def _patch_tornado_client(tracer=None, start_span_cb=None):
    if getattr(tornado, '__opentracing_client_patch', False) is True:
        return

    setattr(tornado, '__opentracing_client_patch', True)
    httpclient._set_tracing_enabled(True)
    httpclient._set_tracing_info(tracer, start_span_cb)

    wrap_function('tornado.httpclient', 'AsyncHTTPClient.fetch',
                  httpclient.fetch_async)


def _unpatch(obj, attr):
    f = getattr(obj, attr, None)
    if f and isinstance(f, ObjectProxy) and hasattr(f, '__wrapped__'):
        setattr(obj, attr, f.__wrapped__)


def _unpatch_tornado():
    if getattr(tornado, '__opentracing_patch', False) is False:
        return

    setattr(tornado, '__opentracing_patch', False)

    _unpatch(tornado.web.Application, '__init__')

    _unpatch(tornado.web.RequestHandler, '_execute')
    _unpatch(tornado.web.RequestHandler, 'on_finish')
    _unpatch(tornado.web.RequestHandler, 'log_exception')


def _unpatch_tornado_client():
    if getattr(tornado, '__opentracing_client_patch', False) is False:
        return

    setattr(tornado, '__opentracing_client_patch', False)

    _unpatch(tornado.httpclient.AsyncHTTPClient, 'fetch')
