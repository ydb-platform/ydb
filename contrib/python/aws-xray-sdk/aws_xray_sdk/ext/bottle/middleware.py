from bottle import request, response, SimpleTemplate

from aws_xray_sdk.core.lambda_launcher import check_in_lambda, LambdaContext
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.ext.util import calculate_sampling_decision, \
    calculate_segment_name, construct_xray_header, prepare_response_header


class XRayMiddleware:
    """
    Middleware that wraps each incoming request to a segment.
    """
    name = 'xray'
    api = 2

    def __init__(self, recorder):
        self._recorder = recorder
        self._in_lambda_ctx = False

        if check_in_lambda() and type(self._recorder.context) == LambdaContext:
            self._in_lambda_ctx = True

        _patch_render(recorder)

    def apply(self, callback, route):
        """
        Apply middleware directly to each route callback.
        """
        def wrapper(*a, **ka):
            headers = request.headers
            xray_header = construct_xray_header(headers)
            name = calculate_segment_name(request.urlparts[1], self._recorder)

            sampling_req = {
               'host': request.urlparts[1],
               'method': request.method,
               'path': request.path,
               'service': name,
            }
            sampling_decision = calculate_sampling_decision(
               trace_header=xray_header,
               recorder=self._recorder,
               sampling_req=sampling_req,
            )

            if self._in_lambda_ctx:
                segment = self._recorder.begin_subsegment(name)
            else:
                segment = self._recorder.begin_segment(
                    name=name,
                    traceid=xray_header.root,
                    parent_id=xray_header.parent,
                    sampling=sampling_decision,
                )

            segment.save_origin_trace_header(xray_header)
            segment.put_http_meta(http.URL, request.url)
            segment.put_http_meta(http.METHOD, request.method)
            segment.put_http_meta(http.USER_AGENT, headers.get('User-Agent'))

            client_ip = request.environ.get('HTTP_X_FORWARDED_FOR') or request.environ.get('REMOTE_ADDR')
            if client_ip:
                segment.put_http_meta(http.CLIENT_IP, client_ip)
                segment.put_http_meta(http.X_FORWARDED_FOR, True)
            else:
                segment.put_http_meta(http.CLIENT_IP, request.remote_addr)

            try:
                rv = callback(*a, **ka)
            except Exception as resp:
                segment.put_http_meta(http.STATUS, getattr(resp, 'status_code', 500))
                stack = stacktrace.get_stacktrace(limit=self._recorder._max_trace_back)
                segment.add_exception(resp, stack)
                if self._in_lambda_ctx:
                    self._recorder.end_subsegment()
                else:
                    self._recorder.end_segment()

                raise resp

            segment.put_http_meta(http.STATUS, response.status_code)

            origin_header = segment.get_origin_trace_header()
            resp_header_str = prepare_response_header(origin_header, segment)
            response.set_header(http.XRAY_HEADER, resp_header_str)

            cont_len = response.headers.get('Content-Length')
            if cont_len:
                segment.put_http_meta(http.CONTENT_LENGTH, int(cont_len))

            if self._in_lambda_ctx:
                self._recorder.end_subsegment()
            else:
                self._recorder.end_segment()

            return rv

        return wrapper

def _patch_render(recorder):

    _render = SimpleTemplate.render

    @recorder.capture('template_render')
    def _traced_render(self, *args, **kwargs):
        if self.filename:
            recorder.current_subsegment().name = self.filename
        return _render(self, *args, **kwargs)

    SimpleTemplate.render = _traced_render
