import flask.templating
from flask import request

from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.ext.util import calculate_sampling_decision, \
    calculate_segment_name, construct_xray_header, prepare_response_header
from aws_xray_sdk.core.lambda_launcher import check_in_lambda, LambdaContext


class XRayMiddleware:

    def __init__(self, app, recorder):
        self.app = app
        self.app.logger.info("initializing xray middleware")

        self._recorder = recorder
        self.app.before_request(self._before_request)
        self.app.after_request(self._after_request)
        self.app.teardown_request(self._teardown_request)
        self.in_lambda_ctx = False

        if check_in_lambda() and type(self._recorder.context) == LambdaContext:
            self.in_lambda_ctx = True

        _patch_render(recorder)

    def _before_request(self):
        headers = request.headers
        xray_header = construct_xray_header(headers)
        req = request._get_current_object()

        name = calculate_segment_name(req.host, self._recorder)

        sampling_req = {
            'host': req.host,
            'method': req.method,
            'path': req.path,
            'service': name,
        }
        sampling_decision = calculate_sampling_decision(
            trace_header=xray_header,
            recorder=self._recorder,
            sampling_req=sampling_req,
        )

        if self.in_lambda_ctx:
            segment = self._recorder.begin_subsegment(name)
        else:
            segment = self._recorder.begin_segment(
                name=name,
                traceid=xray_header.root,
                parent_id=xray_header.parent,
                sampling=sampling_decision,
            )

        segment.save_origin_trace_header(xray_header)
        segment.put_http_meta(http.URL, req.base_url)
        segment.put_http_meta(http.METHOD, req.method)
        segment.put_http_meta(http.USER_AGENT, headers.get('User-Agent'))

        client_ip = headers.get('X-Forwarded-For') or headers.get('HTTP_X_FORWARDED_FOR')
        if client_ip:
            segment.put_http_meta(http.CLIENT_IP, client_ip)
            segment.put_http_meta(http.X_FORWARDED_FOR, True)
        else:
            segment.put_http_meta(http.CLIENT_IP, req.remote_addr)

    def _after_request(self, response):
        if self.in_lambda_ctx:
            segment = self._recorder.current_subsegment()
        else:
            segment = self._recorder.current_segment()
        segment.put_http_meta(http.STATUS, response.status_code)

        origin_header = segment.get_origin_trace_header()
        resp_header_str = prepare_response_header(origin_header, segment)
        response.headers[http.XRAY_HEADER] = resp_header_str

        cont_len = response.headers.get('Content-Length')
        if cont_len:
            segment.put_http_meta(http.CONTENT_LENGTH, int(cont_len))

        return response

    def _teardown_request(self, exception):
        segment = None
        try:
            if self.in_lambda_ctx:
                segment = self._recorder.current_subsegment()
            else:
                segment = self._recorder.current_segment()
        except Exception:
            pass
        if not segment:
            return

        if exception:
            segment.put_http_meta(http.STATUS, 500)
            stack = stacktrace.get_stacktrace(limit=self._recorder._max_trace_back)
            segment.add_exception(exception, stack)

        if self.in_lambda_ctx:
            self._recorder.end_subsegment()
        else:
            self._recorder.end_segment()


def _patch_render(recorder):

    _render = flask.templating._render

    @recorder.capture('template_render')
    def _traced_render(template, context, app):
        if template.name:
            recorder.current_subsegment().name = template.name
        return _render(template, context, app)

    flask.templating._render = _traced_render
