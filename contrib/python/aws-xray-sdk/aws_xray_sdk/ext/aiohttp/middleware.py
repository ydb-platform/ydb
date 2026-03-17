"""
AioHttp Middleware
"""
from aiohttp import web
from aiohttp.web_exceptions import HTTPException

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.ext.util import calculate_sampling_decision, \
    calculate_segment_name, construct_xray_header, prepare_response_header


@web.middleware
async def middleware(request, handler):
    """
    Main middleware function, deals with all the X-Ray segment logic
    """
    # Create X-Ray headers
    xray_header = construct_xray_header(request.headers)
    # Get name of service or generate a dynamic one from host
    name = calculate_segment_name(request.headers['host'].split(':', 1)[0], xray_recorder)

    sampling_req = {
        'host': request.headers['host'],
        'method': request.method,
        'path': request.path,
        'service': name,
    }

    sampling_decision = calculate_sampling_decision(
        trace_header=xray_header,
        recorder=xray_recorder,
        sampling_req=sampling_req,
    )

    # Start a segment
    segment = xray_recorder.begin_segment(
        name=name,
        traceid=xray_header.root,
        parent_id=xray_header.parent,
        sampling=sampling_decision,
    )

    segment.save_origin_trace_header(xray_header)
    # Store request metadata in the current segment
    segment.put_http_meta(http.URL, str(request.url))
    segment.put_http_meta(http.METHOD, request.method)

    if 'User-Agent' in request.headers:
        segment.put_http_meta(http.USER_AGENT, request.headers['User-Agent'])

    if 'X-Forwarded-For' in request.headers:
        segment.put_http_meta(http.CLIENT_IP, request.headers['X-Forwarded-For'])
        segment.put_http_meta(http.X_FORWARDED_FOR, True)
    elif 'remote_addr' in request.headers:
        segment.put_http_meta(http.CLIENT_IP, request.headers['remote_addr'])
    else:
        segment.put_http_meta(http.CLIENT_IP, request.remote)

    try:
        # Call next middleware or request handler
        response = await handler(request)
    except HTTPException as exc:
        # Non 2XX responses are raised as HTTPExceptions
        response = exc
        raise
    except BaseException as err:
        # Store exception information including the stacktrace to the segment
        response = None
        segment.put_http_meta(http.STATUS, 500)
        stack = stacktrace.get_stacktrace(limit=xray_recorder.max_trace_back)
        segment.add_exception(err, stack)
        raise
    finally:
        if response is not None:
            segment.put_http_meta(http.STATUS, response.status)
            if 'Content-Length' in response.headers:
                length = int(response.headers['Content-Length'])
                segment.put_http_meta(http.CONTENT_LENGTH, length)

            header_str = prepare_response_header(xray_header, segment)
            response.headers[http.XRAY_HEADER] = header_str

        xray_recorder.end_segment()

    return response
