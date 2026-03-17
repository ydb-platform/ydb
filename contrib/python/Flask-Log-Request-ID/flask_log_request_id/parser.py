from flask import request


def amazon_elb_trace_id():
    """
    Get the amazon ELB trace id from current Flask request context
    :return: The found Trace-ID or None if not found
    :rtype: str | None
    """
    amazon_request_id = request.headers.get('X-Amzn-Trace-Id', '')
    trace_id_params = dict(x.split('=') if '=' in x else (x, None) for x in amazon_request_id.split(';'))
    if 'Self' in trace_id_params:
        return trace_id_params['Self']
    if 'Root' in trace_id_params:
        return trace_id_params['Root']

    return None


def generic_http_header_parser_for(header_name):
    """
    A parser factory to extract the request id from an HTTP header
    :return: A parser that can be used to extract the request id from the current request context
    :rtype: ()->str|None
    """

    def parser():
        request_id = request.headers.get(header_name, '').strip()

        if not request_id:
            # If the request id is empty return None
            return None
        return request_id
    return parser


def x_request_id():
    """
    Parser for generic X-Request-ID header
    :rtype: str|None
    """
    return generic_http_header_parser_for('X-Request-ID')()


def x_correlation_id():
    """
    Parser for generic X-Correlation-ID header
    :rtype: str|None
    """
    return generic_http_header_parser_for('X-Correlation-ID')()


def auto_parser(parsers=(x_request_id, x_correlation_id, amazon_elb_trace_id)):
    """
    Meta parser that will try all known parser and it will bring the first found id
    :param list[Callable] parsers: A list of callable parsers to try to extract request_id
    :return: The request id if it is found or None
    :rtype: str|None
    """

    for parser in parsers:
        request_id = parser()
        if request_id is not None:
            return request_id

    return None  # request-id was not found
