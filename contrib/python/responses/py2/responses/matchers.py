import six
import json as json_module

from requests import PreparedRequest

if six.PY2:
    from urlparse import parse_qsl, urlparse
else:
    from urllib.parse import parse_qsl, urlparse


try:
    from requests.packages.urllib3.util.url import parse_url
except ImportError:  # pragma: no cover
    from urllib3.util.url import parse_url  # pragma: no cover

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


def _create_key_val_str(input_dict):
    """
    Returns string of format {'key': val, 'key2': val2}
    Function is called recursively for nested dictionaries

    :param input_dict: dictionary to transform
    :return: (str) reformatted string
    """

    def list_to_str(input_list):
        """
        Convert all list items to string.
        Function is called recursively for nested lists
        """
        converted_list = []
        for item in sorted(input_list, key=lambda x: str(x)):
            if isinstance(item, dict):
                item = _create_key_val_str(item)
            elif isinstance(item, list):
                item = list_to_str(item)

            converted_list.append(str(item))
        list_str = ", ".join(converted_list)
        return "[" + list_str + "]"

    items_list = []
    for key in sorted(input_dict.keys(), key=lambda x: str(x)):
        val = input_dict[key]
        if isinstance(val, dict):
            val = _create_key_val_str(val)
        elif isinstance(val, list):
            val = list_to_str(input_list=val)

        items_list.append("{}: {}".format(key, val))

    key_val_str = "{{{}}}".format(", ".join(items_list))
    return key_val_str


def urlencoded_params_matcher(params):
    """
    Matches URL encoded data

    :param params: (dict) data provided to 'data' arg of request
    :return: (func) matcher
    """

    def match(request):
        reason = ""
        request_body = request.body
        qsl_body = dict(parse_qsl(request_body)) if request_body else {}
        params_dict = params or {}
        valid = params is None if request_body is None else params_dict == qsl_body
        if not valid:
            reason = "request.body doesn't match: {} doesn't match {}".format(
                _create_key_val_str(qsl_body), _create_key_val_str(params_dict)
            )

        return valid, reason

    return match


def json_params_matcher(params):
    """
    Matches JSON encoded data

    :param params: (dict) JSON data provided to 'json' arg of request
    :return: (func) matcher
    """

    def match(request):
        reason = ""
        request_body = request.body
        params_dict = params or {}
        try:
            if isinstance(request_body, bytes):
                request_body = request_body.decode("utf-8")
            json_body = json_module.loads(request_body) if request_body else {}

            valid = params is None if request_body is None else params_dict == json_body

            if not valid:
                reason = "request.body doesn't match: {} doesn't match {}".format(
                    _create_key_val_str(json_body), _create_key_val_str(params_dict)
                )

        except JSONDecodeError:
            valid = False
            reason = (
                "request.body doesn't match: JSONDecodeError: Cannot parse request.body"
            )

        return valid, reason

    return match


def fragment_identifier_matcher(identifier):
    def match(request):
        reason = ""
        url_fragment = urlparse(request.url).fragment
        if identifier:
            url_fragment_qsl = sorted(parse_qsl(url_fragment))
            identifier_qsl = sorted(parse_qsl(identifier))
            valid = identifier_qsl == url_fragment_qsl
        else:
            valid = not url_fragment

        if not valid:
            reason = "URL fragment identifier is different: {} doesn't match {}".format(
                identifier, url_fragment
            )
        return valid, reason

    return match


def query_param_matcher(params):
    """
    Matcher to match 'params' argument in request

    :param params: (dict), same as provided to request
    :return: (func) matcher
    """

    def match(request):
        reason = ""
        request_params = request.params
        request_params_dict = request_params or {}
        params_dict = params or {}
        valid = (
            params is None
            if request_params is None
            else params_dict == request_params_dict
        )

        if not valid:
            reason = "Parameters do not match. {} doesn't match {}".format(
                _create_key_val_str(request_params_dict),
                _create_key_val_str(params_dict),
            )

        return valid, reason

    return match


def query_string_matcher(query):
    """
    Matcher to match query string part of request

    :param query: (str), same as constructed by request
    :return: (func) matcher
    """
    if six.PY2 and isinstance(query, unicode):  # noqa: F821
        query = query.encode("utf8")

    def match(request):
        reason = ""
        data = parse_url(request.url)
        request_query = data.query

        request_qsl = sorted(parse_qsl(request_query)) if request_query else {}
        matcher_qsl = sorted(parse_qsl(query)) if query else {}

        valid = not query if request_query is None else request_qsl == matcher_qsl

        if not valid:
            reason = "Query string doesn't match. {} doesn't match {}".format(
                _create_key_val_str(dict(request_qsl)),
                _create_key_val_str(dict(matcher_qsl)),
            )

        return valid, reason

    return match


def request_kwargs_matcher(kwargs):
    """
    Matcher to match keyword arguments provided to request

    :param kwargs: (dict), keyword arguments, same as provided to request
    :return: (func) matcher
    """

    def match(request):
        reason = ""
        kwargs_dict = kwargs or {}
        # validate only kwargs that were requested for comparison, skip defaults
        request_kwargs = {
            k: v for k, v in request.req_kwargs.items() if k in kwargs_dict
        }

        valid = (
            not kwargs_dict
            if not request_kwargs
            else sorted(kwargs.items()) == sorted(request_kwargs.items())
        )

        if not valid:
            reason = "Arguments don't match: {} doesn't match {}".format(
                _create_key_val_str(request_kwargs), _create_key_val_str(kwargs_dict)
            )

        return valid, reason

    return match


def multipart_matcher(files, data=None):
    """
    Matcher to match 'multipart/form-data' content-type.
    This function constructs request body and headers from provided 'data' and 'files'
    arguments and compares to actual request

    :param files: (dict), same as provided to request
    :param data: (dict), same as provided to request
    :return: (func) matcher
    """
    if not files:
        raise TypeError("files argument cannot be empty")

    prepared = PreparedRequest()
    prepared.headers = {"Content-Type": ""}
    prepared.prepare_body(data=data, files=files)

    def get_boundary(content_type):
        """
        Parse 'boundary' value from header.

        :param content_type: (str) headers["Content-Type"] value
        :return: (str) boundary value
        """
        if "boundary=" not in content_type:
            return ""

        return content_type.split("boundary=")[1]

    def match(request):
        reason = "multipart/form-data doesn't match. "
        if "Content-Type" not in request.headers:
            return False, reason + "Request is missing the 'Content-Type' header"

        request_boundary = get_boundary(request.headers["Content-Type"])
        prepared_boundary = get_boundary(prepared.headers["Content-Type"])

        # replace boundary value in header and in body, since by default
        # urllib3.filepost.encode_multipart_formdata dynamically calculates
        # random boundary alphanumeric value
        request_content_type = request.headers["Content-Type"]
        prepared_content_type = prepared.headers["Content-Type"].replace(
            prepared_boundary, request_boundary
        )

        request_body = request.body
        prepared_body = prepared.body

        if isinstance(prepared_body, bytes):
            # since headers always come as str, need to convert to bytes
            prepared_boundary = prepared_boundary.encode("utf-8")
            request_boundary = request_boundary.encode("utf-8")

        prepared_body = prepared_body.replace(prepared_boundary, request_boundary)

        headers_valid = prepared_content_type == request_content_type
        if not headers_valid:
            return (
                False,
                reason
                + "Request headers['Content-Type'] is different. {} isn't equal to {}".format(
                    request_content_type, prepared_content_type
                ),
            )

        body_valid = prepared_body == request_body
        if not body_valid:
            return False, reason + "Request body differs. {} aren't equal {}".format(
                request_body, prepared_body
            )

        return True, ""

    return match


def header_matcher(headers, strict_match=False):
    """
    Matcher to match 'headers' argument in request using the responses library.

    Because ``requests`` will send several standard headers in addition to what
    was specified by your code, request headers that are additional to the ones
    passed to the matcher are ignored by default. You can change this behaviour
    by passing ``strict_match=True``.

    :param headers: (dict), same as provided to request
    :param strict_match: (bool), whether headers in addition to those specified
                         in the matcher should cause the match to fail.
    :return: (func) matcher
    """

    def match(request):
        request_headers = request.headers or {}

        if not strict_match:
            # filter down to just the headers specified in the matcher
            request_headers = {k: v for k, v in request_headers.items() if k in headers}

        valid = sorted(headers.items()) == sorted(request_headers.items())

        if not valid:
            return False, "Headers do not match: {} doesn't match {}".format(
                _create_key_val_str(request_headers), _create_key_val_str(headers)
            )

        return valid, ""

    return match
