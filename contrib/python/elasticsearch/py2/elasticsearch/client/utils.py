#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from __future__ import unicode_literals

import base64
import os
import warnings
import weakref
from datetime import date, datetime
from functools import wraps

from .._version import __versionstr__
from ..compat import PY2, quote, string_types, to_bytes, to_str, unquote, urlparse

# parts of URL to be omitted
SKIP_IN_PATH = (None, "", b"", [], ())

# Switch to this mimetype if 'ELASTIC_CLIENT_APIVERSIONING=1/true'
_COMPATIBILITY_MIMETYPE = "application/vnd.elasticsearch+json;compatible-with=%s" % (
    __versionstr__.partition(".")[0]
)


def _normalize_hosts(hosts):
    """
    Helper function to transform hosts argument to
    :class:`~elasticsearch.Elasticsearch` to a list of dicts.
    """
    # if hosts are empty, just defer to defaults down the line
    if hosts is None:
        return [{}]

    # passed in just one string
    if isinstance(hosts, string_types):
        hosts = [hosts]

    out = []
    # normalize hosts to dicts
    for host in hosts:
        if isinstance(host, string_types):
            if "://" not in host:
                host = "//%s" % host

            parsed_url = urlparse(host)
            h = {"host": parsed_url.hostname}

            if parsed_url.port:
                h["port"] = parsed_url.port

            if parsed_url.scheme == "https":
                h["port"] = parsed_url.port or 443
                h["use_ssl"] = True

            if parsed_url.username or parsed_url.password:
                h["http_auth"] = "%s:%s" % (
                    unquote(parsed_url.username),
                    unquote(parsed_url.password),
                )

            if parsed_url.path and parsed_url.path != "/":
                h["url_prefix"] = parsed_url.path

            out.append(h)
        else:
            out.append(host)
    return out


def _escape(value):
    """
    Escape a single value of a URL string or a query parameter. If it is a list
    or tuple, turn it into a comma-separated string first.
    """

    # make sequences into comma-separated stings
    if isinstance(value, (list, tuple)):
        value = ",".join(value)

    # dates and datetimes into isoformat
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()

    # make bools into true/false strings
    elif isinstance(value, bool):
        value = str(value).lower()

    # don't decode bytestrings
    elif isinstance(value, bytes):
        return value

    # encode strings to utf-8
    if isinstance(value, string_types):
        if PY2 and isinstance(value, unicode):  # noqa: F821
            return value.encode("utf-8")
        if not PY2 and isinstance(value, str):
            return value.encode("utf-8")

    return str(value)


def _make_path(*parts):
    """
    Create a URL string from parts, omit all `None` values and empty strings.
    Convert lists and tuples to comma separated values.
    """
    # TODO: maybe only allow some parts to be lists/tuples ?
    return "/" + "/".join(
        # preserve ',' and '*' in url for nicer URLs in logs
        quote(_escape(p), b",*")
        for p in parts
        if p not in SKIP_IN_PATH
    )


# parameters that apply to all methods
GLOBAL_PARAMS = ("pretty", "human", "error_trace", "format", "filter_path")


def query_params(*es_query_params, **kwargs):
    """
    Decorator that pops all accepted parameters from method's kwargs and puts
    them in the params argument.
    """
    request_mimetypes = kwargs.pop("request_mimetypes", [])
    response_mimetypes = kwargs.pop("response_mimetypes", [])

    default_content_type = "".join(request_mimetypes[:1])
    default_accept = ",".join(response_mimetypes)

    def compat_mimetype(mimetypes):
        return [
            _COMPATIBILITY_MIMETYPE
            if mimetype
            in (
                "application/json",
                "application/x-ndjson",
                "application/vnd.mapbox-vector-tile",
            )
            else mimetype
            for mimetype in mimetypes
        ]

    compat_content_type = "".join(compat_mimetype(request_mimetypes)[:1])
    compat_accept = ",".join(compat_mimetype(response_mimetypes))

    body_params = kwargs.pop("body_params", None)
    body_only_params = set(body_params or ()) - set(es_query_params)
    body_name = kwargs.pop("body_name", None)
    body_required = kwargs.pop("body_required", False)
    type_possible_in_params = "type" in es_query_params

    # There should be no APIs defined with both 'body_params' and a named body.
    assert not (body_name and body_params)

    # 'body_required' implies there's no named body and that body_params are defined.
    assert not (body_name and body_required)
    assert not body_required or body_params

    def _wrapper(func):
        @wraps(func)
        def _wrapped(*args, **kwargs):
            params = (kwargs.pop("params", None) or {}).copy()
            headers = {
                k.lower(): v
                for k, v in (kwargs.pop("headers", None) or {}).copy().items()
            }

            if "opaque_id" in kwargs:
                headers["x-opaque-id"] = kwargs.pop("opaque_id")

            # Detect compatibility mode and set the 'Accept' and 'Content-Type'
            # headers to the compatibility mimetype if detected.
            try:
                if os.environ["ELASTIC_CLIENT_APIVERSIONING"] not in ("true", "1"):
                    raise KeyError  # Unset is the same as env var not being 'true' or '1'
                accept = compat_accept
                content_type = compat_content_type
            except KeyError:
                accept = default_accept
                content_type = default_content_type

            # Set the mimetype headers for the request
            if accept:
                headers.setdefault("accept", accept)
            if content_type:
                headers.setdefault("content-type", content_type)

            http_auth = kwargs.pop("http_auth", None)
            api_key = kwargs.pop("api_key", None)

            # Detect when we should absorb body parameters into 'body'
            # We only do this when there's no 'body' parameter, no
            # positional arguments, and at least one parameter we can
            # serialize in the body.
            using_body_kwarg = kwargs.get("body", None) is not None
            using_positional_args = args and len(args) > 1

            # The 'doc_type' parameter is deprecated in the query
            # string. This was generated and missed in 7.x so to
            # push users to use 'type' instead of 'doc_type' in 8.x
            # we deprecate it here.
            if type_possible_in_params:
                doc_type_in_params = params and "doc_type" in params
                doc_type_in_kwargs = "doc_type" in kwargs

                if doc_type_in_params or doc_type_in_kwargs:
                    warnings.warn(
                        "The 'doc_type' parameter is deprecated, use 'type' for this "
                        "API instead. See https://github.com/elastic/elasticsearch-py/"
                        "issues/1698 for more information",
                        category=DeprecationWarning,
                        stacklevel=2,
                    )
                if doc_type_in_params:
                    params["type"] = params.pop("doc_type")
                if doc_type_in_kwargs:
                    kwargs["type"] = kwargs.pop("doc_type")

            if using_body_kwarg or using_positional_args:
                # If there are any body-only parameters then we raise a 'TypeError'
                # to alert the user they have to either not use a 'body' parameter
                # or to put the parameter into the body.
                body_only_params_in_use = body_only_params.intersection(kwargs)
                if body_only_params_in_use:
                    # Make sure the error message prose makes sense!
                    params_prose = "', '".join(sorted(body_only_params_in_use))
                    plural_params = len(body_only_params_in_use) > 1

                    raise TypeError(
                        "The '%s' parameter%s %s only serialized in the request body "
                        "and can't be combined with the 'body' parameter. Either stop using the "
                        "'body' parameter and use keyword-arguments only or move the specified "
                        "parameters into the 'body'. See https://github.com/elastic/elasticsearch-py/"
                        "issues/1698 for more information"
                        % (
                            params_prose,
                            "s" if plural_params else "",
                            "are" if plural_params else "is",
                        )
                    )

                # If there's no parameter overlap we still warn the user
                # that the 'body' parameter is deprecated for this API.
                if using_body_kwarg and body_params:
                    warnings.warn(
                        "The 'body' parameter is deprecated for the '%s' API and "
                        "will be removed in a future version. Instead use API parameters directly. "
                        "See https://github.com/elastic/elasticsearch-py/issues/1698 for "
                        "more information" % str(func.__name__),
                        DeprecationWarning,
                        stacklevel=2,
                    )

                # If positional arguments are being used we also warn about that being deprecated.
                if using_positional_args:
                    warnings.warn(
                        "Using positional arguments for APIs is deprecated and will be "
                        "disabled in 8.0.0. Instead use only keyword arguments for all APIs. "
                        "See https://github.com/elastic/elasticsearch-py/issues/1698 for "
                        "more information",
                        DeprecationWarning,
                        stacklevel=2,
                    )

            # We need to serialize all these parameters into a JSON body.
            elif set(body_params or ()).intersection(kwargs):
                body = {}
                for param in body_params:
                    value = kwargs.pop(param, None)
                    if value is not None:
                        body[param.rstrip("_")] = value
                kwargs["body"] = body

            # Since we've deprecated 'body' we set body={} if there
            # should be a body on JSON-field APIs but none of those fields
            # are filled.
            elif body_required:
                kwargs["body"] = {}

            # If there's a named body parameter then we transform it to 'body'
            # for backwards compatibility with libraries like APM.
            # Otherwise we warn the user about 'body' being deprecated.
            if body_name:
                if body_name in kwargs:
                    # If passed both 'body' and the named body param we raise an error.
                    if using_body_kwarg:
                        raise TypeError(
                            "Can't use '%s' and 'body' parameters together because '%s' "
                            "is an alias for 'body'. Instead you should only use the "
                            "'%s' parameter. See https://github.com/elastic/elasticsearch-py/"
                            "issues/1698 for more information"
                            % (
                                body_name,
                                body_name,
                                body_name,
                            )
                        )
                    kwargs["body"] = kwargs.pop(body_name)

                # Warn if user passes 'body' but should be using the named body parameter.
                elif using_body_kwarg:
                    warnings.warn(
                        "The 'body' parameter is deprecated for the '%s' API and "
                        "will be removed in a future version. Instead use the '%s' parameter. "
                        "See https://github.com/elastic/elasticsearch-py/issues/1698 "
                        "for more information" % (str(func.__name__), body_name),
                        category=DeprecationWarning,
                        stacklevel=2,
                    )

            if http_auth is not None and api_key is not None:
                raise ValueError(
                    "Only one of 'http_auth' and 'api_key' may be passed at a time"
                )
            elif http_auth is not None:
                headers["authorization"] = "Basic %s" % (
                    _base64_auth_header(http_auth),
                )
            elif api_key is not None:
                headers["authorization"] = "ApiKey %s" % (_base64_auth_header(api_key),)

            for p in es_query_params + GLOBAL_PARAMS:
                if p in kwargs:
                    v = kwargs.pop(p)
                    if v is not None:
                        params[p] = _escape(v)

            # don't treat ignore, request_timeout, and opaque_id as other params to avoid escaping
            for p in ("ignore", "request_timeout"):
                if p in kwargs:
                    params[p] = kwargs.pop(p)
            return func(*args, params=params, headers=headers, **kwargs)

        return _wrapped

    return _wrapper


def _bulk_body(serializer, body):
    # if not passed in a string, serialize items and join by newline
    if not isinstance(body, string_types):
        body = "\n".join(map(serializer.dumps, body))

    # bulk body must end with a newline
    if isinstance(body, bytes):
        if not body.endswith(b"\n"):
            body += b"\n"
    elif isinstance(body, string_types) and not body.endswith("\n"):
        body += "\n"

    return body


def _base64_auth_header(auth_value):
    """Takes either a 2-tuple or a base64-encoded string
    and returns a base64-encoded string to be used
    as an HTTP authorization header.
    """
    if isinstance(auth_value, (list, tuple)):
        auth_value = base64.b64encode(to_bytes(":".join(auth_value)))
    return to_str(auth_value)


class NamespacedClient(object):
    def __init__(self, client):
        self.client = client

    @property
    def transport(self):
        return self.client.transport


class AddonClient(NamespacedClient):
    @classmethod
    def infect_client(cls, client):
        addon = cls(weakref.proxy(client))
        setattr(client, cls.namespace, addon)
        return client
