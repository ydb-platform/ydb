# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


import base64
import weakref
from datetime import date, datetime
from functools import wraps
from typing import Any, Callable, Optional

from opensearchpy.serializer import Serializer

from ..compat import quote, string_types, to_bytes, to_str, unquote, urlparse

# parts of URL to be omitted
SKIP_IN_PATH: Any = (None, "", b"", [], ())


def _normalize_hosts(hosts: Any) -> Any:
    """
    Helper function to transform hosts argument to
    :class:`~opensearchpy.OpenSearch` to a list of dicts.
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
                host = f"//{host}"  # type: ignore

            parsed_url = urlparse(host)
            h = {"host": parsed_url.hostname}

            if parsed_url.port:
                h["port"] = parsed_url.port

            if parsed_url.scheme == "https":
                h["port"] = parsed_url.port or 443
                h["use_ssl"] = True

            if parsed_url.username or parsed_url.password:
                h["http_auth"] = "{}:{}".format(
                    unquote(parsed_url.username),
                    unquote(parsed_url.password),
                )

            if parsed_url.path and parsed_url.path != "/":
                h["url_prefix"] = parsed_url.path

            out.append(h)
        else:
            out.append(host)
    return out


def _escape(value: Any) -> Any:
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
        if isinstance(value, str):
            return value.encode("utf-8")

    return str(value)


def _make_path(*parts: Any) -> str:
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


def query_params(*opensearch_query_params: Any) -> Callable:  # type: ignore
    """
    Decorator that pops all accepted parameters from method's kwargs and puts
    them in the params argument.
    """

    def _wrapper(func: Any) -> Any:
        @wraps(func)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            params = (kwargs.pop("params", None) or {}).copy()
            headers = {
                k.lower(): v
                for k, v in (kwargs.pop("headers", None) or {}).copy().items()
            }

            if "opaque_id" in kwargs:
                headers["x-opaque-id"] = kwargs.pop("opaque_id")

            http_auth = kwargs.pop("http_auth", None)
            api_key = kwargs.pop("api_key", None)

            if http_auth is not None and api_key is not None:
                raise ValueError(
                    "Only one of 'http_auth' and 'api_key' may be passed at a time"
                )
            elif http_auth is not None:
                headers["authorization"] = f"Basic {_base64_auth_header(http_auth)}"
            elif api_key is not None:
                headers["authorization"] = f"ApiKey {_base64_auth_header(api_key)}"

            # don't escape ignore, request_timeout, or timeout
            for p in ("ignore", "request_timeout", "timeout"):
                if p in kwargs:
                    params[p] = kwargs.pop(p)

            for p in opensearch_query_params + GLOBAL_PARAMS:
                if p in kwargs:
                    v = kwargs.pop(p)
                    if v is not None:
                        params[p] = _escape(v)

            return func(*args, params=params, headers=headers, **kwargs)

        return _wrapped

    return _wrapper


def _bulk_body(serializer: Optional[Serializer], body: Any) -> Any:
    # if not passed in a string, serialize items and join by newline
    if not isinstance(body, string_types):
        body = "\n".join(map(serializer.dumps, body))  # type: ignore

    # bulk body must end with a newline
    if isinstance(body, bytes):
        if not body.endswith(b"\n"):
            body += b"\n"
    elif isinstance(body, string_types) and not body.endswith("\n"):  # type: ignore
        body += "\n"  # type: ignore

    return body


def _base64_auth_header(auth_value: Any) -> str:
    """Takes either a 2-tuple or a base64-encoded string
    and returns a base64-encoded string to be used
    as an HTTP authorization header.
    """
    if isinstance(auth_value, (list, tuple)):
        auth_value = base64.b64encode(to_bytes(":".join(auth_value)))
    return to_str(auth_value)


class NamespacedClient:
    def __init__(self, client: Any) -> None:
        self.client = client

    @property
    def transport(self) -> Any:
        return self.client.transport


class AddonClient(NamespacedClient):
    @classmethod
    def infect_client(cls: Any, client: NamespacedClient) -> NamespacedClient:
        addon = cls(weakref.proxy(client))
        setattr(client, cls.namespace, addon)
        return client
