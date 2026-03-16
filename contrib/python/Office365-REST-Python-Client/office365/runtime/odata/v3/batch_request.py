import json
import re
from email.message import Message
from typing import AnyStr, Iterator, List, Tuple

import requests
from requests import Response
from requests.structures import CaseInsensitiveDict

from office365.runtime.compat import (
    message_as_bytes_or_string,
    message_from_bytes_or_string,
)
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.odata.request import ODataRequest
from office365.runtime.queries.batch import BatchQuery, create_boundary
from office365.runtime.queries.client_query import ClientQuery


class ODataBatchV3Request(ODataRequest):
    def build_request(self, query):
        # type: (BatchQuery) -> RequestOptions
        """Construct a OData v3 Batch request"""
        request = RequestOptions(query.url)
        request.method = HttpMethod.Post
        media_type = "multipart/mixed"
        content_type = "; ".join(
            [media_type, "boundary={0}".format(query.current_boundary)]
        )
        request.ensure_header("Content-Type", content_type)
        request.data = self._prepare_payload(query)
        return request

    def process_response(self, response, query):
        # type: (Response, BatchQuery) -> None
        """Parses an HTTP response."""
        for sub_qry, sub_resp in self._extract_response(response, query):
            sub_resp.raise_for_status()
            super(ODataBatchV3Request, self).process_response(sub_resp, sub_qry)

    def _extract_response(self, response, query):
        # type: (Response, BatchQuery) -> Iterator[Tuple[ClientQuery, Response]]
        """Parses a multipart/mixed response body from the position defined by the context."""
        content_type = response.headers["Content-Type"].encode("ascii")
        http_body = b"Content-Type: " + content_type + b"\r\n\r\n" + response.content

        message = message_from_bytes_or_string(http_body)  # type: Message

        query_id = 0
        for raw_response in message.get_payload():
            if raw_response.get_content_type() == "application/http":
                qry = query.ordered_queries[query_id]
                query_id += 1
                yield qry, self._deserialize_response(raw_response)

    def _prepare_payload(self, query):
        # type: (BatchQuery) -> AnyStr
        """Serializes a batch request body."""
        main_message = Message()
        main_message.add_header("Content-Type", "multipart/mixed")
        main_message.set_boundary(query.current_boundary)

        if query.has_change_sets:
            change_set_message = Message()
            change_set_boundary = create_boundary("changeset_", True)
            change_set_message.add_header("Content-Type", "multipart/mixed")
            change_set_message.set_boundary(change_set_boundary)

            for qry in query.change_sets:
                request = qry.build_request()
                message = self._serialize_request(request)
                change_set_message.attach(message)
            main_message.attach(change_set_message)

        for qry in query.get_queries:
            request = qry.build_request()
            message = self._serialize_request(request)
            main_message.attach(message)

        return message_as_bytes_or_string(main_message)

    @staticmethod
    def _normalize_headers(headers_raw):
        # type: (List[str]) -> CaseInsensitiveDict
        """ """
        headers = {}
        for header_line in headers_raw:
            k, v = header_line.split(":", 1)
            headers[k.title()] = v.strip()
        return CaseInsensitiveDict(headers)

    def _deserialize_response(self, raw_response):
        # type: (Message) -> Response
        response = raw_response.get_payload(decode=True)
        lines = list(filter(None, response.decode("utf-8").split("\r\n")))
        response_status_regex = "^HTTP/1\\.\\d (\\d{3}) (.*)$"
        status_result = re.match(response_status_regex, lines[0])
        status_info = status_result.groups()

        resp = requests.Response()
        resp.status_code = int(status_info[0])
        if status_info[1] == "No Content" or len(lines) < 3:
            resp.headers = self._normalize_headers(lines[1:])
            resp._content = bytes(str("").encode("utf-8"))
        else:
            resp._content = bytes(str(lines[-1]).encode("utf-8"))
            resp.headers = self._normalize_headers(lines[1:-1])
        return resp

    @staticmethod
    def _serialize_request(request):
        # type: (RequestOptions) -> Message
        """Serializes a part of a batch request to a string. A part can be either a GET request or
        a change set grouping several CUD (create, update, delete) requests.
        """
        eol = "\r\n"
        method = request.method
        if "X-HTTP-Method" in request.headers:
            method = request.headers["X-HTTP-Method"]
        lines = ["{method} {url} HTTP/1.1".format(method=method, url=request.url)] + [
            ":".join(h) for h in request.headers.items()
        ]
        if request.data:
            lines.append(eol)
            lines.append(json.dumps(request.data))
        raw_content = eol + eol.join(lines) + eol
        payload = raw_content.encode("utf-8").lstrip()

        message = Message()
        message.add_header("Content-Type", "application/http")
        message.add_header("Content-Transfer-Encoding", "binary")
        message.set_payload(payload)
        return message
