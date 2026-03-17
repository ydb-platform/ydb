import os
import typing
from typing import Callable

import requests
from typing_extensions import Self

from office365.runtime.client_request import ClientRequest
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.upload_session import UploadSessionQuery


class UploadSessionRequest(ClientRequest):
    def __init__(self, file_object, chunk_size, chunk_uploaded=None):
        # type: (typing.IO, int, Callable[[int], None]) -> None
        super(UploadSessionRequest, self).__init__()
        self._file_object = file_object
        self._chunk_size = chunk_size
        self._chunk_uploaded = chunk_uploaded
        self._range_data = None

    def build_request(self, query):
        # type: (UploadSessionQuery) -> Self
        request = RequestOptions(query.upload_session_url)
        request.method = HttpMethod.Put
        request.set_header("Content-Length", str(len(self._range_data)))
        request.set_header(
            "Content-Range",
            "bytes {0}-{1}/{2}".format(
                self.range_start, self.range_end - 1, self.file_size
            ),
        )
        request.set_header("Accept", "*/*")
        request.data = self._range_data
        return request

    def process_response(self, response, query):
        # type: (requests.Response, UploadSessionQuery) -> None
        response.raise_for_status()
        if callable(self._chunk_uploaded):
            self._chunk_uploaded(self.range_end)

    def execute_query(self, query):
        # type: (UploadSessionQuery) -> None
        for self._range_data in self._read_next():
            super(UploadSessionRequest, self).execute_query(query)

    def _read_next(self):
        while True:
            content = self._file_object.read(self._chunk_size)
            if not content:
                break
            yield content

    @property
    def file_size(self):
        return os.fstat(self._file_object.fileno()).st_size

    @property
    def range_start(self):
        if self.range_end == 0:
            return 0
        return self.range_end - len(self._range_data)

    @property
    def range_end(self):
        return self._file_object.tell()
