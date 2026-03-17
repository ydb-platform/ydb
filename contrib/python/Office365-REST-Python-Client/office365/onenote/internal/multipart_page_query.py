from email.message import Message
from typing import AnyStr

from office365.runtime.compat import get_mime_type, message_as_bytes_or_string
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.batch import create_boundary
from office365.runtime.queries.client_query import ClientQuery


def _message_to_payload(message):
    # type: (Message) -> AnyStr
    lf = b"\n"
    crlf = b"\r\n"
    payload = message_as_bytes_or_string(message)
    lines = payload.split(lf)
    payload = bytes.join(crlf, lines[2:]) + crlf
    return payload


class OneNotePageCreateQuery(ClientQuery):
    def __init__(self, pages, presentation_file, attachment_files=None):
        """
        :type pages: office365.onenote.pages.collection.OnenotePageCollection
        :type presentation_file: typing.IO
        :type attachment_files: dict or None
        """
        super(OneNotePageCreateQuery, self).__init__(pages.context, pages)
        pages.context.before_execute(self._construct_multipart_request)
        self._presentation = presentation_file
        if attachment_files is None:
            attachment_files = {}
        self._files = attachment_files

    def _construct_multipart_request(self, request):
        # type: (RequestOptions) -> None
        request.method = HttpMethod.Post
        boundary = create_boundary("PageBoundary", True)
        request.set_header(
            "Content-Type", "multipart/form-data; boundary={0}".format(boundary)
        )

        main_message = Message()
        main_message.add_header(
            "Content-Type", "multipart/form-data; boundary={0}".format(boundary)
        )
        main_message.set_boundary(boundary)

        c_type, _enc = get_mime_type(self._presentation.name)
        presentation_message = Message()
        presentation_message.add_header("Content-Type", c_type)
        presentation_message.add_header(
            "Content-Disposition", 'form-data; name="Presentation"'
        )
        presentation_message.set_payload(self._presentation.read())
        main_message.attach(presentation_message)

        for name, file in self._files.items():
            file_message = Message()
            c_type, _enc = get_mime_type(file.name)
            file_message.add_header("Content-Type", c_type)
            file_message.add_header(
                "Content-Disposition", 'form-data; name="{0}"'.format(name)
            )
            file_content = file.read()
            file_message.set_payload(file_content)
            main_message.attach(file_message)

        request.data = _message_to_payload(main_message)

    @property
    def return_type(self):
        if self._return_type is None:
            from office365.onenote.pages.page import OnenotePage

            self._return_type = OnenotePage(self.context)
            self.binding_type.add_child(self._return_type)
        return self._return_type
