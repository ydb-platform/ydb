# -*- coding: utf-8 -*-
import io
import sys
import unittest
try:
    from unittest import mock
except ImportError:
    import mock
import pytest
import requests
from requests_toolbelt.multipart.decoder import BodyPart
from requests_toolbelt.multipart.decoder import (
    ImproperBodyPartContentException
)
from requests_toolbelt.multipart.decoder import MultipartDecoder
from requests_toolbelt.multipart.decoder import (
    NonMultipartContentTypeException
)
from requests_toolbelt.multipart.encoder import encode_with
from requests_toolbelt.multipart.encoder import MultipartEncoder


class TestBodyPart(unittest.TestCase):
    @staticmethod
    def u(content):
        major = sys.version_info[0]
        if major == 3:
            return content
        else:
            return unicode(content.replace(r'\\', r'\\\\'), 'unicode_escape')

    @staticmethod
    def bodypart_bytes_from_headers_and_values(headers, value, encoding):
        return b'\r\n\r\n'.join(
            [
                b'\r\n'.join(
                    [
                        b': '.join([encode_with(i, encoding) for i in h])
                        for h in headers
                    ]
                ),
                encode_with(value, encoding)
            ]
        )

    def setUp(self):
        self.header_1 = (TestBodyPart.u('Snowman'), TestBodyPart.u('☃'))
        self.value_1 = TestBodyPart.u('©')
        self.part_1 = BodyPart(
            TestBodyPart.bodypart_bytes_from_headers_and_values(
                (self.header_1,), self.value_1, 'utf-8'
            ),
            'utf-8'
        )
        self.part_2 = BodyPart(
            TestBodyPart.bodypart_bytes_from_headers_and_values(
                [], self.value_1, 'utf-16'
            ),
            'utf-16'
        )

    def test_equality_content_should_be_equal(self):
        part_3 = BodyPart(
            TestBodyPart.bodypart_bytes_from_headers_and_values(
                [], self.value_1, 'utf-8'
            ),
            'utf-8'
        )
        assert self.part_1.content == part_3.content

    def test_equality_content_equals_bytes(self):
        assert self.part_1.content == encode_with(self.value_1, 'utf-8')

    def test_equality_content_should_not_be_equal(self):
        assert self.part_1.content != self.part_2.content

    def test_equality_content_does_not_equal_bytes(self):
        assert self.part_1.content != encode_with(self.value_1, 'latin-1')

    def test_changing_encoding_changes_text(self):
        part_2_orig_text = self.part_2.text
        self.part_2.encoding = 'latin-1'
        assert self.part_2.text != part_2_orig_text

    def test_text_should_be_equal(self):
        assert self.part_1.text == self.part_2.text

    def test_no_headers(self):
        sample_1 = b'\r\n\r\nNo headers\r\nTwo lines'
        part_3 = BodyPart(sample_1, 'utf-8')
        assert len(part_3.headers) == 0
        assert part_3.content == b'No headers\r\nTwo lines'

    def test_no_crlf_crlf_in_content(self):
        content = b'no CRLF CRLF here!\r\n'
        with pytest.raises(ImproperBodyPartContentException):
            BodyPart(content, 'utf-8')


class TestMultipartDecoder(unittest.TestCase):
    def setUp(self):
        self.sample_1 = (
            ('field 1', 'value 1'),
            ('field 2', 'value 2'),
            ('field 3', 'value 3'),
            ('field 4', 'value 4'),
        )
        self.boundary = 'test boundary'
        self.encoded_1 = MultipartEncoder(self.sample_1, self.boundary)
        self.decoded_1 = MultipartDecoder(
            self.encoded_1.to_string(),
            self.encoded_1.content_type
        )

    def test_non_multipart_response_fails(self):
        jpeg_response = mock.NonCallableMagicMock(spec=requests.Response)
        jpeg_response.headers = {'content-type': 'image/jpeg'}
        with pytest.raises(NonMultipartContentTypeException):
            MultipartDecoder.from_response(jpeg_response)

    def test_length_of_parts(self):
        assert len(self.sample_1) == len(self.decoded_1.parts)

    def test_content_of_parts(self):
        def parts_equal(part, sample):
            return part.content == encode_with(sample[1], 'utf-8')

        parts_iter = zip(self.decoded_1.parts, self.sample_1)
        assert all(parts_equal(part, sample) for part, sample in parts_iter)

    def test_header_of_parts(self):
        def parts_header_equal(part, sample):
            return part.headers[b'Content-Disposition'] == encode_with(
                'form-data; name="{}"'.format(sample[0]), 'utf-8'
            )

        parts_iter = zip(self.decoded_1.parts, self.sample_1)
        assert all(
            parts_header_equal(part, sample)
            for part, sample in parts_iter
        )

    def test_from_response(self):
        response = mock.NonCallableMagicMock(spec=requests.Response)
        response.headers = {
            'content-type': 'multipart/related; boundary="samp1"'
        }
        cnt = io.BytesIO()
        cnt.write(b'\r\n--samp1\r\n')
        cnt.write(b'Header-1: Header-Value-1\r\n')
        cnt.write(b'Header-2: Header-Value-2\r\n')
        cnt.write(b'\r\n')
        cnt.write(b'Body 1, Line 1\r\n')
        cnt.write(b'Body 1, Line 2\r\n')
        cnt.write(b'--samp1\r\n')
        cnt.write(b'\r\n')
        cnt.write(b'Body 2, Line 1\r\n')
        cnt.write(b'--samp1--\r\n')
        response.content = cnt.getvalue()
        decoder_2 = MultipartDecoder.from_response(response)
        assert decoder_2.content_type == response.headers['content-type']
        assert (
            decoder_2.parts[0].content == b'Body 1, Line 1\r\nBody 1, Line 2'
        )
        assert decoder_2.parts[0].headers[b'Header-1'] == b'Header-Value-1'
        assert len(decoder_2.parts[1].headers) == 0
        assert decoder_2.parts[1].content == b'Body 2, Line 1'

    def test_from_responsecaplarge(self):
        response = mock.NonCallableMagicMock(spec=requests.Response)
        response.headers = {
            'content-type': 'Multipart/Related; boundary="samp1"'
        }
        cnt = io.BytesIO()
        cnt.write(b'\r\n--samp1\r\n')
        cnt.write(b'Header-1: Header-Value-1\r\n')
        cnt.write(b'Header-2: Header-Value-2\r\n')
        cnt.write(b'\r\n')
        cnt.write(b'Body 1, Line 1\r\n')
        cnt.write(b'Body 1, Line 2\r\n')
        cnt.write(b'--samp1\r\n')
        cnt.write(b'\r\n')
        cnt.write(b'Body 2, Line 1\r\n')
        cnt.write(b'--samp1--\r\n')
        response.content = cnt.getvalue()
        decoder_2 = MultipartDecoder.from_response(response)
        assert decoder_2.content_type == response.headers['content-type']
        assert (
            decoder_2.parts[0].content == b'Body 1, Line 1\r\nBody 1, Line 2'
        )
        assert decoder_2.parts[0].headers[b'Header-1'] == b'Header-Value-1'
        assert len(decoder_2.parts[1].headers) == 0
        assert decoder_2.parts[1].content == b'Body 2, Line 1'
