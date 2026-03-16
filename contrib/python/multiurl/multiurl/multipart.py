# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#
import logging
import re

import requests

from .heuristics import Part, parts_heuristics

LOG = logging.getLogger(__name__)


# S3 does not support multiple ranges
class S3Streamer:
    def __init__(self, url, request, parts, headers, **kwargs):
        self.url = url
        self.parts = parts
        self.request = request
        self.headers = dict(**headers)
        self.kwargs = kwargs

    def __call__(self, chunk_size):
        # See https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html

        headers = dict(**self.headers)
        # TODO: add assertions

        for i, part in enumerate(self.parts):
            if i == 0:
                request = self.request
            else:
                offset, length = part
                headers["range"] = f"bytes={offset}-{offset+length-1}"
                request = requests.get(
                    self.url,
                    stream=True,
                    headers=headers,
                    **self.kwargs,
                )
                try:
                    request.raise_for_status()
                except Exception:
                    LOG.error("URL %s: %s", self.url, request.text)
                    raise

            header = request.headers
            bytes = header["content-range"]
            LOG.debug("HEADERS %s", header)
            m = re.match(r"^bytes (\d+)d?-(\d+)d?/(\d+)d?$", bytes)
            assert m, header
            start, end, total = int(m.group(1)), int(m.group(2)), int(m.group(3))

            assert end >= start
            assert start < total
            assert end < total

            assert start == part.offset, (bytes, part)
            # (end + 1 == total) means that we overshoot the end of the file,
            # this happens when we round transfer blocks
            assert (end == part.offset + part.length - 1) or (end + 1 == total), (
                bytes,
                part,
            )

            yield from request.iter_content(chunk_size)


class MultiPartStreamer:
    def __init__(self, url, request, parts, boundary, **kwargs):
        self.request = request
        self.size = None
        if "content-length" in request.headers:
            self.size = int(request.headers["content-length"])
        self.encoding = "utf-8"
        self.parts = parts
        self.boundary = boundary

    def __call__(self, chunk_size):
        from email.parser import HeaderParser

        from requests.structures import CaseInsensitiveDict

        header_parser = HeaderParser()
        marker = f"--{self.boundary}\r\n".encode(self.encoding)
        end_header = b"\r\n\r\n"
        end_data = b"\r\n"

        end_of_input = f"--{self.boundary}--\r\n".encode(self.encoding)

        if chunk_size < len(end_data):
            chunk_size = len(end_data)

        iter_content = self.request.iter_content(chunk_size)
        chunk = next(iter_content)

        # Some servers start with \r\n
        if chunk[:2] == end_data:
            chunk = chunk[2:]

        LOG.debug("MARKER %s", marker)
        part = 0
        while True:
            while len(chunk) < max(len(marker), len(end_of_input)):
                more = next(iter_content)
                assert more is not None
                chunk += more

            if chunk.find(end_of_input) == 0:
                assert part == len(self.parts)
                break

            pos = chunk.find(marker)
            assert pos == 0, (pos, marker, chunk)

            chunk = chunk[pos + len(marker) :]
            while True:
                pos = chunk.find(end_header)
                LOG.debug("FIND %s %s", end_header, chunk[:80])
                if pos != -1:
                    break
                more = next(iter_content)
                assert more is not None
                chunk += more
                assert len(chunk) < 1024 * 1024

            pos += len(end_header)
            header = chunk[:pos].decode(self.encoding)
            header = CaseInsensitiveDict(header_parser.parsestr(header))
            chunk = chunk[pos:]
            # kind = header["content-type"]
            bytes = header["content-range"]
            LOG.debug("HEADERS %s", header)
            m = re.match(r"^bytes (\d+)d?-(\d+)d?/(\d+)d?$", bytes)
            assert m, header
            start, end, total = int(m.group(1)), int(m.group(2)), int(m.group(3))

            assert end >= start
            assert start < total
            assert end < total

            size = end - start + 1

            assert start == self.parts[part].offset
            # (end + 1 == total) means that we overshoot the end of the file,
            # this happens when we round transfer blocks
            assert (end == self.parts[part].offset + self.parts[part].length - 1) or (
                end + 1 == total
            ), (bytes, self.parts[part])

            while size > 0:
                if len(chunk) >= size:
                    yield chunk[:size]
                    chunk = chunk[size:]
                    size = 0
                else:
                    yield chunk
                    size -= len(chunk)
                    chunk = next(iter_content)

            if len(chunk) == 0:
                chunk = next(iter_content)
                assert chunk

            assert chunk.find(end_data) == 0, chunk
            chunk = chunk[len(end_data) :]
            part += 1


class DecodeMultipart:
    def __init__(self, url, request, parts, **kwargs):
        LOG.debug("URL: %s", url)

        LOG.debug("RESPONSE Headers: %s", request.headers)
        self.request = request
        assert request.status_code == 206, request.status_code

        content_type = request.headers["content-type"]

        if content_type.startswith("multipart/byteranges; boundary="):
            _, boundary = content_type.split("=")
            LOG.debug("******  MULTI-PART supported by server %s", url)
            self.streamer = MultiPartStreamer(url, request, parts, boundary, **kwargs)
        else:
            LOG.debug("******  MULTI-PART *NOT* supported by server %s", url)
            self.streamer = S3Streamer(url, request, parts, **kwargs)

    def __call__(self, chunk_size):
        return self.streamer(chunk_size)


class PartFilter:
    def __init__(self, parts, positions=None):
        self.parts = parts

        if positions is None:
            positions = [x.offset for x in parts]
        self.positions = positions

        assert len(self.parts) == len(self.positions)

    def __call__(self, streamer):
        def execute(chunk_size):
            stream = streamer(chunk_size)
            chunk = next(stream)
            pos = 0
            for (_, length), offset in zip(self.parts, self.positions):
                offset -= pos

                while offset > len(chunk):
                    pos += len(chunk)
                    offset -= len(chunk)
                    chunk = next(stream)
                    assert chunk

                chunk = chunk[offset:]
                pos += offset
                size = length
                while size > 0:
                    if len(chunk) >= size:
                        yield chunk[:size]
                        chunk = chunk[size:]
                        pos += size
                        size = 0
                    else:
                        yield chunk
                        size -= len(chunk)
                        pos += len(chunk)
                        chunk = next(stream)

            # Drain stream, so we don't created error messages in the server's logs
            while True:
                try:
                    next(stream)
                except StopIteration:
                    break

        return execute


def compress_parts(parts):
    last = -1
    result = []
    # Compress and check
    for offset, length in parts:
        assert offset >= 0 and length > 0
        assert offset >= last, (
            f"Offsets and lengths must be in order, and not overlapping:"
            f" offset={offset}, end of previous part={last}"
        )
        if offset == last:
            # Compress
            offset, prev_length = result.pop()
            length += prev_length

        result.append((offset, length))
        last = offset + length
    return tuple(Part(offset, length) for offset, length in result)


def compute_byte_ranges(parts, method, url, statistics_gatherer):
    if callable(method):
        blocks = method(parts)
    else:
        blocks = parts_heuristics(method, statistics_gatherer)(parts)

    blocks = compress_parts(blocks)

    assert len(blocks) > 0
    assert len(blocks) <= len(parts)

    statistics_gatherer(
        "byte-ranges",
        method=str(method),
        url=url,
        parts=parts,
        blocks=blocks,
    )

    i = 0
    positions = []
    block_offset, block_length = blocks[i]
    for offset, length in parts:
        while offset > block_offset + block_length:
            i += 1
            block_offset, block_length = blocks[i]
        start = i
        while offset + length > block_offset + block_length:
            i += 1
            block_offset, block_length = blocks[i]
        end = i
        # Sanity check: assert that each parts is contain in a rounded part
        assert start == end
        positions.append(
            offset - blocks[i].offset + sum(blocks[j].length for j in range(i))
        )

    return blocks, positions
