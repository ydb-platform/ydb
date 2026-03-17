# Copyright 2014 Globo.com Player authors. All rights reserved.
# Use of this source code is governed by a MIT License
# license that can be found in the LICENSE file.

import os
from urllib.parse import urljoin, urlsplit

from m3u8.httpclient import DefaultHTTPClient
from m3u8.model import (
    M3U8,
    ContentSteering,
    DateRange,
    DateRangeList,
    IFramePlaylist,
    ImagePlaylist,
    Key,
    Media,
    MediaList,
    PartialSegment,
    PartialSegmentList,
    PartInformation,
    Playlist,
    PlaylistList,
    PreloadHint,
    RenditionReport,
    RenditionReportList,
    Segment,
    SegmentList,
    ServerControl,
    Skip,
    Start,
    Tiles,
)
from m3u8.parser import ParseError, parse

__all__ = (
    "M3U8",
    "Segment",
    "SegmentList",
    "PartialSegment",
    "PartialSegmentList",
    "Key",
    "Playlist",
    "IFramePlaylist",
    "Media",
    "MediaList",
    "PlaylistList",
    "Start",
    "RenditionReport",
    "RenditionReportList",
    "ServerControl",
    "Skip",
    "PartInformation",
    "PreloadHint",
    "DateRange",
    "DateRangeList",
    "ContentSteering",
    "ImagePlaylist",
    "Tiles",
    "loads",
    "load",
    "parse",
    "ParseError",
)


def loads(content, uri=None, custom_tags_parser=None):
    """
    Given a string with a m3u8 content, returns a M3U8 object.
    Optionally parses a uri to set a correct base_uri on the M3U8 object.
    Raises ValueError if invalid content
    """

    if uri is None:
        return M3U8(content, custom_tags_parser=custom_tags_parser)
    else:
        base_uri = urljoin(uri, ".")
        return M3U8(content, base_uri=base_uri, custom_tags_parser=custom_tags_parser)


def load(
    uri,
    timeout=None,
    headers={},
    custom_tags_parser=None,
    http_client=DefaultHTTPClient(),
    verify_ssl=True,
):
    """
    Retrieves the content from a given URI and returns a M3U8 object.
    Raises ValueError if invalid content or IOError if request fails.
    """
    if urlsplit(uri).scheme:
        content, base_uri = http_client.download(uri, timeout, headers, verify_ssl)
        return M3U8(content, base_uri=base_uri, custom_tags_parser=custom_tags_parser)
    else:
        return _load_from_file(uri, custom_tags_parser)


def _load_from_file(uri, custom_tags_parser=None):
    with open(uri, encoding="utf8") as fileobj:
        raw_content = fileobj.read().strip()
    base_uri = os.path.dirname(uri)
    return M3U8(raw_content, base_uri=base_uri, custom_tags_parser=custom_tags_parser)
