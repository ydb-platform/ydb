#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import NamedTuple


class CompressionType(NamedTuple):
    name: str
    file_extension: str
    mime_type: str
    mime_subtypes: list[str]
    is_supported: bool


CompressionTypes = {
    "GZIP": CompressionType(
        name="GZIP",
        file_extension=".gz",
        mime_type="application",
        mime_subtypes=["gzip", "x-gzip"],
        is_supported=True,
    ),
    "DEFLATE": CompressionType(
        name="DEFLATE",
        file_extension=".deflate",
        mime_type="application",
        mime_subtypes=["zlib", "deflate"],
        is_supported=True,
    ),
    "RAW_DEFLATE": CompressionType(
        name="RAW_DEFLATE",
        file_extension=".raw_deflate",
        mime_type="application",
        mime_subtypes=["raw_deflate"],
        is_supported=True,
    ),
    "BZIP2": CompressionType(
        name="BZIP2",
        file_extension=".bz2",
        mime_type="application",
        mime_subtypes=["bzip2", "x-bzip2", "x-bz2", "x-bzip", "bz2"],
        is_supported=True,
    ),
    "LZIP": CompressionType(
        name="LZIP",
        file_extension=".lz",
        mime_type="application",
        mime_subtypes=["lzip", "x-lzip"],
        is_supported=False,
    ),
    "LZMA": CompressionType(
        name="LZMA",
        file_extension=".lzma",
        mime_type="application",
        mime_subtypes=["lzma", "x-lzma"],
        is_supported=False,
    ),
    "LZO": CompressionType(
        name="LZO",
        file_extension=".lzo",
        mime_type="application",
        mime_subtypes=["lzo", "x-lzo"],
        is_supported=False,
    ),
    "XZ": CompressionType(
        name="XZ",
        file_extension=".xz",
        mime_type="application",
        mime_subtypes=["xz", "x-xz"],
        is_supported=False,
    ),
    "COMPRESS": CompressionType(
        name="COMPRESS",
        file_extension=".Z",
        mime_type="application",
        mime_subtypes=["compress", "x-compress"],
        is_supported=False,
    ),
    "PARQUET": CompressionType(
        name="PARQUET",
        file_extension=".parquet",
        mime_type="snowflake",
        mime_subtypes=["parquet"],
        is_supported=True,
    ),
    "ZSTD": CompressionType(
        name="ZSTD",
        file_extension=".zst",
        mime_type="application",
        mime_subtypes=["zstd", "x-zstd"],
        is_supported=True,
    ),
    "BROTLI": CompressionType(
        name="BROTLI",
        file_extension=".br",
        mime_type="application",
        mime_subtypes=["br", "x-br"],
        is_supported=True,
    ),
    "ORC": CompressionType(
        name="ORC",
        file_extension=".orc",
        mime_type="snowflake",
        mime_subtypes=["orc"],
        is_supported=True,
    ),
}

subtype_to_meta: dict[str, CompressionType] = {
    ms.lower(): meta for meta in CompressionTypes.values() for ms in meta.mime_subtypes
}

# TODO: Snappy avro doesn't need to be compressed again


def lookup_by_mime_sub_type(mime_subtype: str) -> CompressionType | None:
    """Look up a CompressionType for a specific mime subtype."""
    return subtype_to_meta.get(mime_subtype.lower())
