# Copyright (c) 2019-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, TextIO, Iterable, Optional

from ezdxf.lldxf.validator import is_dxf_file
from ezdxf.filemanagement import dxf_file_info
from ezdxf.lldxf.tagger import ascii_tags_loader

if TYPE_CHECKING:
    from ezdxf.lldxf.types import DXFTag


def from_stream(stream: TextIO, codes: Optional[set[int]] = None) -> Iterable[DXFTag]:
    """
    Yields comment tags from text `stream` as :class:`~ezdxf.lldxf.types.DXFTag` objects.

    Args:
        stream: input text stream
        codes: set of group codes to yield additional DXF tags e.g. {5, 0} to also yield handle and structure tags

    """
    codes = codes or set()
    codes.add(999)
    return (tag for tag in ascii_tags_loader(stream, skip_comments=False) if tag.code in codes)


def from_file(filename: str, codes: Optional[set[int]] = None) -> Iterable[DXFTag]:
    """
    Yields comment tags from file `filename` as :class:`~ezdxf.lldxf.types.DXFTag` objects.

    Args:
        filename: filename as string
        codes: yields also additional tags with specified group codes e.g. {5, 0} to also yield handle and
               structure tags

    """
    if is_dxf_file(filename):
        info = dxf_file_info(filename)
        with open(filename, mode='rt', encoding=info.encoding) as fp:
            yield from from_stream(fp, codes=codes)
    else:
        raise IOError(f'File "{filename}" is not a DXF file.')
