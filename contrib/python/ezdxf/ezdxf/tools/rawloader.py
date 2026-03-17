# Copyright (c) 2021-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Union, Iterable, TYPE_CHECKING
from pathlib import Path

from ezdxf.lldxf import loader
from ezdxf.lldxf.types import DXFTag
from ezdxf.lldxf.tagger import ascii_tags_loader
from ezdxf.lldxf.validator import is_dxf_file
from ezdxf.filemanagement import dxf_file_info

if TYPE_CHECKING:
    from ezdxf.eztypes import SectionDict


def raw_structure_loader(filename: Union[str, Path]) -> SectionDict:
    """Load content of ASCII DXF file `filename` as SectionDict, all tags are in
    raw format with the group code as integer and the value as string: (int, str).

    """
    tagger = get_tag_loader(filename)
    return loader.load_dxf_structure(tagger)


def get_tag_loader(
    filename: Union[str, Path], errors: str = "ignore"
) -> Iterable[DXFTag]:

    filename = str(filename)
    if not is_dxf_file(filename):
        raise IOError(f"File '{filename}' is not an ASCII DXF file.")

    info = dxf_file_info(filename)
    with open(filename, mode="rt", encoding=info.encoding, errors=errors) as fp:
        return list(ascii_tags_loader(fp, skip_comments=True))
