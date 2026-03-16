#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
from typing import NamedTuple, Union
import os
from ezdxf.lldxf import const


class DWGInfo(NamedTuple):
    version: str = "unknown"
    release: str = "unknown"


def dwg_info(data: bytes) -> DWGInfo:
    """Returns the version and release name of a DWG file."""
    if len(data) < 6:
        return DWGInfo("invalid", "invalid")
    version = data[:6].decode(errors="ignore")
    if version[:4] != "AC10":
        return DWGInfo("invalid", "invalid")
    release = const.acad_release.get(version, "unknown")
    return DWGInfo(version, release)


def dwg_file_info(file: Union[str, os.PathLike]) -> DWGInfo:
    """Returns the version and release name of a DWG file."""
    with open(file, "rb") as fp:
        return dwg_info(fp.read(6))
