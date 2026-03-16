# Copyright (C) 2011-2023, Manfred Moitzi
# License: MIT License
"""Ezdxf is an interface library for the DXF file format.

The package is designed to facilitate the creation and manipulation of DXF
documents, with compatibility across various DXF versions. It empowers users to
seamlessly load and edit DXF files while preserving all content, except for comments.

Any unfamiliar DXF tags encountered in the document are gracefully ignored but retained
for future modifications. This feature enables the processing of DXF documents
containing data from third-party applications without any loss of valuable information.
"""
from typing import TextIO, Optional
import sys
import os
from .version import version, __version__

VERSION = __version__
__author__ = "mozman <me@mozman.at>"

TRUE_STATE = {"True", "true", "On", "on", "1"}
PYPY = hasattr(sys, "pypy_version_info")
PYPY_ON_WINDOWS = sys.platform.startswith("win") and PYPY

# name space imports - do not remove
from ezdxf._options import options, config_files
from ezdxf.colors import (
    int2rgb,
    rgb2int,
    transparency2float,
    float2transparency,
)
from ezdxf.enums import InsertUnits
from ezdxf.lldxf import const
from ezdxf.lldxf.validator import is_dxf_file, is_dxf_stream
from ezdxf.filemanagement import readzip, new, read, readfile, decode_base64
from ezdxf.tools.standards import (
    setup_linetypes,
    setup_styles,
    setup_dimstyles,
    setup_dimstyle,
)
from ezdxf.tools import pattern
from ezdxf.render.arrows import ARROWS
from ezdxf.lldxf.const import (
    DXFError,
    DXFStructureError,
    DXFVersionError,
    DXFTableEntryError,
    DXFAppDataError,
    DXFXDataError,
    DXFAttributeError,
    DXFValueError,
    DXFKeyError,
    DXFIndexError,
    DXFTypeError,
    DXFBlockInUseError,
    InvalidGeoDataException,
    DXF12,
    DXF2000,
    DXF2004,
    DXF2007,
    DXF2010,
    DXF2013,
    DXF2018,
)

# name space imports - do not remove

import codecs
from ezdxf.lldxf.encoding import (
    dxf_backslash_replace,
    has_dxf_unicode,
    decode_dxf_unicode,
)


# setup DXF unicode encoder -> '\U+nnnn'
codecs.register_error("dxfreplace", dxf_backslash_replace)

EZDXF_TEST_FILES = options.test_files
YES_NO = {True: "yes", False: "no"}


def print_config(verbose: bool = False, stream: Optional[TextIO] = None) -> None:
    from pathlib import Path

    if stream is None:
        stream = sys.stdout
    stream.write(
        "\n".join([
            f"ezdxf {__version__} from {Path(__file__).parent}",
            f"Python version: {sys.version}",
            f"using C-extensions: {YES_NO[options.use_c_ext]}\n",
        ])
    )
    if verbose:
        stream.write("\nConfiguration:\n")
        options.write(stream)
        stream.write("\nEnvironment Variables:\n")
        for v in options.CONFIG_VARS:
            stream.write(f"{v}={os.environ.get(v, '')}\n")

        stream.write("\nLoaded Config Files:\n")
        for path in options.loaded_config_files:
            stream.write(str(path.absolute()) + "\n")
