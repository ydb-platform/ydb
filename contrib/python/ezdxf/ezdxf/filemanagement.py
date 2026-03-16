# Copyright (C) 2018-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TextIO, TYPE_CHECKING, Union, Sequence, Optional
import base64
import io
import pathlib
import os

from ezdxf.tools.standards import setup_drawing
from ezdxf.lldxf.const import DXF2013
from ezdxf.document import Drawing

if TYPE_CHECKING:
    from ezdxf.lldxf.validator import DXFInfo


def new(
    dxfversion: str = DXF2013,
    setup: Union[str, bool, Sequence[str]] = False,
    units: int = 6,
) -> Drawing:
    """Create a new :class:`~ezdxf.drawing.Drawing` from scratch, `dxfversion`
    can be either "AC1009" the official DXF version name or "R12" the
    AutoCAD release name.

    :func:`new` can create drawings for following DXF versions:

    ======= ========================
    Version AutoCAD Release
    ======= ========================
    AC1009  AutoCAD R12
    AC1015  AutoCAD R2000
    AC1018  AutoCAD R2004
    AC1021  AutoCAD R2007
    AC1024  AutoCAD R2010
    AC1027  AutoCAD R2013
    AC1032  AutoCAD R2018
    ======= ========================

    The `units` argument defines th document and modelspace units. The header
    variable $MEASUREMENT will be set according to the given `units`, 0 for
    inch, feet, miles, ... and 1 for metric units. For more information go to
    module :mod:`ezdxf.units`

    Args:
        dxfversion: DXF version specifier as string, default is "AC1027"
            respectively "R2013"
        setup: setup default styles, ``False`` for no setup,
            ``True`` to setup everything or a list of topics as strings,
            e.g. ["linetypes", "styles"] to setup only some topics:

            ================== ========================================
            Topic              Description
            ================== ========================================
            linetypes          setup line types
            styles             setup text styles
            dimstyles          setup default `ezdxf` dimension styles
            visualstyles       setup 25 standard visual styles
            ================== ========================================
        units: document and modelspace units, default is 6 for meters

    """
    doc = Drawing.new(dxfversion)
    doc.units = units
    doc.header["$MEASUREMENT"] = 0 if units in (1, 2, 3, 8, 9, 10) else 1
    if setup:
        setup_drawing(doc, topics=setup)
    return doc


def read(stream: TextIO) -> Drawing:
    """Read a DXF document from a text-stream. Open stream in text mode
    (``mode='rt'``) and set correct text encoding, the stream requires at least
    a :meth:`readline` method.

    Since DXF version R2007 (AC1021) file encoding is always "utf-8",
    use the helper function :func:`dxf_stream_info` to detect the required
    text encoding for prior DXF versions. To preserve possible binary data in
    use :code:`errors='surrogateescape'` as error handler for the import stream.

    If this function struggles to load the DXF document and raises a
    :class:`DXFStructureError` exception, try the :func:`ezdxf.recover.read`
    function to load this corrupt DXF document.

    Args:
        stream: input text stream opened with correct encoding

    Raises:
        DXFStructureError: for invalid or corrupted DXF structures

    """
    from ezdxf.document import Drawing

    return Drawing.read(stream)


def readfile(
    filename: str | os.PathLike,
    encoding: Optional[str] = None,
    errors: str = "surrogateescape",
) -> Drawing:
    """Read the DXF document `filename` from the file-system.

    This is the preferred method to load existing ASCII or Binary DXF files,
    the required text encoding will be detected automatically and decoding
    errors will be ignored.

    Override encoding detection by setting argument `encoding` to the
    estimated encoding. (use Python encoding names like in the :func:`open`
    function).

    If this function struggles to load the DXF document and raises a
    :class:`DXFStructureError` exception, try the :func:`ezdxf.recover.readfile`
    function to load this corrupt DXF document.

    Args:
        filename: filename of the ASCII- or Binary DXF document
        encoding: use ``None`` for auto detect (default), or set a specific
            encoding like "utf-8", argument is ignored for Binary DXF files
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        IOError: not a DXF file or file does not exist
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    from ezdxf.lldxf.validator import is_dxf_file, is_binary_dxf_file
    from ezdxf.tools.codepage import is_supported_encoding
    from ezdxf.lldxf.tagger import binary_tags_loader

    filename = str(filename)
    if is_binary_dxf_file(filename):
        with open(filename, "rb") as fp:
            data = fp.read()
            loader = binary_tags_loader(data, errors=errors)
            doc = Drawing.load(loader)
            doc.filename = filename
            return doc

    if not is_dxf_file(filename):
        raise IOError(f"File '{filename}' is not a DXF file.")

    info = dxf_file_info(filename)
    if encoding is not None:
        # override default encodings if absolute necessary
        info.encoding = encoding
    with open(filename, mode="rt", encoding=info.encoding, errors=errors) as fp:
        doc = read(fp)

    doc.filename = filename
    if encoding is not None and is_supported_encoding(encoding):
        # store overridden encoding if supported by AutoCAD, else default
        # encoding stored in $DWGENCODING is used as document encoding or
        # 'cp1252' if $DWGENCODING is unset.
        doc.encoding = encoding
    return doc


def dxf_file_info(filename: str | os.PathLike) -> DXFInfo:
    """Reads basic file information from a DXF document: DXF version, encoding
    and handle seed.

    """
    filename = str(filename)
    with open(filename, mode="rt", encoding="utf-8", errors="ignore") as fp:
        return dxf_stream_info(fp)


def dxf_stream_info(stream: TextIO) -> DXFInfo:
    """Reads basic DXF information from a text stream: DXF version, encoding
    and handle seed.

    """
    from ezdxf.lldxf.validator import dxf_info

    info = dxf_info(stream)
    # R2007 files and later are always encoded as UTF-8
    if info.version >= "AC1021":
        info.encoding = "utf-8"
    return info


def readzip(
    zipfile: str | os.PathLike,
    filename: Optional[str] = None,
    errors: str = "surrogateescape",
) -> Drawing:
    """Load a DXF document specified by `filename` from a zip archive, or if
    `filename` is ``None`` the first DXF document in the zip archive.

    Args:
        zipfile: name of the zip archive
        filename: filename of DXF file, or ``None`` to load the first DXF
            document from the zip archive.
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        IOError: not a DXF file or file does not exist or
            if `filename` is ``None`` - no DXF file found
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    from ezdxf.tools.zipmanager import ctxZipReader

    with ctxZipReader(str(zipfile), filename, errors=errors) as zipstream:
        doc = read(zipstream)  # type: ignore
        doc.filename = zipstream.dxf_file_name
    return doc


def decode_base64(data: bytes, errors: str = "surrogateescape") -> Drawing:
    """Load a DXF document from base64 encoded binary data, like uploaded data
    to web applications.

    Args:
        data: DXF document base64 encoded binary data
        errors: specify decoding error handler

            - "surrogateescape" to preserve possible binary data (default)
            - "ignore" to use the replacement char U+FFFD "\ufffd" for invalid data
            - "strict" to raise an :class:`UnicodeDecodeError` exception for invalid data

    Raises:
        DXFStructureError: for invalid or corrupted DXF structures
        UnicodeDecodeError: if `errors` is "strict" and a decoding error occurs

    """
    # Copyright (c) 2020, Joseph Flack
    # License: MIT License
    # Decode base64 encoded data into binary data
    binary_data = base64.b64decode(data)

    # Replace windows line ending '\r\n' by universal line ending '\n'
    binary_data = binary_data.replace(b"\r\n", b"\n")

    # Read DXF file info from data, basic DXF information in the HEADER
    # section is ASCII encoded so encoding setting here is not important
    # for this task:
    text = binary_data.decode("utf-8", errors="ignore")
    stream = io.StringIO(text)
    info = dxf_stream_info(stream)
    stream.close()

    # Use encoding info to create correct decoded text input stream for ezdxf
    text = binary_data.decode(info.encoding, errors=errors)
    stream = io.StringIO(text)

    # Load DXF document from data stream
    doc = read(stream)
    stream.close()
    return doc


def find_support_file(
    filename: str, support_dirs: Optional[Sequence[str]] = None
) -> str:
    """Find file `filename` in the support directories`."""
    if pathlib.Path(filename).exists():
        return filename
    if support_dirs is None:
        support_dirs = []
    for directory in support_dirs:
        directory = directory.strip("\"'")
        filepath = pathlib.Path(directory).expanduser() / filename
        if filepath.exists():
            return str(filepath)
    return filename
