# Copyright (c) 2022, Manfred Moitzi
# License: MIT License
# This add-on was created to solve this problem: https://github.com/mozman/ezdxf/discussions/789
from __future__ import annotations
from typing import TextIO
import os
import io

import ezdxf
from ezdxf.document import Drawing
from ezdxf.layouts import Modelspace
from ezdxf.lldxf.tagwriter import TagWriter, AbstractTagWriter

__all__ = ["export_file", "export_stream"]


def export_file(doc: Drawing, filename: str | os.PathLike) -> None:
    """Exports the specified DXF R12 document, which should contain content conforming
    to the ASTM-D6673-10 standard, in a special way so that Gerber Technology applications
    can parse it by their low-quality DXF parser.
    """
    fp = io.open(filename, mode="wt", encoding="ascii", errors="dxfreplace")
    export_stream(doc, fp)


def export_stream(doc: Drawing, stream: TextIO) -> None:
    """Exports the specified DXF R12 document into a `stream` object."""

    if doc.dxfversion != ezdxf.const.DXF12:
        raise ezdxf.DXFVersionError("only DXF R12 format is supported")
    tagwriter = TagWriter(stream, write_handles=False, dxfversion=ezdxf.const.DXF12)
    _export_sections(doc, tagwriter)


def _export_sections(doc: Drawing, tagwriter: AbstractTagWriter) -> None:
    _export_header(tagwriter)
    _export_blocks(doc, tagwriter)
    _export_entities(doc.modelspace(), tagwriter)


def _export_header(tagwriter: AbstractTagWriter) -> None:
    # export empty header section
    tagwriter.write_str("  0\nSECTION\n  2\nHEADER\n")
    tagwriter.write_tag2(0, "ENDSEC")


def _export_blocks(doc: Drawing, tagwriter: AbstractTagWriter) -> None:
    # This is the important part:
    #
    # Gerber Technology applications have a bad DXF parser which do not accept DXF
    # files that contain blocks without ASTM-D6673-10 content, such as the standard
    # $MODEL_SPACE and $PAPER_SPACE block definitions.
    #
    # This is annoying but the presence of these blocks is NOT mandatory for
    # the DXF R12 standard.
    #
    tagwriter.write_str("  0\nSECTION\n  2\nBLOCKS\n")
    for block_record in doc.block_records:
        # export only BLOCK definitions, ignore LAYOUT definition blocks
        if block_record.is_block_layout:
            block_record.export_block_definition(tagwriter)
    tagwriter.write_tag2(0, "ENDSEC")


def _export_entities(msp: Modelspace, tagwriter: AbstractTagWriter) -> None:
    tagwriter.write_str("  0\nSECTION\n  2\nENTITIES\n")
    msp.entity_space.export_dxf(tagwriter)
    tagwriter.write_tag2(0, "ENDSEC")
    tagwriter.write_tag2(0, "EOF")
