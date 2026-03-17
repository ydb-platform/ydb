# Copyright (c) 2022-2024, Manfred Moitzi
# License: MIT License
# Public API module (interface)
"""
The main goals of this ACIS support library is:

    1. load and parse simple and known ACIS data structures
    2. create and export simple and known ACIS data structures

It is NOT a goal to edit and export arbitrary existing ACIS structures.

    Don't even try it!

This modules do not implement an ACIS kernel!!!
So tasks beyond stitching some flat polygonal faces to a polyhedron or creating
simple curves is not possible.

To all beginners: GO AWAY!

"""
from .const import (
    AcisException,
    ParsingError,
    InvalidLinkStructure,
    ExportError,
)
from .mesh import mesh_from_body, body_from_mesh, vertices_from_body
from .entities import load, export_sat, export_sab, Body
from .dbg import AcisDebugger, dump_sab_as_text
from .dxf import export_dxf, load_dxf
from .cache import AcisCache
