# Copyright (c) 2011-2021 Manfred Moitzi
# License: MIT License

from . import factory

# basic classes
from .xdict import ExtensionDict
from .xdata import XData
from .appdata import AppData, Reactors
from .dxfentity import DXFEntity, DXFTagStorage
from .dxfgfx import DXFGraphic, SeqEnd, is_graphic_entity, get_font_name
from .dxfobj import DXFObject, is_dxf_object
from .dxfns import DXFNamespace, SubclassProcessor

# register management structures
from .dxfclass import DXFClass
from .table import TableHead

# register table entries
from .ltype import Linetype
from .layer import Layer, LayerOverrides
from .textstyle import Textstyle
from .dimstyle import DimStyle
from .view import View
from .vport import VPort
from .ucs import UCSTableEntry
from .appid import AppID
from .blockrecord import BlockRecord

# register DXF objects R2000
from .acad_proxy_entity import ACADProxyEntity
from .dxfobj import XRecord, Placeholder, VBAProject, SortEntsTable
from .dictionary import Dictionary, DictionaryVar, DictionaryWithDefault
from .layout import DXFLayout
from .idbuffer import IDBuffer
from .sun import Sun
from .material import Material, MaterialCollection
from .oleframe import OLE2Frame
from .spatial_filter import SpatialFilter

# register DXF objects R2007
from .visualstyle import VisualStyle

# register entities R12
from .line import Line
from .point import Point
from .circle import Circle
from .arc import Arc
from .shape import Shape
from .solid import Solid, Face3d, Trace
from .text import Text
from .subentity import LinkedEntities, entity_linker
from .insert import Insert
from .block import Block, EndBlk
from .polyline import Polyline, Polyface, Polymesh, MeshVertexCache
from .attrib import Attrib, AttDef, copy_attrib_as_text
from .dimension import *
from .dimstyleoverride import DimStyleOverride
from .viewport import Viewport

# register graphical entities R2000
from .lwpolyline import LWPolyline
from .ellipse import Ellipse
from .xline import XLine, Ray
from .mtext import MText
from .mtext_columns import *
from .spline import Spline
from .mesh import Mesh, MeshData
from .boundary_paths import *
from .gradient import *
from .pattern import *
from .hatch import *
from .mpolygon import MPolygon
from .image import Image, ImageDef, Wipeout
from .underlay import (
    Underlay,
    UnderlayDefinition,
    PdfUnderlay,
    DgnUnderlay,
    DwfUnderlay,
)
from .leader import Leader
from .tolerance import Tolerance
from .helix import Helix
from .acis import (
    Body,
    Solid3d,
    Region,
    Surface,
    ExtrudedSurface,
    LoftedSurface,
    RevolvedSurface,
    SweptSurface,
)
from .mline import MLine, MLineVertex, MLineStyle, MLineStyleCollection
from .mleader import MLeader, MLeaderStyle, MLeaderStyleCollection, MultiLeader

# register graphical entities R2004

# register graphical entities R2007

from .light import Light
from .acad_table import (
    AcadTableBlockContent,
    acad_table_to_block,
    read_acad_table_content,
)

# register graphical entities R2010

from .geodata import GeoData

# register graphical entities R2013

# register graphical entities R2018
