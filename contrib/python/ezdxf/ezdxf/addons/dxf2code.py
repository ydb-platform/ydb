# Copyright (c) 2019-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Mapping, Optional
import json

from ezdxf.sections.tables import TABLENAMES
from ezdxf.lldxf.tags import Tags
from ezdxf.entities import BoundaryPathType, EdgeType
import numpy as np

if TYPE_CHECKING:
    from ezdxf.lldxf.types import DXFTag
    from ezdxf.entities import (
        Insert,
        MText,
        LWPolyline,
        Polyline,
        Spline,
        Leader,
        Dimension,
        Image,
        Mesh,
        Hatch,
        MPolygon,
        Wipeout,
    )
    from ezdxf.entities import DXFEntity, Linetype
    from ezdxf.entities.polygon import DXFPolygon
    from ezdxf.layouts import BlockLayout

__all__ = [
    "entities_to_code",
    "block_to_code",
    "table_entries_to_code",
    "black",
]


def black(code: str, line_length=88, fast: bool = True) -> str:
    """Returns the source `code` as a single string formatted by `Black`_

    Requires the installed `Black`_ formatter::

        pip3 install black

    Args:
        code: source code
        line_length: max. source code line length
        fast: ``True`` for fast mode, ``False`` to check that the reformatted
            code is valid

    Raises:
        ImportError: Black is not available

    .. _black: https://pypi.org/project/black/

    """

    import black

    mode = black.FileMode()
    mode.line_length = line_length
    return black.format_file_contents(code, fast=fast, mode=mode)


def entities_to_code(
    entities: Iterable[DXFEntity],
    layout: str = "layout",
    ignore: Optional[Iterable[str]] = None,
) -> Code:
    """
    Translates DXF entities into Python source code to recreate this entities
    by ezdxf.

    Args:
        entities: iterable of DXFEntity
        layout: variable name of the layout (model space or block) as string
        ignore: iterable of entities types to ignore as strings
            like ``['IMAGE', 'DIMENSION']``

    Returns:
        :class:`Code`

    """
    code = _SourceCodeGenerator(layout=layout)
    code.translate_entities(entities, ignore=ignore)
    return code.code


def block_to_code(
    block: BlockLayout,
    drawing: str = "doc",
    ignore: Optional[Iterable[str]] = None,
) -> Code:
    """
    Translates a BLOCK into Python source code to recreate the BLOCK by ezdxf.

    Args:
        block: block definition layout
        drawing: variable name of the drawing as string
        ignore: iterable of entities types to ignore as strings
            like ['IMAGE', 'DIMENSION']

    Returns:
        :class:`Code`

    """
    assert block.block is not None
    dxfattribs = _purge_handles(block.block.dxfattribs())
    block_name = dxfattribs.pop("name")
    base_point = dxfattribs.pop("base_point")
    code = _SourceCodeGenerator(layout="b")
    prolog = f'b = {drawing}.blocks.new("{block_name}", base_point={base_point}, dxfattribs={{'
    code.add_source_code_line(prolog)
    code.add_source_code_lines(_fmt_mapping(dxfattribs, indent=4))
    code.add_source_code_line("    }")
    code.add_source_code_line(")")
    code.translate_entities(block, ignore=ignore)
    return code.code


def table_entries_to_code(
    entities: Iterable[DXFEntity], drawing="doc"
) -> Code:
    code = _SourceCodeGenerator(doc=drawing)
    code.translate_entities(entities)
    return code.code


class Code:
    """Source code container."""

    def __init__(self) -> None:
        self.code: list[str] = []
        # global imports -> indentation level 0:
        self.imports: set[str] = set()
        # layer names as string:
        self.layers: set[str] = set()
        # text style name as string, requires a TABLE entry:
        self.styles: set[str] = set()
        # line type names as string, requires a TABLE entry:
        self.linetypes: set[str] = set()
        # dimension style names as string, requires a TABLE entry:
        self.dimstyles: set[str] = set()
        # block names as string, requires a BLOCK definition:
        self.blocks: set[str] = set()

    def code_str(self, indent: int = 0) -> str:
        """Returns the source code as a single string.

        Args:
            indent: source code indentation count by spaces

        """
        lead_str = " " * indent
        return "\n".join(lead_str + line for line in self.code)

    def black_code_str(self, line_length=88) -> str:
        """Returns the source code as a single string formatted by `Black`_

        Args:
            line_length: max. source code line length

        Raises:
            ImportError: Black is not available

        """
        return black(self.code_str(), line_length)

    def __str__(self) -> str:
        """Returns the source code as a single string."""

        return self.code_str()

    def import_str(self, indent: int = 0) -> str:
        """Returns required imports as a single string.

        Args:
            indent: source code indentation count by spaces

        """
        lead_str = " " * indent
        return "\n".join(lead_str + line for line in self.imports)

    def add_import(self, statement: str) -> None:
        """Add import statement, identical import statements are merged
        together.
        """
        self.imports.add(statement)

    def add_line(self, code: str, indent: int = 0) -> None:
        """Add a single source code line without line ending ``\\n``."""
        self.code.append(" " * indent + code)

    def add_lines(self, code: Iterable[str], indent: int = 0) -> None:
        """Add multiple source code lines without line ending ``\\n``."""
        for line in code:
            self.add_line(line, indent=indent)

    def merge(self, code: Code, indent: int = 0) -> None:
        """Add another :class:`Code` object."""
        # merge used resources
        self.imports.update(code.imports)
        self.layers.update(code.layers)
        self.linetypes.update(code.linetypes)
        self.styles.update(code.styles)
        self.dimstyles.update(code.dimstyles)
        self.blocks.update(code.blocks)

        # append source code lines
        self.add_lines(code.code, indent=indent)


_PURGE_DXF_ATTRIBUTES = {
    "handle",
    "owner",
    "paperspace",
    "material_handle",
    "visualstyle_handle",
    "plotstyle_handle",
}


def _purge_handles(attribs: dict) -> dict:
    """Purge handles from DXF attributes which will be invalid in a new
    document, or which will be set automatically by adding an entity to a
    layout (paperspace).

    Args:
        attribs: entity DXF attributes dictionary

    """
    return {k: v for k, v in attribs.items() if k not in _PURGE_DXF_ATTRIBUTES}


def _fmt_mapping(mapping: Mapping, indent: int = 0) -> Iterable[str]:
    # key is always a string
    fmt = " " * indent + "'{}': {},"
    for k, v in mapping.items():
        assert isinstance(k, str)
        if isinstance(v, str):
            v = json.dumps(v)  # for correct escaping of quotes
        else:
            v = str(v)  # format uses repr() for Vec3s
        yield fmt.format(k, v)


def _fmt_list(l: Iterable, indent: int = 0) -> Iterable[str]:
    def cleanup(values: Iterable) -> Iterable:
        for value in values:
            if isinstance(value, np.float64):
                yield float(value)
            else:
                yield value

    fmt = " " * indent + "{},"
    for v in l:
        if not isinstance(v, (float, int, str)):
            v = tuple(cleanup(v))
        yield fmt.format(str(v))


def _fmt_api_call(
    func_call: str, args: Iterable[str], dxfattribs: dict
) -> list[str]:
    attributes = dict(dxfattribs)
    args = list(args) if args else []

    def fmt_keywords() -> Iterable[str]:
        for arg in args:
            if arg not in attributes:
                continue
            value = attributes.pop(arg)
            if isinstance(value, str):
                valuestr = json.dumps(value)  # quoted string!
            else:
                valuestr = str(value)
            yield "    {}={},".format(arg, valuestr)

    s = [func_call]
    s.extend(fmt_keywords())
    s.append("    dxfattribs={")
    s.extend(_fmt_mapping(attributes, indent=8))
    s.extend(
        [
            "    },",
            ")",
        ]
    )
    return s


def _fmt_dxf_tags(tags: Iterable[DXFTag], indent: int = 0):
    fmt = " " * indent + "dxftag({}, {}),"
    for code, value in tags:
        assert isinstance(code, int)
        if isinstance(value, str):
            value = json.dumps(value)  # for correct escaping of quotes
        else:
            value = str(value)  # format uses repr() for Vec3s
        yield fmt.format(code, value)


class _SourceCodeGenerator:
    """
    The :class:`_SourceCodeGenerator` translates DXF entities into Python source
    code for creating the same DXF entity in another model space or block
    definition.

    :ivar code: list of source code lines without line endings
    :ivar required_imports: list of import source code lines, which are required
        to create executable Python code.

    """

    def __init__(self, layout: str = "layout", doc: str = "doc"):
        self.doc = doc
        self.layout = layout
        self.code = Code()

    def translate_entity(self, entity: DXFEntity) -> None:
        """Translates one DXF entity into Python source code. The generated
        source code is appended to the attribute `source_code`.

        Args:
            entity: DXFEntity object

        """
        dxftype = entity.dxftype()
        try:
            entity_translator = getattr(self, "_" + dxftype.lower())
        except AttributeError:
            self.add_source_code_line(f'# unsupported DXF entity "{dxftype}"')
        else:
            entity_translator(entity)

    def translate_entities(
        self,
        entities: Iterable[DXFEntity],
        ignore: Optional[Iterable[str]] = None,
    ) -> None:
        """Translates multiple DXF entities into Python source code. The
        generated source code is appended to the attribute `source_code`.

        Args:
            entities: iterable of DXFEntity
            ignore: iterable of entities types to ignore as strings
                like ['IMAGE', 'DIMENSION']

        """
        ignore = set(ignore) if ignore else set()

        for entity in entities:
            if entity.dxftype() not in ignore:
                self.translate_entity(entity)

    def add_used_resources(self, dxfattribs: Mapping) -> None:
        """Register used resources like layers, line types, text styles and
        dimension styles.

        Args:
            dxfattribs: DXF attributes dictionary

        """
        if "layer" in dxfattribs:
            self.code.layers.add(dxfattribs["layer"])
        if "linetype" in dxfattribs:
            self.code.linetypes.add(dxfattribs["linetype"])
        if "style" in dxfattribs:
            self.code.styles.add(dxfattribs["style"])
        if "dimstyle" in dxfattribs:
            self.code.dimstyles.add(dxfattribs["dimstyle"])

    def add_import_statement(self, statement: str) -> None:
        self.code.add_import(statement)

    def add_source_code_line(self, code: str) -> None:
        self.code.add_line(code)

    def add_source_code_lines(self, code: Iterable[str]) -> None:
        assert not isinstance(code, str)
        self.code.add_lines(code)

    def add_list_source_code(
        self,
        values: Iterable,
        prolog: str = "[",
        epilog: str = "]",
        indent: int = 0,
    ) -> None:
        fmt_str = " " * indent + "{}"
        self.add_source_code_line(fmt_str.format(prolog))
        self.add_source_code_lines(_fmt_list(values, indent=4 + indent))
        self.add_source_code_line(fmt_str.format(epilog))

    def add_dict_source_code(
        self,
        mapping: Mapping,
        prolog: str = "{",
        epilog: str = "}",
        indent: int = 0,
    ) -> None:
        fmt_str = " " * indent + "{}"
        self.add_source_code_line(fmt_str.format(prolog))
        self.add_source_code_lines(_fmt_mapping(mapping, indent=4 + indent))
        self.add_source_code_line(fmt_str.format(epilog))

    def add_tags_source_code(
        self, tags: Tags, prolog="tags = Tags(", epilog=")", indent=4
    ):
        fmt_str = " " * indent + "{}"
        self.add_source_code_line(fmt_str.format(prolog))
        self.add_source_code_lines(_fmt_dxf_tags(tags, indent=4 + indent))
        self.add_source_code_line(fmt_str.format(epilog))

    def generic_api_call(
        self, dxftype: str, dxfattribs: dict, prefix: str = "e = "
    ) -> Iterable[str]:
        """Returns the source code strings to create a DXF entity by a generic
        `new_entity()` call.

        Args:
            dxftype: DXF entity type as string, like 'LINE'
            dxfattribs: DXF attributes dictionary
            prefix: prefix string like variable assignment 'e = '

        """
        dxfattribs = _purge_handles(dxfattribs)
        self.add_used_resources(dxfattribs)
        s = [
            f"{prefix}{self.layout}.new_entity(",
            f"    '{dxftype}',",
            "    dxfattribs={",
        ]
        s.extend(_fmt_mapping(dxfattribs, indent=8))
        s.extend(
            [
                "    },",
                ")",
            ]
        )
        return s

    def api_call(
        self,
        api_call: str,
        args: Iterable[str],
        dxfattribs: dict,
        prefix: str = "e = ",
    ) -> Iterable[str]:
        """Returns the source code strings to create a DXF entity by the
        specialised API call.

        Args:
            api_call: API function call like 'add_line('
            args: DXF attributes to pass as arguments
            dxfattribs: DXF attributes dictionary
            prefix: prefix string like variable assignment 'e = '

        """
        dxfattribs = _purge_handles(dxfattribs)
        func_call = f"{prefix}{self.layout}.{api_call}"
        return _fmt_api_call(func_call, args, dxfattribs)

    def new_table_entry(self, dxftype: str, dxfattribs: dict) -> Iterable[str]:
        """Returns the source code strings to create a new table entity by
        ezdxf.

        Args:
            dxftype: table entry type as string, like 'LAYER'
            dxfattribs: DXF attributes dictionary

        """
        table = f"{self.doc}.{TABLENAMES[dxftype]}"
        dxfattribs = _purge_handles(dxfattribs)
        name = dxfattribs.pop("name")
        s = [
            f"if '{name}' not in {table}:",
            f"    t = {table}.new(",
            f"        '{name}',",
            "        dxfattribs={",
        ]
        s.extend(_fmt_mapping(dxfattribs, indent=12))
        s.extend(
            [
                "        },",
                "    )",
            ]
        )
        return s

    # simple graphical types

    def _line(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call("add_line(", ["start", "end"], entity.dxfattribs())
        )

    def _point(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call("add_point(", ["location"], entity.dxfattribs())
        )

    def _circle(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call(
                "add_circle(", ["center", "radius"], entity.dxfattribs()
            )
        )

    def _arc(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call(
                "add_arc(",
                ["center", "radius", "start_angle", "end_angle"],
                entity.dxfattribs(),
            )
        )

    def _text(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call("add_text(", ["text"], entity.dxfattribs())
        )

    def _solid(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.generic_api_call("SOLID", entity.dxfattribs())
        )

    def _trace(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.generic_api_call("TRACE", entity.dxfattribs())
        )

    def _3dface(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.generic_api_call("3DFACE", entity.dxfattribs())
        )

    def _shape(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call(
                "add_shape(", ["name", "insert", "size"], entity.dxfattribs()
            )
        )

    def _attrib(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call(
                "add_attrib(", ["tag", "text", "insert"], entity.dxfattribs()
            )
        )

    def _attdef(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.generic_api_call("ATTDEF", entity.dxfattribs())
        )

    def _ellipse(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.api_call(
                "add_ellipse(",
                ["center", "major_axis", "ratio", "start_param", "end_param"],
                entity.dxfattribs(),
            )
        )

    def _viewport(self, entity: DXFEntity) -> None:
        self.add_source_code_lines(
            self.generic_api_call("VIEWPORT", entity.dxfattribs())
        )
        self.add_source_code_line(
            '# Set valid handles or remove attributes ending with "_handle", '
            "otherwise the DXF file is invalid for AutoCAD"
        )

    # complex graphical types

    def _insert(self, entity: Insert) -> None:
        self.code.blocks.add(entity.dxf.name)
        self.add_source_code_lines(
            self.api_call(
                "add_blockref(", ["name", "insert"], entity.dxfattribs()
            )
        )
        if len(entity.attribs):
            for attrib in entity.attribs:
                dxfattribs = attrib.dxfattribs()
                dxfattribs[
                    "layer"
                ] = entity.dxf.layer  # set ATTRIB layer to same as INSERT
                self.add_source_code_lines(
                    self.generic_api_call(
                        "ATTRIB", attrib.dxfattribs(), prefix="a = "
                    )
                )
                self.add_source_code_line("e.attribs.append(a)")

    def _mtext(self, entity: MText) -> None:
        self.add_source_code_lines(
            self.generic_api_call("MTEXT", entity.dxfattribs())
        )
        # MTEXT content 'text' is not a single DXF tag and therefore not a DXF
        # attribute
        self.add_source_code_line("e.text = {}".format(json.dumps(entity.text)))

    def _lwpolyline(self, entity: LWPolyline) -> None:
        self.add_source_code_lines(
            self.generic_api_call("LWPOLYLINE", entity.dxfattribs())
        )
        # lwpolyline points are not DXF attributes
        self.add_list_source_code(
            entity.get_points(), prolog="e.set_points([", epilog="])"
        )

    def _spline(self, entity: Spline) -> None:
        self.add_source_code_lines(
            self.api_call("add_spline(", ["degree"], entity.dxfattribs())
        )
        # spline points, knots and weights are not DXF attributes
        if len(entity.fit_points):
            self.add_list_source_code(
                entity.fit_points, prolog="e.fit_points = [", epilog="]"
            )

        if len(entity.control_points):
            self.add_list_source_code(
                entity.control_points, prolog="e.control_points = [", epilog="]"
            )

        if len(entity.knots):
            self.add_list_source_code(
                entity.knots, prolog="e.knots = [", epilog="]"
            )

        if len(entity.weights):
            self.add_list_source_code(
                entity.weights, prolog="e.weights = [", epilog="]"
            )

    def _polyline(self, entity: Polyline) -> None:
        self.add_source_code_lines(
            self.generic_api_call("POLYLINE", entity.dxfattribs())
        )
        # polyline vertices are separate DXF entities and therefore not DXF attributes
        for v in entity.vertices:
            attribs = _purge_handles(v.dxfattribs())
            location = attribs.pop("location")
            if "layer" in attribs:
                del attribs[
                    "layer"
                ]  # layer is automatically set to the POLYLINE layer

            # each VERTEX can have different DXF attributes: bulge, start_width, end_width ...
            self.add_source_code_line(
                f"e.append_vertex({str(location)}, dxfattribs={attribs})"
            )

    def _leader(self, entity: Leader):
        self.add_source_code_line(
            "# Dimension style attribute overriding is not supported!"
        )
        self.add_source_code_lines(
            self.generic_api_call("LEADER", entity.dxfattribs())
        )
        self.add_list_source_code(
            entity.vertices, prolog="e.set_vertices([", epilog="])"
        )

    def _dimension(self, entity: Dimension):
        self.add_import_statement(
            "from ezdxf.dimstyleoverride import DimStyleOverride"
        )
        self.add_source_code_line(
            "# Dimension style attribute overriding is not supported!"
        )
        self.add_source_code_lines(
            self.generic_api_call("DIMENSION", entity.dxfattribs())
        )
        self.add_source_code_lines(
            [
                "# You have to create the required graphical representation for ",
                "# the DIMENSION entity as anonymous block, otherwise the DXF file",
                "# is invalid for AutoCAD (but not for BricsCAD):",
                "# DimStyleOverride(e).render()",
                "",
            ]
        )

    def _image(self, entity: Image):
        self.add_source_code_line(
            "# Image requires IMAGEDEF and IMAGEDEFREACTOR objects in the "
            "OBJECTS section!"
        )
        self.add_source_code_lines(
            self.generic_api_call("IMAGE", entity.dxfattribs())
        )
        if len(entity.boundary_path):
            self.add_list_source_code(
                (v for v in entity.boundary_path),  # just x, y axis
                prolog="e.set_boundary_path([",
                epilog="])",
            )
        self.add_source_code_line(
            "# Set valid image_def_handle and image_def_reactor_handle, "
            "otherwise the DXF file is invalid for AutoCAD"
        )

    def _wipeout(self, entity: Wipeout):
        self.add_source_code_lines(
            self.generic_api_call("WIPEOUT", entity.dxfattribs())
        )
        if len(entity.boundary_path):
            self.add_list_source_code(
                (v for v in entity.boundary_path),  # just x, y axis
                prolog="e.set_boundary_path([",
                epilog="])",
            )

    def _mesh(self, entity: Mesh):
        self.add_source_code_lines(
            self.api_call("add_mesh(", [], entity.dxfattribs())
        )
        if len(entity.vertices):
            self.add_list_source_code(
                entity.vertices, prolog="e.vertices = [", epilog="]"
            )
        if len(entity.edges):
            # array.array -> tuple
            self.add_list_source_code(
                (tuple(e) for e in entity.edges),
                prolog="e.edges = [",
                epilog="]",
            )
        if len(entity.faces):
            # array.array -> tuple
            self.add_list_source_code(
                (tuple(f) for f in entity.faces),
                prolog="e.faces = [",
                epilog="]",
            )
        if len(entity.creases):
            self.add_list_source_code(
                entity.creases, prolog="e.creases = [", epilog="]"
            )

    def _hatch(self, entity: Hatch):
        dxfattribs = entity.dxfattribs()
        dxfattribs["associative"] = 0  # associative hatch not supported
        self.add_source_code_lines(
            self.api_call("add_hatch(", ["color"], dxfattribs)
        )
        self._polygon(entity)

    def _mpolygon(self, entity: MPolygon):
        dxfattribs = entity.dxfattribs()
        self.add_source_code_lines(
            self.api_call("add_mpolygon(", ["color"], dxfattribs)
        )
        if entity.dxf.solid_fill:
            self.add_source_code_line(
                f"e.set_solid_fill(color={entity.dxf.fill_color})\n"
            )
        self._polygon(entity)

    def _polygon(self, entity: DXFPolygon):
        add_line = self.add_source_code_line
        if len(entity.seeds):
            add_line(f"e.set_seed_points({entity.seeds})")
        if entity.pattern:
            self.add_list_source_code(
                map(str, entity.pattern.lines),
                prolog="e.set_pattern_definition([",
                epilog="])",
            )
            self.add_source_code_line("e.dxf.solid_fill = 0")
        arg = "    {}={},"

        if entity.gradient is not None:
            g = entity.gradient
            add_line("e.set_gradient(")
            add_line(arg.format("color1", str(g.color1)))
            add_line(arg.format("color2", str(g.color2)))
            add_line(arg.format("rotation", g.rotation))
            add_line(arg.format("centered", g.centered))
            add_line(arg.format("one_color", g.one_color))
            add_line(arg.format("name", json.dumps(g.name)))
            add_line(")")
        for count, path in enumerate(entity.paths, start=1):
            if path.type == BoundaryPathType.POLYLINE:
                add_line("# {}. polyline path".format(count))
                self.add_list_source_code(
                    path.vertices,
                    prolog="e.paths.add_polyline_path([",
                    epilog="    ],",
                )
                add_line(arg.format("is_closed", str(path.is_closed)))
                add_line(arg.format("flags", str(path.path_type_flags)))
                add_line(")")
            else:  # EdgePath
                add_line(
                    f"# {count}. edge path: associative hatch not supported"
                )
                add_line(
                    f"ep = e.paths.add_edge_path(flags={path.path_type_flags})"
                )
                for edge in path.edges:
                    if edge.type == EdgeType.LINE:
                        add_line(f"ep.add_line({edge.start}, {str(edge.end)})")
                    elif edge.type == EdgeType.ARC:
                        # Start- and end angles are always stored in ccw
                        # orientation:
                        add_line("ep.add_arc(")
                        add_line(arg.format("center", str(edge.center)))
                        add_line(arg.format("radius", edge.radius))
                        add_line(arg.format("start_angle", edge.start_angle))
                        add_line(arg.format("end_angle", edge.end_angle))
                        add_line(arg.format("ccw", edge.ccw))
                        add_line(")")
                    elif edge.type == EdgeType.ELLIPSE:
                        # Start- and end params are always stored in ccw
                        # orientation:
                        add_line("ep.add_ellipse(")
                        add_line(arg.format("center", str(edge.center)))
                        add_line(arg.format("major_axis", str(edge.major_axis)))
                        add_line(arg.format("ratio", edge.ratio))
                        add_line(arg.format("start_angle", edge.start_angle))
                        add_line(arg.format("end_angle", edge.end_angle))
                        add_line(arg.format("ccw", edge.ccw))
                        add_line(")")
                    elif edge.type == EdgeType.SPLINE:
                        add_line("ep.add_spline(")
                        if edge.fit_points:
                            add_line(
                                arg.format(
                                    "fit_points",
                                    str([fp for fp in edge.fit_points]),
                                )
                            )
                        if edge.control_points:
                            add_line(
                                arg.format(
                                    "control_points",
                                    str([cp for cp in edge.control_points]),
                                )
                            )
                        if edge.knot_values:
                            add_line(
                                arg.format("knot_values", str(edge.knot_values))
                            )
                        if edge.weights:
                            add_line(arg.format("weights", str(edge.weights)))
                        add_line(arg.format("degree", edge.degree))
                        add_line(arg.format("periodic", edge.periodic))
                        if edge.start_tangent is not None:
                            add_line(
                                arg.format(
                                    "start_tangent", str(edge.start_tangent)
                                )
                            )
                        if edge.end_tangent is not None:
                            add_line(
                                arg.format("end_tangent", str(edge.end_tangent))
                            )
                        add_line(")")

    # simple table entries
    def _layer(self, layer: DXFEntity):
        self.add_source_code_lines(
            self.new_table_entry("LAYER", layer.dxfattribs())
        )

    def _ltype(self, ltype: Linetype):
        self.add_import_statement("from ezdxf.lldxf.tags import Tags")
        self.add_import_statement("from ezdxf.lldxf.types import dxftag")
        self.add_import_statement(
            "from ezdxf.entities.ltype import LinetypePattern"
        )
        self.add_source_code_lines(
            self.new_table_entry("LTYPE", ltype.dxfattribs())
        )
        self.add_tags_source_code(
            ltype.pattern_tags.tags,
            prolog="tags = Tags([",
            epilog="])",
            indent=4,
        )
        self.add_source_code_line("    t.pattern_tags = LinetypePattern(tags)")

    def _style(self, style: DXFEntity):
        self.add_source_code_lines(
            self.new_table_entry("STYLE", style.dxfattribs())
        )

    def _dimstyle(self, dimstyle: DXFEntity):
        self.add_source_code_lines(
            self.new_table_entry("DIMSTYLE", dimstyle.dxfattribs())
        )

    def _appid(self, appid: DXFEntity):
        self.add_source_code_lines(
            self.new_table_entry("APPID", appid.dxfattribs())
        )
