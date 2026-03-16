"""A library for reading and converting SVG files.

This module provides a converter from SVG to ReportLab Graphics (RLG) drawings.
It handles basic shapes, paths, and simple text elements.

The intended usage is either as a module within other projects or from the
command-line for converting SVG files to PDF. It also supports gzip-compressed
SVG files with the .svgz extension.

Example:
    To convert an SVG file to a ReportLab Drawing object::

        from svglib.svglib import svg2rlg
        drawing = svg2rlg("foo.svg")

    To convert an SVG file to a PDF from the command-line::

        $ python -m svglib foo.svg
"""

import base64
import copy
import gzip
import itertools
import logging
import os
import pathlib
import re
import shlex
import shutil
from collections import defaultdict, namedtuple
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Union

from PIL import Image as PILImage
from reportlab.graphics.shapes import (
    _CLOSEPATH,
    Circle,
    Drawing,
    Ellipse,
    Group,
    Image,
    Line,
    Path,
    Polygon,
    PolyLine,
    Rect,
    SolidShape,
    String,
)
from reportlab.lib import colors
from reportlab.lib.units import pica, toLength
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen.canvas import FILL_EVEN_ODD, FILL_NON_ZERO
from reportlab.pdfgen.pdfimages import PDFImage

try:
    from reportlab.graphics.transform import mmult
except ImportError:
    # Before Reportlab 3.5.61
    from reportlab.graphics.shapes import mmult

import cssselect2
import tinycss2
from lxml import etree

from .fonts import (
    DEFAULT_FONT_NAME,
    DEFAULT_FONT_SIZE,
    DEFAULT_FONT_STYLE,
    DEFAULT_FONT_WEIGHT,
    get_global_font_map,
)
from .fonts import (
    find_font as _fonts_find_font,
)

# To keep backward compatibility, since those functions where previously part of
# the svglib module
from .fonts import (
    register_font as _fonts_register_font,
)
from .utils import (
    bezier_arc_from_end_points,
    convert_quadratic_to_cubic_path,
    normalise_svg_path,
)


def _convert_palette_to_rgba(image: PILImage.Image) -> PILImage.Image:
    """Convert a palette-based image with transparency to RGBA format.

    This function checks if a PIL Image is in palette mode ('P') and has
    transparency information. If so, it converts the image to RGBA to prevent
    potential warnings or errors during processing.

    Args:
        image: The input PIL Image object.

    Returns:
        The converted RGBA PIL Image object if changes were made, otherwise
        the original image.
    """
    if image.mode == "P" and "transparency" in image.info:
        # Convert palette image with transparency to RGBA
        return image.convert("RGBA")
    return image


def register_font(
    font_name: str,
    font_path: Optional[str] = None,
    weight: str = "normal",
    style: str = "normal",
    rlgFontName: Optional[str] = None,
) -> Tuple[Optional[str], bool]:
    """Register a font for use in SVG processing.

    This function serves as a backward-compatible wrapper for the font
    registration logic defined in the `svglib.fonts` module.

    Args:
        font_name: The name of the font to register.
        font_path: The file path to the font file (optional).
        weight: The font weight (e.g., 'normal', 'bold').
        style: The font style (e.g., 'normal', 'italic').
        rlgFontName: The ReportLab-specific font name (optional).

    Returns:
        A tuple containing the registered font name and a boolean indicating
        if the registration was successful.
    """
    return _fonts_register_font(font_name, font_path, weight, style, rlgFontName)


def find_font(
    font_name: str, weight: str = "normal", style: str = "normal"
) -> Tuple[str, bool]:
    """Find a registered font by its properties.

    This function serves as a backward-compatible wrapper for the font
    finding logic defined in the `svglib.fonts` module.

    Args:
        font_name: The name of the font to find.
        weight: The font weight to match.
        style: The font style to match.

    Returns:
        A tuple containing the matched font name and a boolean indicating
        if an exact match was found.
    """
    return _fonts_find_font(font_name, weight, style)


XML_NS = "http://www.w3.org/XML/1998/namespace"

# A sentinel to identify a situation where a node reference a fragment not yet defined.
DELAYED = object()

logger = logging.getLogger(__name__)

Box = namedtuple("Box", ["x", "y", "width", "height"])

split_whitespace = re.compile(r"[^ \t\r\n\f]+").findall


class NoStrokePath(Path):
    """A Path object that never has a stroke width.

    This class is used to create filled shapes from unclosed paths, where
    only the fill should be rendered and the stroke should be ignored.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        copy_from = kwargs.pop("copy_from", None)
        super().__init__(*args, **kwargs)
        if copy_from:
            self.__dict__.update(copy.deepcopy(copy_from.__dict__))

    def getProperties(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Return the properties of the path, ensuring no stroke is applied."""
        props = super().getProperties(*args, **kwargs)
        if "strokeWidth" in props:
            props["strokeWidth"] = 0
        if "strokeColor" in props:
            props["strokeColor"] = None
        return props


class ClippingPath(Path):
    """A Path object used for defining a clipping region.

    This path will not be rendered with a fill or stroke but will be used
    as a clipping mask for other shapes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        copy_from = kwargs.pop("copy_from", None)
        Path.__init__(self, *args, **kwargs)
        if copy_from:
            self.__dict__.update(copy.deepcopy(copy_from.__dict__))
        self.isClipPath = 1

    def getProperties(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """Return the properties of the path, ensuring no fill or stroke."""
        props = Path.getProperties(self, *args, **kwargs)
        if "fillColor" in props:
            props["fillColor"] = None
        if "strokeColor" in props:
            props["strokeColor"] = None
        return props


class CSSMatcher(cssselect2.Matcher):
    """A CSS matcher to handle styles defined in SVG <style> elements."""

    def add_styles(self, style_content: str) -> None:
        """Parse a string of CSS rules and add them to the matcher.

        Args:
            style_content: A string containing CSS rules.
        """
        rules = tinycss2.parse_stylesheet(
            style_content, skip_comments=True, skip_whitespace=True
        )

        for rule in rules:
            if not rule.prelude or rule.type == "at-rule":
                continue
            selectors = cssselect2.compile_selector_list(rule.prelude)
            selector_string = tinycss2.serialize(rule.prelude)
            content_dict = {
                attr.split(":")[0].strip(): attr.split(":")[1].strip()
                for attr in tinycss2.serialize(rule.content).split(";")
                if ":" in attr
            }
            payload = (selector_string, content_dict)
            for selector in selectors:
                self.add_selector(selector, payload)


# Attribute converters (from SVG to RLG)


class AttributeConverter:
    """An abstract class for converting SVG attributes to ReportLab properties."""

    def __init__(self) -> None:
        self.css_rules: Optional[CSSMatcher] = None
        self.main_box: Optional[Box] = None

    def set_box(self, main_box: Optional[Box]) -> None:
        """Set the main viewbox for resolving percentage-based units.

        Args:
            main_box: A Box tuple representing the main viewbox.
        """
        self.main_box = main_box

    def parseMultiAttributes(self, line: str) -> Dict[str, str]:
        """Parse a compound attribute string into a dictionary.

        Args:
            line: A string of semicolon-separated style attributes.

        Returns:
            A dictionary of attribute key-value pairs.
        """
        attrs = line.split(";")
        attrs = [a.strip() for a in attrs]
        attrs = [a for a in attrs if len(a) > 0]

        new_attrs = {}
        for a in attrs:
            k, v = a.split(":")
            k, v = (s.strip() for s in (k, v))
            new_attrs[k] = v

        return new_attrs

    def findAttr(self, svgNode: Any, name: str) -> str:
        """Find an attribute value, searching the node and its ancestors.

        The search order is:
        1. The node's own attributes.
        2. The node's 'style' attribute.
        3. The node's parent, recursively.

        Args:
            svgNode: The lxml node to start the search from.
            name: The name of the attribute to find.

        Returns:
            The attribute value, or an empty string if not found.
        """
        if not svgNode.attrib.get("__rules_applied", False):
            # Apply global styles...
            if self.css_rules is not None:
                svgNode.apply_rules(self.css_rules)
            # ...and locally defined
            if svgNode.attrib.get("style"):
                attrs = self.parseMultiAttributes(svgNode.attrib.get("style"))
                for key, val in attrs.items():
                    # lxml nodes cannot accept attributes starting with '-'
                    if not key.startswith("-"):
                        svgNode.attrib[key] = val
                svgNode.attrib["__rules_applied"] = "1"

        attr_value = svgNode.attrib.get(name, "").strip()

        if attr_value and attr_value != "inherit":
            return attr_value
        if svgNode.parent is not None:
            return self.findAttr(svgNode.parent, name)
        return ""

    def getAllAttributes(self, svgNode: Any) -> Dict[str, str]:
        """Return a dictionary of all attributes of a node and its ancestors.

        Args:
            svgNode: The lxml node to get attributes from.

        Returns:
            A dictionary of all applicable attributes.
        """
        dict = {}

        if node_name(svgNode.getparent()) == "g":
            dict.update(self.getAllAttributes(svgNode.getparent()))

        style = svgNode.attrib.get("style")
        if style:
            d = self.parseMultiAttributes(style)
            dict.update(d)

        for key, value in svgNode.attrib.items():
            if key != "style":
                dict[key] = value

        return dict

    def id(self, svgAttr: str) -> str:
        """Return the attribute value as is."""
        return svgAttr

    def convertTransform(
        self,
        svgAttr: str,
    ) -> List[Tuple[str, Union[float, Tuple[float, ...]]]]:
        """Parse a transform attribute string into a list of operations.

        Args:
            svgAttr: The SVG transform attribute string.

        Returns:
            A list of tuples, where each tuple contains the transform
            operation and its arguments.

        Example:
            >>> converter = AttributeConverter()
            >>> converter.convertTransform("scale(2) translate(10,20)")
            [('scale', 2.0), ('translate', (10.0, 20.0))]
        """
        line = svgAttr.strip()

        ops: str = line[:]
        brackets: List[int] = []
        indices: List[Union[float, Tuple[float, ...]]] = []
        for i, lin in enumerate(line):
            if lin in "()":
                brackets.append(i)
        for i in range(0, len(brackets), 2):
            bi, bj = brackets[i], brackets[i + 1]
            subline = line[bi + 1 : bj]
            subline = subline.strip()
            subline = subline.replace(",", " ")
            subline = re.sub("[ ]+", ",", subline)
            try:
                if "," in subline:
                    indices.append(tuple(float(num) for num in subline.split(",")))
                else:
                    indices.append(float(subline))
            except ValueError:
                continue
            ops = ops[:bi] + " " * (bj - bi + 1) + ops[bj + 1 :]
        ops_list: List[str] = ops.replace(",", " ").split()

        if len(ops_list) != len(indices):
            logger.warning("Unable to parse transform expression %r", svgAttr)
            return []

        result = []
        for i, op in enumerate(ops_list):
            result.append((op, indices[i]))

        return result


class Svg2RlgAttributeConverter(AttributeConverter):
    """A concrete attribute converter for SVG to ReportLab Graphics."""

    def __init__(
        self,
        color_converter: Optional[Any] = None,
        font_map: Optional[Any] = None,
    ) -> None:
        super().__init__()
        self.color_converter = color_converter or self.identity_color_converter
        self._font_map = font_map or get_global_font_map()

    @staticmethod
    def identity_color_converter(c: Any) -> Any:
        """A default color converter that returns the color as is."""
        return c

    @staticmethod
    def split_attr_list(attr: str) -> List[str]:
        """Split a string of attributes into a list."""
        return shlex.split(attr.strip().replace(",", " "))

    def convertLength(
        self,
        svgAttr: str,
        em_base: float = DEFAULT_FONT_SIZE,
        attr_name: Optional[str] = None,
        default: float = 0.0,
    ) -> Union[float, List[float]]:
        """Convert an SVG length string to points.

        Args:
            svgAttr: The SVG length string (e.g., "10px", "5em").
            em_base: The base font size for 'em' units.
            attr_name: The name of the attribute being converted.
            default: The default value to return if the string is empty.

        Returns:
            The length in points as a float, or a list of floats for
            space-separated values.
        """
        text = svgAttr.replace(",", " ").strip()
        if not text:
            return default
        if " " in text:
            # Multiple length values, returning a list
            items = [
                self.convertLength(
                    val, em_base=em_base, attr_name=attr_name, default=default
                )
                for val in self.split_attr_list(text)
            ]
            result: List[float] = []
            for item in items:
                if isinstance(item, list):
                    result.extend(item)
                else:
                    result.append(item)
            return result

        if text.endswith("%"):
            if self.main_box is None:
                logger.error("Unable to resolve percentage unit without a main box")
                return float(text[:-1])
            if attr_name is None:
                logger.error(
                    "Unable to resolve percentage unit without knowing the node name"
                )
                return float(text[:-1])
            if attr_name in ("x", "cx", "x1", "x2", "width"):
                full = self.main_box.width
            elif attr_name in ("y", "cy", "y1", "y2", "height"):
                full = self.main_box.height
            else:
                logger.error(
                    "Unable to detect if node %r is width or height", attr_name
                )
                return float(text[:-1])
            return float(text[:-1]) / 100 * full
        elif text.endswith("pc"):
            return float(text[:-2]) * pica
        elif text.endswith("pt"):
            return float(text[:-2])
        elif text.endswith("em"):
            return float(text[:-2]) * em_base
        elif text.endswith("px"):
            return float(text[:-2]) * 0.75
        elif text.endswith("ex"):
            # The x-height of the text must be assumed to be 0.5em tall when the
            # text cannot be measured.
            return float(text[:-2]) * em_base / 2
        elif text.endswith("ch"):
            # The advance measure of the "0" glyph must be assumed to be 0.5em
            # wide when the text cannot be measured.
            return float(text[:-2]) * em_base / 2

        text = text.strip()
        length = toLength(text)  # this does the default measurements such as mm and cm

        return length

    def convertLengthList(self, svgAttr: str) -> List[Union[float, List[float]]]:
        """Convert a space-separated list of lengths into a list of floats."""
        return [self.convertLength(a) for a in self.split_attr_list(svgAttr)]

    def convertOpacity(self, svgAttr: str) -> float:
        """Convert an opacity string to a float."""
        return float(svgAttr)

    def convertFillRule(self, svgAttr: str) -> Union[int, str]:
        """Convert an SVG fill-rule string to a ReportLab fill rule."""
        return {
            "nonzero": FILL_NON_ZERO,
            "evenodd": FILL_EVEN_ODD,
        }.get(svgAttr, "")

    def convertColor(self, svgAttr: str) -> Any:
        """Convert an SVG color string to a ReportLab color object.

        Args:
            svgAttr: The SVG color string (e.g., "#FF0000", "blue").

        Returns:
            A ReportLab color object, or None if the color is invalid.
        """
        text = svgAttr
        if not text or text == "none":
            return None

        if text == "currentColor":
            return "currentColor"
        if len(text) in (7, 9) and text[0] == "#":
            color = colors.HexColor(text, hasAlpha=len(text) == 9)
        elif len(text) == 4 and text[0] == "#":
            color = colors.HexColor("#" + 2 * text[1] + 2 * text[2] + 2 * text[3])
        elif len(text) == 5 and text[0] == "#":
            color = colors.HexColor(
                "#" + 2 * text[1] + 2 * text[2] + 2 * text[3] + 2 * text[4],
                hasAlpha=True,
            )
        else:
            # Should handle pcmyk|cmyk|rgb|hsl values (including 'a' for alpha)
            color = colors.cssParse(text)
            if color is None:
                # Test if text is a predefined color constant
                try:
                    color = getattr(colors, text).clone()
                except AttributeError:
                    pass
        if color is None:
            logger.warning("Can't handle color: %s", text)
        else:
            return self.color_converter(color)
        return None

    def convertLineJoin(self, svgAttr: str) -> int:
        """Convert an SVG stroke-linejoin string to a ReportLab line join."""
        return {"miter": 0, "round": 1, "bevel": 2}[svgAttr]

    def convertLineCap(self, svgAttr: str) -> int:
        """Convert an SVG stroke-linecap string to a ReportLab line cap."""
        return {"butt": 0, "round": 1, "square": 2}[svgAttr]

    def convertDashArray(self, svgAttr: str) -> List[Union[float, List[float]]]:
        """Convert an SVG stroke-dasharray string to a list of lengths."""
        strokeDashArray = self.convertLengthList(svgAttr)
        return strokeDashArray

    def convertDashOffset(self, svgAttr: str) -> Union[float, List[float]]:
        """Convert an SVG stroke-dashoffset string to a length."""
        strokeDashOffset = self.convertLength(svgAttr)
        return strokeDashOffset

    def convertFontFamily(
        self,
        fontAttr: Optional[str],
        weightAttr: str = "normal",
        styleAttr: str = "normal",
    ) -> str:
        """Convert an SVG font-family string to a registered font name.

        Args:
            fontAttr: The SVG font-family attribute string.
            weightAttr: The font-weight attribute string.
            styleAttr: The font-style attribute string.

        Returns:
            The best-matched registered font name.
        """
        if not fontAttr:
            return ""
        # split the fontAttr in actual font family names
        font_names = self.split_attr_list(fontAttr)

        non_exact_matches = []
        for font_name in font_names:
            font_name, exact = self._font_map.find_font(
                font_name, weightAttr, styleAttr
            )
            if exact:
                return font_name
            elif font_name:
                non_exact_matches.append(font_name)
        if non_exact_matches:
            return non_exact_matches[0]
        else:
            logger.warning(
                f"Unable to find a suitable font for 'font-family:{fontAttr}', "
                f"weight:{weightAttr}, style:{styleAttr}"
            )
            return DEFAULT_FONT_NAME


class NodeTracker(cssselect2.ElementWrapper):
    """A wrapper for lxml nodes to track attribute usage.

    This class wraps an lxml node and keeps a record of which attributes
    have been accessed, which is useful for debugging unused attributes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.usedAttrs: List[str] = []

    def __repr__(self) -> str:
        return f"<NodeTracker for node {self.etree_element}>"

    def getAttribute(self, name: str) -> str:
        """Get an attribute value and record that it has been used."""
        if name not in self.usedAttrs:
            self.usedAttrs.append(name)
        return self.etree_element.attrib.get(name, "")

    def __getattr__(self, name: str) -> Any:
        """Forward attribute access to the wrapped lxml node."""
        return getattr(self.etree_element, name)

    def apply_rules(self, rules: Any) -> None:
        """Apply CSS rules to the wrapped node.

        Args:
            rules: A CSSMatcher object containing the styles to apply.
        """
        matches = rules.match(self)
        for match in matches:
            attr_dict = match[3][1]
            for attr, val in attr_dict.items():
                try:
                    self.etree_element.attrib[attr] = val
                except ValueError:
                    pass
        # Set marker on the node to not apply rules more than once
        self.etree_element.set("__rules_applied", "1")


class CircularRefError(Exception):
    """Exception raised for circular references in SVG files."""

    pass


class ExternalSVG:
    """A class to handle external SVG files referenced via xlink:href."""

    def __init__(
        self,
        path: Union[str, os.PathLike[str]],
        renderer: "SvgRenderer",
    ) -> None:
        self.root_node = load_svg_file(path)
        self.renderer = SvgRenderer(
            path, parent_svgs=renderer._parent_chain + [str(renderer.source_path)]
        )
        self.rendered = False

    def get_fragment(self, fragment: str) -> Any:
        """Get a defined fragment from the external SVG file.

        Args:
            fragment: The ID of the fragment to retrieve.

        Returns:
            The rendered fragment, or None if not found.
        """
        if not self.rendered:
            self.renderer.render(self.root_node)
            self.rendered = True
        return self.renderer.definitions.get(fragment)


# ## the main meat ###


class SvgRenderer:
    """A class to render an SVG file into a ReportLab Drawing.

    This class walks the SVG DOM and converts SVG elements into their
    corresponding ReportLab Graphics objects.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike[str]],
        color_converter: Optional[Any] = None,
        parent_svgs: Optional[List[str]] = None,
        font_map: Optional[Any] = None,
    ) -> None:
        self.source_path: Union[str, os.PathLike[str]] = path
        self._parent_chain: List[str] = parent_svgs or []  # To detect circular refs.
        self.attrConverter = Svg2RlgAttributeConverter(
            color_converter=color_converter, font_map=font_map
        )
        self.shape_converter = Svg2RlgShapeConverter(path, self.attrConverter)
        self.handled_shapes = self.shape_converter.get_handled_shapes()
        self.definitions: Dict[str, Any] = {}
        self.waiting_use_nodes: Dict[str, List[Tuple[NodeTracker, Optional[Any]]]] = (
            defaultdict(list)
        )
        self._external_svgs: Dict[str, ExternalSVG] = {}
        self.attrConverter.css_rules = CSSMatcher()

    def render(self, svg_node: Any) -> Drawing:
        """Render an SVG node into a ReportLab Drawing.

        Args:
            svg_node: The root lxml node of the SVG document.

        Returns:
            A ReportLab Drawing object representing the SVG.
        """
        node = NodeTracker.from_xml_root(svg_node)
        view_box = self.get_box(node, default_box=True)
        # Knowing the main box is useful for percentage units
        self.attrConverter.set_box(view_box)

        main_group = self.renderSvg(node, outermost=True)
        for xlink in self.waiting_use_nodes.keys():
            logger.debug("Ignoring unavailable object width ID %r.", xlink)

        main_group.translate(0 - view_box.x, -view_box.height - view_box.y)

        width, height = self.shape_converter.convert_length_attrs(
            svg_node, "width", "height", defaults=(view_box.width, view_box.height)
        )
        drawing = Drawing(width, height)
        drawing.add(main_group)
        return drawing

    def renderNode(self, node: NodeTracker, parent: Optional[Any] = None) -> None:
        """Render a single SVG node and add it to a parent group.

        Args:
            node: The NodeTracker object for the SVG node.
            parent: The parent ReportLab Group to add the rendered object to.
        """
        if parent is None:
            return
        nid = node.getAttribute("id")
        ignored = False
        item = None
        name = node_name(node)

        clipping = self.get_clippath(node)
        if name == "svg":
            item = self.renderSvg(node)
            parent.add(item)
        elif name == "defs":
            ignored = True  # defs are handled in the initial rendering phase.
        elif name == "a":
            item = self.renderA(node)
            parent.add(item)
        elif name == "g":
            display = node.getAttribute("display")
            item = self.renderG(node, clipping=clipping)
            if display != "none":
                parent.add(item)
        elif name == "style":
            self.renderStyle(node)
        elif name == "symbol":
            item = self.renderSymbol(node)
            # First time the symbol node is rendered, it should not be part of a group.
            # It is only rendered to be part of definitions.
            if node.attrib.get("_rendered"):
                parent.add(item)
            else:
                node.set("_rendered", "1")
        elif name == "use":
            item = self.renderUse(node, clipping=clipping)
            parent.add(item)
        elif name == "clipPath":
            item = self.renderG(node)
        elif name in self.handled_shapes:
            if name == "image":
                # We resolve the image target at renderer level because it can point
                # to another SVG file or node which has to be rendered too.
                target = self.xlink_href_target(node)
                if target is None:
                    return
                elif isinstance(target, tuple):
                    # This is SVG content needed to be rendered
                    gr = Group()
                    renderer, img_node = target
                    renderer.renderNode(img_node, parent=gr)
                    self.apply_node_attr_to_group(node, gr)
                    parent.add(gr)
                    return
                else:
                    # Attaching target to node, so we can get it back in convertImage
                    node._resolved_target = target

            item = self.shape_converter.convertShape(name, node, clipping)
            display = node.getAttribute("display")
            if item and display != "none":
                parent.add(item)
        else:
            ignored = True
            logger.debug("Ignoring node: %s", name)

        if not ignored:
            if nid and item:
                self.definitions[nid] = node
                # preserve id to keep track of svg objects
                # and simplify further analyses of generated document
                item.setProperties({"svgid": nid})
                # labels are used in inkscape to name specific groups as layers
                # preserving them simplify extraction of feature from the generated
                # document
                label_attrs = [v for k, v in node.attrib.items() if "label" in k]
                if len(label_attrs) == 1:
                    (label,) = label_attrs
                    item.setProperties({"label": label})
            if nid in self.waiting_use_nodes.keys():
                to_render = self.waiting_use_nodes.pop(nid)
                for use_node, group in to_render:
                    self.renderUse(use_node, group=group)
            self.print_unused_attributes(node)

    def get_clippath(self, node: NodeTracker) -> Optional[Any]:
        """Get the clipping path object referenced by a node's 'clip-path' attribute.

        Args:
            node: The NodeTracker object for the SVG node.

        Returns:
            A ClippingPath object, or None if no valid clipping path is found.
        """

        def get_shape_from_group(group: Any) -> Optional[Any]:
            for elem in group.contents:
                if isinstance(elem, Group):
                    return get_shape_from_group(elem)
                elif isinstance(elem, SolidShape):
                    return elem
            return None

        def get_shape_from_node(node: Any) -> Optional[Any]:
            for child in node.iter_children():
                if node_name(child) == "path":
                    group = self.shape_converter.convertShape("path", child)
                    return group.contents[-1]
                elif node_name(child) == "use":
                    grp = self.renderUse(child)
                    return get_shape_from_group(grp)
                elif node_name(child) == "rect":
                    return self.shape_converter.convertRect(child)
                else:
                    return get_shape_from_node(child)
            return None

        clip_path = node.getAttribute("clip-path")
        if not clip_path:
            return None
        m = re.match(r"url\(#([^)]*)\)", clip_path)
        if not m:
            return None
        ref = m.groups()[0]
        if ref not in self.definitions:
            logger.warning("Unable to find a clipping path with id %s", ref)
            return None

        shape = get_shape_from_node(self.definitions[ref])
        if isinstance(shape, Rect):
            # It is possible to use a rect as a clipping path in an svg, so we
            # need to convert it to a path for rlg.
            x1, y1, x2, y2 = shape.getBounds()
            cp = ClippingPath()
            cp.moveTo(x1, y1)
            cp.lineTo(x2, y1)
            cp.lineTo(x2, y2)
            cp.lineTo(x1, y2)
            cp.closePath()
            # Copy the styles from the rect to the clipping path.
            copy_shape_properties(shape, cp)
            return cp
        elif isinstance(shape, Path):
            return ClippingPath(copy_from=shape)
        elif shape:
            logger.error(
                "Unsupported shape type %s for clipping", shape.__class__.__name__
            )
        return None

    def print_unused_attributes(self, node: NodeTracker) -> None:
        """Print any attributes that were not used during rendering.

        This is a debugging helper to identify unsupported SVG attributes.

        Args:
            node: The NodeTracker object for the SVG node.
        """
        if logger.level > logging.DEBUG:
            return
        all_attrs = self.attrConverter.getAllAttributes(node.etree_element).keys()
        unused_attrs = [attr for attr in all_attrs if attr not in node.usedAttrs]
        if unused_attrs:
            logger.debug("Unused attrs: %s %s", node_name(node), unused_attrs)

    def apply_node_attr_to_group(self, node: NodeTracker, group: Any) -> None:
        """Apply common attributes (transform, x, y) from a node to a group.

        Args:
            node: The NodeTracker object for the SVG node.
            group: The ReportLab Group to apply the attributes to.
        """
        getAttr = node.getAttribute
        transform, x, y = map(getAttr, ("transform", "x", "y"))
        if x or y:
            transform += f" translate({x or 0}, {y or 0})"
        if transform:
            self.shape_converter.applyTransformOnGroup(transform, group)

    def xlink_href_target(self, node: NodeTracker, group: Optional[Any] = None) -> Any:
        """Resolve an xlink:href attribute to its target.

        The target can be an internal fragment, an external SVG file, or a
        raster image.

        Args:
            node: The NodeTracker object for the SVG node with the href attribute.
            group: The parent group, used for delayed rendering.

        Returns:
            - A tuple (renderer, node) for vector targets.
            - A PIL Image object for raster images.
            - None if the target cannot be resolved.
        """
        # Bare 'href' was introduced in SVG 2.
        xlink_href = node.attrib.get(
            "{http://www.w3.org/1999/xlink}href"
        ) or node.attrib.get("href")
        if not xlink_href:
            return None

        # First handle any raster embedded image data
        match = re.match(r"^data:image/(jpe?g|png);base64", xlink_href)
        if match:
            image_data = base64.decodebytes(
                xlink_href[(match.span(0)[1] + 1) :].encode("ascii")
            )
            bytes_stream = BytesIO(image_data)

            return _convert_palette_to_rgba(PILImage.open(bytes_stream))

        # From here, we can assume this is a path.
        if "#" in xlink_href:
            iri, fragment = xlink_href.split("#", 1)
        else:
            iri, fragment = xlink_href, None

        if iri:
            # Only local relative paths are supported yet
            if not isinstance(self.source_path, str):
                logger.error(
                    "Unable to resolve image path %r as the SVG source is not "
                    "a file system path.",
                    iri,
                )
                return None
            path = os.path.normpath(
                os.path.join(os.path.dirname(self.source_path), iri)
            )
            if not os.access(path, os.R_OK):
                return None
            if path == self.source_path:
                # Self-referencing, ignore the IRI part
                iri = None

        if iri:
            if path.endswith(".svg"):
                if path in self._parent_chain:
                    logger.error("Circular reference detected in file.")
                    raise CircularRefError()
                if path not in self._external_svgs:
                    self._external_svgs[path] = ExternalSVG(path, self)
                ext_svg = self._external_svgs[path]
                if ext_svg.root_node is not None:
                    if fragment:
                        ext_frag = ext_svg.get_fragment(fragment)
                        if ext_frag is not None:
                            return ext_svg.renderer, ext_frag
                    else:
                        return ext_svg.renderer, NodeTracker.from_xml_root(
                            ext_svg.root_node
                        )
            else:
                # A raster image path
                try:
                    # This will catch invalid images
                    PDFImage(path, 0, 0)
                except OSError:
                    logger.error("Unable to read the image %s. Skipping...", path)
                    return None
                return path

        elif fragment:
            # A pointer to an internal definition
            if fragment in self.definitions:
                return self, self.definitions[fragment]
            else:
                # The missing definition should appear later in the file
                self.waiting_use_nodes[fragment].append((node, group))
                return DELAYED
            return None

    def renderTitle_(self, node: NodeTracker) -> None:
        """Handle the <title> element (currently a no-op)."""
        pass

    def renderDesc_(self, node: NodeTracker) -> None:
        """Handle the <desc> element (currently a no-op)."""
        pass

    def get_box(self, svg_node: NodeTracker, default_box: bool = False) -> Box:
        """Get the viewBox or dimensions of an SVG node.

        Args:
            svg_node: The NodeTracker for the SVG node.
            default_box: If True, use width/height as a fallback.

        Returns:
            A Box tuple representing the dimensions.
        """
        view_box = svg_node.getAttribute("viewBox")
        if view_box:
            view_box = self.attrConverter.convertLengthList(view_box)  # type: ignore
            return Box(*view_box)
        if default_box:
            width, height = map(svg_node.getAttribute, ("width", "height"))
            width, height = map(self.attrConverter.convertLength, (width, height))  # type: ignore
            return Box(0, 0, width, height)
        return Box(0, 0, 0, 0)  # fallback

    def renderSvg(self, node: NodeTracker, outermost: bool = False) -> Any:
        """Render an <svg> element into a ReportLab Group.

        Args:
            node: The NodeTracker for the <svg> element.
            outermost: True if this is the root <svg> element.

        Returns:
            A ReportLab Group containing the rendered content.
        """
        _saved_preserve_space = self.shape_converter.preserve_space
        self.shape_converter.preserve_space = (
            node.getAttribute(f"{{{XML_NS}}}space") == "preserve"
        )
        view_box = self.get_box(node, default_box=True)
        _saved_box = self.attrConverter.main_box
        if view_box:
            self.attrConverter.set_box(view_box)

        # Rendering all definition nodes first.
        svg_ns = node.nsmap.get(None)
        for def_node in node.iter_subtree():
            if def_node.tag == (f"{{{svg_ns}}}defs" if svg_ns else "defs"):
                self.renderG(def_node)

        group = Group()
        for child in node.iter_children():
            self.renderNode(child, group)
        self.shape_converter.preserve_space = _saved_preserve_space
        self.attrConverter.set_box(_saved_box)

        # Translating
        if not outermost:
            x, y = self.shape_converter.convert_length_attrs(node, "x", "y")
            if x or y:
                group.translate(x or 0, y or 0)

        # Scaling
        if not view_box and outermost:
            # Apply only the 'reverse' y-scaling (PDF 0,0 is bottom left)
            group.scale(1, -1)
        elif view_box:
            x_scale, y_scale = 1, 1
            width, height = self.shape_converter.convert_length_attrs(
                node, "width", "height", defaults=(None,) * 2
            )
            if height is not None and view_box.height != height:
                y_scale = height / view_box.height
            if width is not None and view_box.width != width:
                x_scale = width / view_box.width
            group.scale(x_scale, y_scale * (-1 if outermost else 1))

        return group

    def renderG(self, node: NodeTracker, clipping: Optional[Any] = None) -> Any:
        """Render a <g> element into a ReportLab Group.

        Args:
            node: The NodeTracker for the <g> element.
            clipping: An optional clipping path to apply.

        Returns:
            A ReportLab Group containing the rendered content.
        """
        getAttr = node.getAttribute
        id, transform = map(getAttr, ("id", "transform"))
        gr = Group()
        if clipping:
            gr.add(clipping)
        for child in node.iter_children():
            self.renderNode(child, parent=gr)

        if transform:
            self.shape_converter.applyTransformOnGroup(transform, gr)

        return gr

    def renderStyle(self, node: NodeTracker) -> None:
        """Render a <style> element by adding its content to the CSS matcher."""
        if self.attrConverter.css_rules is not None:
            self.attrConverter.css_rules.add_styles(node.text or "")

    def renderSymbol(self, node: NodeTracker) -> Any:
        """Render a <symbol> element as a ReportLab Group."""
        return self.renderG(node)

    def renderA(self, node: NodeTracker) -> Any:
        """Render an <a> element as a ReportLab Group (no linking support)."""
        return self.renderG(node)

    def renderUse(
        self,
        node: NodeTracker,
        group: Optional[Any] = None,
        clipping: Optional[Any] = None,
    ) -> Any:
        """Render a <use> element by cloning a defined element.

        Args:
            node: The NodeTracker for the <use> element.
            group: The parent group to render into.
            clipping: An optional clipping path to apply.

        Returns:
            A ReportLab Group containing the rendered content.
        """
        if group is None:
            group = Group()

        try:
            item = self.xlink_href_target(node, group=group)
        except CircularRefError:
            node.parent.etree_element.remove(node.etree_element)
            return group
        if item is None:
            return
        elif isinstance(item, str):
            logger.error("<use> nodes cannot reference bitmap image files")
            return
        elif item is DELAYED:
            return group
        else:
            item = item[1]  # [0] is the renderer, not used here.

        if clipping:
            group.add(clipping)
        if len(node.getchildren()) == 0:
            # Append a copy of the referenced node as the <use> child (if not
            # already done)
            node.append(copy.deepcopy(item))
        self.renderNode(list(node.iter_children())[-1], parent=group)
        self.apply_node_attr_to_group(node, group)
        return group
        return None


class SvgShapeConverter:
    """An abstract class for converting SVG shapes to another format.

    Subclasses should implement `convertX` methods for each SVG shape `X`
    (e.g., `convertRect`, `convertCircle`).
    """

    def __init__(
        self,
        path: Union[str, os.PathLike[str]],
        attrConverter: Optional[Svg2RlgAttributeConverter] = None,
    ) -> None:
        self.attrConverter = attrConverter or Svg2RlgAttributeConverter()
        self.svg_source_file = path
        self.preserve_space = False

    @classmethod
    def get_handled_shapes(cls) -> List[str]:
        """Return a list of SVG shape names that this converter can handle."""
        return [key[7:].lower() for key in dir(cls) if key.startswith("convert")]


class Svg2RlgShapeConverter(SvgShapeConverter):
    """A class for converting SVG shapes to ReportLab Graphics shapes."""

    def convertShape(self, name: str, node: Any, clipping: Optional[Any] = None) -> Any:
        """Convert an SVG shape by calling the appropriate `convertX` method.

        Args:
            name: The name of the SVG shape (e.g., "rect", "circle").
            node: The lxml node for the shape.
            clipping: An optional clipping path to apply.

        Returns:
            A ReportLab shape object, or a Group if transforms or clipping
            are applied.
        """
        method_name = f"convert{name.capitalize()}"
        shape = getattr(self, method_name)(node)
        if not shape:
            return
        if name not in ("path", "polyline", "text"):
            # Only apply style where the convert method did not apply it.
            self.applyStyleOnShape(shape, node)
        transform = node.getAttribute("transform")
        if not (transform or clipping):
            return shape
        else:
            group = Group()
            if transform:
                self.applyTransformOnGroup(transform, group)
            if clipping:
                group.add(clipping)
            group.add(shape)
            return group

    def convert_length_attrs(
        self,
        node: Any,
        *attrs: str,
        em_base: float = DEFAULT_FONT_SIZE,
        **kwargs: Any,
    ) -> List[float]:
        """Convert a list of length attributes from a node.

        Args:
            node: The lxml node.
            *attrs: The names of the attributes to convert.
            em_base: The base font size for 'em' units.
            **kwargs: Can include 'defaults' for fallback values.

        Returns:
            A list of converted lengths in points.
        """
        getAttr = (
            node.getAttribute
            if hasattr(node, "getAttribute")
            else lambda attr: node.attrib.get(attr, "")
        )
        convLength = self.attrConverter.convertLength
        defaults = kwargs.get("defaults", (0.0,) * len(attrs))
        return [
            convLength(getAttr(attr), attr_name=attr, em_base=em_base, default=default)  # type: ignore
            for attr, default in zip(attrs, defaults)
        ]

    def convertLine(self, node: Any) -> Line:
        """Convert an SVG <line> element to a ReportLab Line."""
        points = self.convert_length_attrs(node, "x1", "y1", "x2", "y2")
        nudge_points(points)
        return Line(*points)

    def convertRect(self, node: Any) -> Optional[Rect]:
        """Convert an SVG <rect> element to a ReportLab Rect."""
        x, y, width, height, rx, ry = self.convert_length_attrs(
            node, "x", "y", "width", "height", "rx", "ry"
        )
        if rx > (width / 2):
            rx = width / 2
        if ry > (height / 2):
            ry = height / 2
        if rx and not ry:
            ry = rx
        elif ry and not rx:
            rx = ry
        return Rect(x, y, width, height, rx=rx, ry=ry)

    def convertCircle(self, node: Any) -> Circle:
        """Convert an SVG <circle> element to a ReportLab Circle."""
        cx, cy, r = self.convert_length_attrs(node, "cx", "cy", "r")
        return Circle(cx, cy, r)

    def convertEllipse(self, node: Any) -> Ellipse:
        """Convert an SVG <ellipse> element to a ReportLab Ellipse."""
        cx, cy, rx, ry = self.convert_length_attrs(node, "cx", "cy", "rx", "ry")
        width, height = rx, ry
        return Ellipse(cx, cy, width, height)

    def convertPolyline(self, node: Any) -> Optional[Any]:
        """Convert an SVG <polyline> element to a ReportLab PolyLine."""
        points = node.getAttribute("points")
        points = points.replace(",", " ")
        points = points.split()
        points = list(map(self.attrConverter.convertLength, points))
        if len(points) % 2 != 0 or len(points) == 0:
            # Odd number of coordinates or no coordinates, invalid polyline
            return None

        nudge_points(points)
        polyline = PolyLine(points)
        self.applyStyleOnShape(polyline, node)
        has_fill = self.attrConverter.findAttr(node, "fill") not in ("", "none")

        if has_fill:
            # ReportLab doesn't fill polylines, so we are creating a polygon
            # polygon copy of the polyline, but without stroke.
            group = Group()
            polygon = Polygon(points)
            self.applyStyleOnShape(polygon, node)
            polygon.strokeColor = None
            group.add(polygon)
            group.add(polyline)
            return group

        return polyline

    def convertPolygon(self, node: Any) -> Optional[Polygon]:
        """Convert an SVG <polygon> element to a ReportLab Polygon."""
        points = node.getAttribute("points")
        points = points.replace(",", " ")
        points = points.split()
        points = list(map(self.attrConverter.convertLength, points))
        if len(points) % 2 != 0 or len(points) == 0:
            # Odd number of coordinates or no coordinates, invalid polygon
            return None
        nudge_points(points)
        shape = Polygon(points)

        return shape

    def convertText(self, node: Any) -> Any:
        """Convert an SVG <text> element to a ReportLab Group of Strings."""
        attrConv = self.attrConverter
        xml_space = node.getAttribute(f"{{{XML_NS}}}space")
        if xml_space:
            preserve_space = xml_space == "preserve"
        else:
            preserve_space = self.preserve_space

        gr = Group()

        frag_lengths: List[float] = []

        dx0: float = 0
        dy0: float = 0
        x1: Union[float, List[float]] = 0
        y1: Union[float, List[float]] = 0
        ff = attrConv.findAttr(node, "font-family") or DEFAULT_FONT_NAME
        fw = attrConv.findAttr(node, "font-weight") or DEFAULT_FONT_WEIGHT
        fstyle = attrConv.findAttr(node, "font-style") or DEFAULT_FONT_STYLE
        ff = attrConv.convertFontFamily(ff, fw, fstyle)
        fs = attrConv.findAttr(node, "font-size") or str(DEFAULT_FONT_SIZE)
        fs = attrConv.convertLength(fs)  # type: ignore
        x: List[float]
        y: List[float]
        x, y = self.convert_length_attrs(node, "x", "y", em_base=fs)  # type: ignore
        for subnode, text, is_tail in iter_text_node(node, preserve_space):
            if not text:
                continue
            has_x, has_y = False, False
            dx: Union[float, List[float]] = 0
            dy: Union[float, List[float]] = 0
            baseLineShift: Union[float, int] = 0
            if not is_tail:
                x1, y1, dx, dy = self.convert_length_attrs(  # type: ignore
                    subnode,
                    "x",
                    "y",
                    "dx",
                    "dy",
                    em_base=fs,  # type: ignore
                )
                has_x, has_y = (
                    subnode.attrib.get("x", "") != "",
                    subnode.attrib.get("y", "") != "",
                )
                dx0 = dx0 + (dx[0] if isinstance(dx, list) else dx)  # type: ignore
                dy0 = dy0 + (dy[0] if isinstance(dy, list) else dy)  # type: ignore
            baseLineShift = subnode.attrib.get("baseline-shift", "0")
            if baseLineShift in ("sub", "super", "baseline"):
                baseLineShift = {"sub": -fs / 2, "super": fs / 2, "baseline": 0}[  # type: ignore
                    baseLineShift  # type: ignore
                ]
            else:
                baseLineShift = attrConv.convertLength(baseLineShift, em_base=fs)  # type: ignore

            frag_lengths.append(stringWidth(text, ff, fs))  # type: ignore

            # When x, y, dx, or dy is a list, we calculate position for each char of
            # text.
            if any(isinstance(val, list) for val in (x1, y1, dx, dy)):
                if has_x:
                    xlist = x1 if isinstance(x1, list) else [x1]
                else:
                    xlist = [x + dx0 + sum(frag_lengths[:-1])]  # type: ignore
                if has_y:
                    ylist = y1 if isinstance(y1, list) else [y1]
                else:
                    ylist = [y + dy0]  # type: ignore
                dxlist = dx if isinstance(dx, list) else [dx]
                dylist = dy if isinstance(dy, list) else [dy]
                last_x, last_y, last_char = xlist[0], ylist[0], ""
                for char_x, char_y, char_dx, char_dy, char in itertools.zip_longest(
                    xlist, ylist, dxlist, dylist, text
                ):
                    if char is None:
                        break
                    if char_dx is None:
                        char_dx = 0
                    if char_dy is None:
                        char_dy = 0
                    new_x = char_dx + (
                        last_x + stringWidth(last_char, ff, fs)
                        if char_x is None
                        else char_x
                    )
                    new_y = char_dy + (last_y if char_y is None else char_y)
                    shape = String(new_x, -(new_y - baseLineShift), char)
                    self.applyStyleOnShape(shape, node)
                    if node_name(subnode) == "tspan":
                        self.applyStyleOnShape(shape, subnode)
                    gr.add(shape)
                    last_x = new_x
                    last_y = new_y
                    last_char = char
            else:
                new_x = (x1 + dx) if has_x else (x + dx0 + sum(frag_lengths[:-1]))  # type: ignore
                new_y = (y1 + dy) if has_y else (y + dy0)  # type: ignore
                shape = String(new_x, -(new_y - baseLineShift), text)  # type: ignore
                self.applyStyleOnShape(shape, node)
                if node_name(subnode) == "tspan":
                    self.applyStyleOnShape(shape, subnode)
                gr.add(shape)

        gr.scale(1, -1)

        return gr

    def convertPath(self, node: Any) -> Optional[Any]:
        """Convert an SVG <path> element to a ReportLab Path."""
        d = node.get("d")
        if not d:
            return None
        normPath = normalise_svg_path(d)
        path = Path()
        points = path.points
        # Track subpaths needing to be closed later
        unclosed_subpath_pointers: List[int] = []
        subpath_start: List[float] = []
        lastop = ""
        last_quadratic_cp: Optional[Tuple[float, float]] = None

        for i in range(0, len(normPath), 2):
            op: str
            nums: List[float]
            op, nums = normPath[i : i + 2]  # type: ignore

            if op in ("m", "M") and i > 0 and path.operators[-1] != _CLOSEPATH:
                unclosed_subpath_pointers.append(len(path.operators))

            # moveto absolute
            if op == "M":
                path.moveTo(*nums)
                subpath_start = points[-2:]
            # lineto absolute
            elif op == "L":
                path.lineTo(*nums)

            # moveto relative
            elif op == "m":
                if len(points) >= 2:
                    if lastop in ("Z", "z"):
                        starting_point = subpath_start
                    else:
                        starting_point = points[-2:]
                    xn, yn = starting_point[0] + nums[0], starting_point[1] + nums[1]
                    path.moveTo(xn, yn)
                else:
                    path.moveTo(*nums)
                subpath_start = points[-2:]
            # lineto relative
            elif op == "l":
                xn, yn = points[-2] + nums[0], points[-1] + nums[1]
                path.lineTo(xn, yn)

            # horizontal/vertical line absolute
            elif op == "H":
                path.lineTo(nums[0], points[-1])
            elif op == "V":
                path.lineTo(points[-2], nums[0])

            # horizontal/vertical line relative
            elif op == "h":
                path.lineTo(points[-2] + nums[0], points[-1])
            elif op == "v":
                path.lineTo(points[-2], points[-1] + nums[0])

            # cubic bezier, absolute
            elif op == "C":
                path.curveTo(*nums)
            elif op == "S":
                x2, y2, xn, yn = nums
                if len(points) < 4 or lastop not in {"c", "C", "s", "S"}:
                    xp, yp, x0, y0 = points[-2:] * 2
                else:
                    xp, yp, x0, y0 = points[-4:]
                xi, yi = x0 + (x0 - xp), y0 + (y0 - yp)
                path.curveTo(xi, yi, x2, y2, xn, yn)

            # cubic bezier, relative
            elif op == "c":
                xp, yp = points[-2:]
                x1, y1, x2, y2, xn, yn = nums
                path.curveTo(xp + x1, yp + y1, xp + x2, yp + y2, xp + xn, yp + yn)
            elif op == "s":
                x2, y2, xn, yn = nums
                if len(points) < 4 or lastop not in {"c", "C", "s", "S"}:
                    xp, yp, x0, y0 = points[-2:] * 2
                else:
                    xp, yp, x0, y0 = points[-4:]
                xi, yi = x0 + (x0 - xp), y0 + (y0 - yp)
                path.curveTo(xi, yi, x0 + x2, y0 + y2, x0 + xn, y0 + yn)

            # quadratic bezier, absolute
            elif op == "Q":
                x0, y0 = points[-2:]
                x1, y1, xn, yn = nums
                last_quadratic_cp = (x1, y1)
                (_, _), (x1, y1), (x2, y2), (_, _) = convert_quadratic_to_cubic_path(
                    (x0, y0), (x1, y1), (xn, yn)
                )
                path.curveTo(x1, y1, x2, y2, xn, yn)
            elif op == "T":
                if last_quadratic_cp is not None:
                    xp, yp = last_quadratic_cp
                else:
                    xp, yp = points[-2:]
                x0, y0 = points[-2:]
                xi, yi = x0 + (x0 - xp), y0 + (y0 - yp)
                last_quadratic_cp = (xi, yi)
                xn, yn = nums
                (
                    (_, _),
                    (x1, y1),
                    (x2, y2),
                    (_, _),
                ) = convert_quadratic_to_cubic_path((x0, y0), (xi, yi), (xn, yn))
                path.curveTo(x1, y1, x2, y2, xn, yn)

            # quadratic bezier, relative
            elif op == "q":
                x0, y0 = points[-2:]
                x1, y1, xn, yn = nums
                x1, y1, xn, yn = x0 + x1, y0 + y1, x0 + xn, y0 + yn
                last_quadratic_cp = (x1, y1)
                (_, _), (x1, y1), (x2, y2), (_, _) = convert_quadratic_to_cubic_path(
                    (x0, y0), (x1, y1), (xn, yn)
                )
                path.curveTo(x1, y1, x2, y2, xn, yn)
            elif op == "t":
                if last_quadratic_cp is not None:
                    xp, yp = last_quadratic_cp
                else:
                    xp, yp = points[-2:]
                x0, y0 = points[-2:]
                xn, yn = nums
                xn, yn = x0 + xn, y0 + yn
                xi, yi = x0 + (x0 - xp), y0 + (y0 - yp)
                last_quadratic_cp = (xi, yi)
                (
                    (_, _),
                    (x1, y1),
                    (x2, y2),
                    (_, _),
                ) = convert_quadratic_to_cubic_path((x0, y0), (xi, yi), (xn, yn))
                path.curveTo(x1, y1, x2, y2, xn, yn)

            # elliptical arc
            elif op in ("A", "a"):
                rx, ry, phi, fA, fS, x2, y2 = nums
                x1, y1 = points[-2:]
                if op == "a":
                    x2 += x1
                    y2 += y1
                if abs(rx) <= 1e-10 or abs(ry) <= 1e-10:
                    path.lineTo(x2, y2)
                else:
                    bp = bezier_arc_from_end_points(
                        x1, y1, rx, ry, phi, int(fA), int(fS), x2, y2
                    )
                    for _, _, x1, y1, x2, y2, xn, yn in bp:
                        path.curveTo(x1, y1, x2, y2, xn, yn)

            # close path
            elif op in ("Z", "z"):
                path.closePath()

            else:
                logger.debug("Suspicious path operator: %s", op)

            if op not in ("Q", "q", "T", "t"):
                last_quadratic_cp = None
            lastop = op

        gr = Group()
        self.applyStyleOnShape(path, node)

        if path.operators[-1] != _CLOSEPATH:
            unclosed_subpath_pointers.append(len(path.operators))

        if unclosed_subpath_pointers and path.fillColor is not None:
            # ReportLab doesn't fill unclosed paths, so we are creating a copy
            # of the path with all subpaths closed, but without stroke.
            # https://bitbucket.org/rptlab/reportlab/issues/99/
            closed_path = NoStrokePath(copy_from=path)
            for pointer in reversed(unclosed_subpath_pointers):
                closed_path.operators.insert(pointer, _CLOSEPATH)
            gr.add(closed_path)
            path.fillColor = None

        gr.add(path)
        return gr

    def convertImage(self, node: Any) -> Any:
        """Convert an SVG <image> element to a ReportLab Image."""
        x, y, width, height = self.convert_length_attrs(
            node, "x", "y", "width", "height"
        )
        image = node._resolved_target
        image = Image(int(x), int(y + height), int(width), int(height), image)

        group = Group(image)
        group.translate(0, (y + height) * 2)
        group.scale(1, -1)
        return group

    def applyTransformOnGroup(self, transform: str, group: Any) -> None:
        """Apply an SVG transformation to a ReportLab Group.

        Args:
            transform: The SVG transform attribute string.
            group: The ReportLab Group to apply the transform to.
        """
        tr = self.attrConverter.convertTransform(transform)
        for op, values in tr:
            if op == "scale":
                if not isinstance(values, tuple):
                    values = (values, values)
                group.scale(*values)
            elif op == "translate":
                if isinstance(values, (int, float)):
                    # From the SVG spec: If <ty> is not provided, it is assumed to
                    # be zero.
                    values = values, 0
                group.translate(*values)
            elif op == "rotate":
                if not isinstance(values, tuple) or len(values) == 1:  # type: ignore
                    group.rotate(values)
                elif len(values) == 3:
                    angle, cx, cy = values
                    group.translate(cx, cy)
                    group.rotate(angle)
                    group.translate(-cx, -cy)
            elif op == "skewX":
                group.skew(values, 0)
            elif op == "skewY":
                group.skew(0, values)
            elif op == "matrix" and len(values) == 6:  # type: ignore
                group.transform = mmult(group.transform, values)
            else:
                logger.debug("Ignoring transform: %s %s", op, values)

    def applyStyleOnShape(
        self,
        shape: Any,
        node: Any,
        only_explicit: bool = False,
    ) -> None:
        """Apply styles from an SVG node to a ReportLab shape.

        Args:
            shape: The ReportLab shape to apply styles to.
            node: The lxml node to get style attributes from.
            only_explicit: If True, only apply explicitly defined attributes.
        """
        # RLG-specific: all RLG shapes
        """Apply style attributes of a sequence of nodes to an RL shape."""

        # tuple format: (svgAttributes, rlgAttr, converter, default)
        mappingN = (
            (["fill"], "fillColor", "convertColor", ["black"]),
            (["fill-opacity"], "fillOpacity", "convertOpacity", [1]),
            (["fill-rule"], "_fillRule", "convertFillRule", ["nonzero"]),
            (["stroke"], "strokeColor", "convertColor", ["none"]),
            (["stroke-width"], "strokeWidth", "convertLength", ["1"]),
            (["stroke-opacity"], "strokeOpacity", "convertOpacity", [1]),
            (["stroke-linejoin"], "strokeLineJoin", "convertLineJoin", ["0"]),
            (["stroke-linecap"], "strokeLineCap", "convertLineCap", ["0"]),
            (["stroke-dasharray"], "strokeDashArray", "convertDashArray", ["none"]),
        )
        mappingF = (
            (
                ["font-family", "font-weight", "font-style"],
                "fontName",
                "convertFontFamily",
                [DEFAULT_FONT_NAME, DEFAULT_FONT_WEIGHT, DEFAULT_FONT_STYLE],
            ),
            (["font-size"], "fontSize", "convertLength", [str(DEFAULT_FONT_SIZE)]),
            (["text-anchor"], "textAnchor", "id", ["start"]),
        )

        if shape.__class__ == Group:
            # Recursively apply style on Group subelements
            for subshape in shape.contents:
                self.applyStyleOnShape(subshape, node, only_explicit=only_explicit)
            return

        ac = self.attrConverter
        for mapping in (mappingN, mappingF):
            if shape.__class__ != String and mapping == mappingF:
                continue
            for svgAttrNames, rlgAttr, func, defaults in mapping:
                svgAttrValues = []
                for index, svgAttrName in enumerate(svgAttrNames):
                    svgAttrValue = ac.findAttr(node, svgAttrName)
                    if svgAttrValue == "":
                        if only_explicit:
                            continue
                        if (
                            svgAttrName == "fill-opacity"
                            and getattr(shape, "fillColor", None) is not None
                            and getattr(shape.fillColor, "alpha", 1) != 1  # type: ignore
                        ):
                            svgAttrValue = shape.fillColor.alpha  # type: ignore
                        elif (
                            svgAttrName == "stroke-opacity"
                            and getattr(shape, "strokeColor", None) is not None
                            and getattr(shape.strokeColor, "alpha", 1) != 1  # type: ignore
                        ):
                            svgAttrValue = shape.strokeColor.alpha  # type: ignore
                        else:
                            svgAttrValue = defaults[index]  # type: ignore
                    if svgAttrValue == "currentColor":
                        svgAttrValue = (
                            ac.findAttr(node.parent, "color") or defaults[index]  # type: ignore
                        )
                    if isinstance(svgAttrValue, str):
                        svgAttrValue = svgAttrValue.replace("!important", "").strip()
                    svgAttrValues.append(svgAttrValue)
                try:
                    meth = getattr(ac, func)
                    setattr(shape, rlgAttr, meth(*svgAttrValues))
                except (AttributeError, KeyError, ValueError):
                    logger.debug("Exception during applyStyleOnShape")
        if getattr(shape, "fillOpacity", None) is not None and shape.fillColor:
            shape.fillColor.alpha = shape.fillOpacity
        if getattr(shape, "strokeWidth", None) == 0:
            # Quoting from the PDF 1.7 spec:
            # A line width of 0 denotes the thinnest line that can be rendered at
            # device resolution: 1 device pixel wide. However, some devices cannot
            # reproduce 1-pixel lines, and on high-resolution devices, they are
            # nearly invisible. Since the results of rendering such zero-width
            # lines are device-dependent, their use is not recommended.
            shape.strokeColor = None


def svg2rlg(
    path: Union[str, os.PathLike[str]], resolve_entities: bool = False, **kwargs: Any
) -> Optional[Drawing]:
    """Convert an SVG file to a ReportLab Drawing object.

    Args:
        path: A file path, file-like object, or pathlib.Path to the SVG file.
        resolve_entities: Whether to resolve XML entities (default False).
        **kwargs: Additional keyword arguments for the SvgRenderer.

    Returns:
        A ReportLab Drawing object, or None if the file cannot be processed.
    """
    if isinstance(path, pathlib.Path):
        path = str(path)

    # unzip .svgz file into .svg
    unzipped = False
    if isinstance(path, str) and os.path.splitext(path)[1].lower() == ".svgz":
        with gzip.open(path, "rb") as f_in, open(path[:-1], "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        path = path[:-1]
        unzipped = True

    svg_root = load_svg_file(path, resolve_entities=resolve_entities)
    if svg_root is None:
        return None

    # convert to a RLG drawing
    svgRenderer = SvgRenderer(path, **kwargs)
    drawing = svgRenderer.render(svg_root)

    # remove unzipped .svgz file (.svg)
    if unzipped:
        os.remove(path)

    return drawing


def nudge_points(points: List[float]) -> None:
    """Nudge the first coordinate if all coordinate pairs are identical.

    This is a workaround for a ReportLab issue where shapes of size zero
    are not rendered, even if they have a visible stroke.

    Args:
        points: A list of coordinates [x1, y1, x2, y2, ...].
    """
    if not points:
        return
    if len(points) < 4:
        return
    x = points[0]
    y = points[1]
    for i in range(2, len(points) - 1, 2):
        if x != points[i] or y != points[i + 1]:
            break
    else:
        # All points were identical, so we nudge.
        points[0] *= 1.0000001


def load_svg_file(
    path: Union[str, os.PathLike[str]], resolve_entities: bool = False
) -> Optional[Any]:
    """Load an SVG file and return the root lxml node.

    Args:
        path: A file path or file-like object for the SVG file.
        resolve_entities: Whether to resolve XML entities.

    Returns:
        The root lxml node of the SVG document, or None on failure.
    """
    parser = etree.XMLParser(
        remove_comments=True, recover=True, resolve_entities=resolve_entities
    )
    try:
        doc = etree.parse(path, parser=parser)
        svg_root = doc.getroot()
    except Exception as exc:
        logger.error("Failed to load input file! (%s)", exc)
        return None
    else:
        return svg_root


def node_name(node: Any) -> Optional[str]:
    """Return the name of an lxml node without the namespace prefix.

    Args:
        node: The lxml node.

    Returns:
        The node name as a string, or None if the node is invalid.
    """
    try:
        return node.tag.split("}")[-1]
    except AttributeError:
        return None


def iter_text_node(node: Any, preserve_space: bool, level: int = 0) -> Any:
    """Recursively iterate through a text node and its children.

    This generator yields the node, its text, and its tail text, handling
    whitespace according to the 'xml:space' attribute.

    Args:
        node: The lxml node to start iteration from.
        preserve_space: Whether to preserve whitespace.
        level: The current recursion level.

    Yields:
        A tuple of (node, text, is_tail).
    """
    level0 = level == 0
    text = (
        clean_text(
            node.text,
            preserve_space,
            strip_start=level0,
            strip_end=(level0 and len(node.getchildren()) == 0),
        )
        if node.text
        else None
    )

    yield node, text, False

    for child in node.iter_children():
        yield from iter_text_node(child, preserve_space, level=level + 1)

    if level > 0:  # We are not interested by tail of main node.
        strip_end = level <= 1 and node.getnext() is None
        tail = (
            clean_text(node.tail, preserve_space, strip_end=strip_end)
            if node.tail
            else None
        )
        if tail not in (None, ""):
            yield node.parent, tail, True


def clean_text(
    text: Optional[str],
    preserve_space: bool,
    strip_start: bool = False,
    strip_end: bool = False,
) -> Optional[str]:
    """Clean text content according to SVG whitespace handling rules.

    Args:
        text: The text content to clean.
        preserve_space: Whether to preserve whitespace.
        strip_start: Whether to strip leading whitespace.
        strip_end: Whether to strip trailing whitespace.

    Returns:
        The cleaned text, or None if the input was None.
    """
    if text is None:
        return None
    text = text.replace("\r\n", " ").replace("\n", " ").replace("\t", " ")
    if not preserve_space:
        if strip_start:
            text = text.lstrip()
        if strip_end:
            text = text.rstrip()
        while "  " in text:
            text = text.replace("  ", " ")
    return text


def copy_shape_properties(source_shape: Any, dest_shape: Any) -> None:
    """Copy properties from one ReportLab shape to another.

    Args:
        source_shape: The shape to copy properties from.
        dest_shape: The shape to copy properties to.
    """
    for prop, val in source_shape.getProperties().items():
        try:
            setattr(dest_shape, prop, val)
        except AttributeError:
            pass


def monkeypatch_reportlab() -> None:
    """Apply a patch to ReportLab to handle path fill rules correctly.

    This patch addresses an issue where ReportLab does not honor the
    'fill-rule' attribute of SVG paths, defaulting to 'even-odd'.
    """
    from reportlab.graphics import shapes
    from reportlab.pdfgen.canvas import Canvas

    original_renderPath = shapes._renderPath

    def patchedRenderPath(path: Any, drawFuncs: Any, **kwargs: Any) -> Any:
        # Patched method to transfer fillRule from Path to PDFPathObject
        # Get back from bound method to instance
        try:
            drawFuncs[0].__self__.fillMode = path._fillRule
        except AttributeError:
            pass
        return original_renderPath(path, drawFuncs, **kwargs)

    shapes._renderPath = patchedRenderPath

    original_drawPath = Canvas.drawPath

    def patchedDrawPath(self: Any, path: Any, **kwargs: Any) -> Any:
        current = self._fillMode
        if hasattr(path, "fillMode"):
            self._fillMode = path.fillMode
        else:
            self._fillMode = FILL_NON_ZERO
        original_drawPath(self, path, **kwargs)
        self._fillMode = current

    Canvas.drawPath = patchedDrawPath


monkeypatch_reportlab()
