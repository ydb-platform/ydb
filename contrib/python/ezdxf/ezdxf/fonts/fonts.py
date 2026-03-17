#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Optional, TYPE_CHECKING, cast
import abc
import enum
import logging
import os
import pathlib

from ezdxf import options
from .font_face import FontFace
from .font_manager import (
    FontManager,
    SUPPORTED_TTF_TYPES,
    FontNotFoundError,
    UnsupportedFont,
)
from .font_synonyms import FONT_SYNONYMS
from .font_measurements import FontMeasurements
from .glyphs import GlyphPath, Glyphs

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity, Textstyle

logger = logging.getLogger("ezdxf")
FONT_MANAGER_CACHE_FILE = "font_manager_cache.json"
# SUT = System Under Test, see build_sut_font_manager_cache() function
SUT_FONT_MANAGER_CACHE = False
CACHE_DIRECTORY = ".cache"
font_manager = FontManager()

SHX_FONTS = {
    # See examples in: CADKitSamples/Shapefont.dxf
    # Shape file structure is not documented, therefore, replace these fonts by
    # true type fonts.
    # `None` is for: use the default font.
    #
    # All these replacement TTF fonts have a copyright remark:
    # "(c) Copyright 1996 by Autodesk Inc., All rights reserved"
    # and therefore can not be included in ezdxf or the associated repository!
    # You got them if you install any Autodesk product, like the free available
    # DWG/DXF viewer "TrueView" : https://www.autodesk.com/viewers
    "AMGDT": "amgdt___.ttf",  # Tolerance symbols
    "AMGDT.SHX": "amgdt___.ttf",
    "COMPLEX": "complex_.ttf",
    "COMPLEX.SHX": "complex_.ttf",
    "ISOCP": "isocp.ttf",
    "ISOCP.SHX": "isocp.ttf",
    "ITALIC": "italicc_.ttf",
    "ITALIC.SHX": "italicc_.ttf",
    "GOTHICG": "gothicg_.ttf",
    "GOTHICG.SHX": "gothicg_.ttf",
    "GREEKC": "greekc.ttf",
    "GREEKC.SHX": "greekc.ttf",
    "ROMANS": "romans__.ttf",
    "ROMANS.SHX": "romans__.ttf",
    "SCRIPTS": "scripts_.ttf",
    "SCRIPTS.SHX": "scripts_.ttf",
    "SCRIPTC": "scriptc_.ttf",
    "SCRIPTC.SHX": "scriptc_.ttf",
    "SIMPLEX": "simplex_.ttf",
    "SIMPLEX.SHX": "simplex_.ttf",
    "SYMATH": "symath__.ttf",
    "SYMATH.SHX": "symath__.ttf",
    "SYMAP": "symap___.ttf",
    "SYMAP.SHX": "symap___.ttf",
    "SYMETEO": "symeteo_.ttf",
    "SYMETEO.SHX": "symeteo_.ttf",
    "TXT": "txt_____.ttf",  # Default AutoCAD font
    "TXT.SHX": "txt_____.ttf",
}
LFF_FONTS = {
    "TXT": "standard.lff",
    "TXT.SHX": "standard.lff",
}
TTF_TO_SHX = {v: k for k, v in SHX_FONTS.items() if k.endswith("SHX")}
DESCENDER_FACTOR = 0.333  # from TXT SHX font - just guessing
X_HEIGHT_FACTOR = 0.666  # from TXT SHX font - just guessing
MONOSPACE = "*monospace"  # last resort fallback font only for measurement


def map_shx_to_ttf(font_name: str) -> str:
    """Map .shx font names to .ttf file names. e.g. "TXT" -> "txt_____.ttf" """
    # Map SHX fonts to True Type Fonts:
    font_upper = font_name.upper()
    if font_upper in SHX_FONTS:
        font_name = SHX_FONTS[font_upper]
    return font_name


def map_shx_to_lff(font_name: str) -> str:
    """Map .shx font names to .lff file names. e.g. "TXT" -> "standard.lff" """
    font_upper = font_name.upper()
    name = LFF_FONTS.get(font_upper, "")
    if font_manager.has_font(name):
        return name
    if not font_upper.endswith(".SHX"):
        lff_name = font_name + ".lff"
    else:
        lff_name = font_name[:-4] + ".lff"
    if font_manager.has_font(lff_name):
        return lff_name
    return font_name


def is_shx_font_name(font_name: str) -> bool:
    name = font_name.lower()
    if name.endswith(".shx"):
        return True
    if "." not in name:
        return True
    return False


def map_ttf_to_shx(ttf: str) -> Optional[str]:
    """Maps .ttf filenames to .shx font names. e.g. "txt_____.ttf" -> "TXT" """
    return TTF_TO_SHX.get(ttf.lower())


def build_system_font_cache() -> None:
    """Builds or rebuilds the font manager cache. The font manager cache has a fixed
    location in the cache directory of the users home directory "~/.cache/ezdxf" or the
    directory specified by the environment variable "XDG_CACHE_HOME".
    """
    build_font_manager_cache(_get_font_manager_path())


def find_font_face(font_name: str) -> FontFace:
    """Returns the :class:`FontFace` definition for the given font filename
    e.g. "LiberationSans-Regular.ttf".

    """
    return font_manager.get_font_face(font_name)


def get_font_face(font_name: str, map_shx=True) -> FontFace:
    """Returns  the :class:`FontFace` definition for the given font filename
    e.g. "LiberationSans-Regular.ttf".

    This function translates a DXF font definition by the TTF font file name into a
    :class:`FontFace` object. Returns the :class:`FontFace` of the default font when a
    font is not available on the current system.

    Args:
        font_name: raw font file name as stored in the
            :class:`~ezdxf.entities.Textstyle` entity
        map_shx: maps SHX font names to TTF replacement fonts,
            e.g. "TXT" -> "txt_____.ttf"

    """
    if not isinstance(font_name, str):
        raise TypeError("font_name has invalid type")
    if map_shx:
        font_name = map_shx_to_ttf(font_name)
    return find_font_face(font_name)


def resolve_shx_font_name(font_name: str, order: str) -> str:
    """Resolves a .shx font name, the argument `order` defines the resolve order:

    - "t" = map .shx fonts to TrueType fonts (.ttf, .ttc, .otf)
    - "s" = use shapefile fonts (.shx, .shp)
    - "l" = map .shx fonts to LibreCAD fonts (.lff)

    """
    if len(order) == 0:
        return font_name
    order = order.lower()
    for type_str in order:
        if type_str == "t":
            name = map_shx_to_ttf(font_name)
            if font_manager.has_font(name):
                return name
        elif type_str == "s":
            if not font_name.lower().endswith(".shx"):
                font_name += ".shx"
            if font_manager.has_font(font_name):
                return font_name
        elif type_str == "l":
            name = map_shx_to_lff(font_name)
            if font_manager.has_font(name):
                return name
    return font_name


def resolve_font_face(font_name: str, order="tsl") -> FontFace:
    """Returns the :class:`FontFace` definition for the given font filename
    e.g. "LiberationSans-Regular.ttf".

    This function translates a DXF font definition by the TTF font file name into a
    :class:`FontFace` object. Returns the :class:`FontFace` of the default font when a
    font is not available on the current system.

    The order argument defines the resolve order for .shx fonts:

    - "t" = map .shx fonts to TrueType fonts (.ttf, .ttc, .otf)
    - "s" = use shapefile fonts (.shx, .shp)
    - "l" = map .shx fonts to LibreCAD fonts (.lff)

    Args:
        font_name: raw font file name as stored in the
            :class:`~ezdxf.entities.Textstyle` entity
        order: resolving order

    """
    if not isinstance(font_name, str):
        raise TypeError("font_name has invalid type")
    if is_shx_font_name(font_name):
        font_name = resolve_shx_font_name(font_name, order)
    return find_font_face(font_name)


def get_font_measurements(font_name: str, map_shx=True) -> FontMeasurements:
    """Get :class:`FontMeasurements` for the given font filename
    e.g. "LiberationSans-Regular.ttf".

    Args:
        font_name: raw font file name as stored in the
            :class:`~ezdxf.entities.Textstyle` entity
        map_shx: maps SHX font names to TTF replacement fonts,
            e.g. "TXT" -> "txt_____.ttf"

    """
    if map_shx:
        font_name = map_shx_to_ttf(font_name)
    elif is_shx_font_name(font_name):
        return FontMeasurements(
            baseline=0,
            cap_height=1,
            x_height=X_HEIGHT_FACTOR,
            descender_height=DESCENDER_FACTOR,
        )
    font = TrueTypeFont(font_name, cap_height=1)
    return font.measurements


def find_best_match(
    *,
    family: str = "sans-serif",
    style: str = "Regular",
    weight: int = 400,
    width: int = 5,
    italic: Optional[bool] = False,
) -> Optional[FontFace]:
    """Returns a :class:`FontFace` that matches the given properties best. The search
    is based the descriptive properties and not on comparing glyph shapes. Returns
    ``None`` if no font was found.

    Args:
        family: font family name e.g. "sans-serif", "Liberation Sans"
        style: font style e.g. "Regular", "Italic", "Bold"
        weight: weight in the range from 1-1000 (usWeightClass)
        width: width in the range from 1-9 (usWidthClass)
        italic: ``True``, ``False`` or ``None`` to ignore this flag

    """
    return font_manager.find_best_match(family, style, weight, width, italic)


def find_font_file_name(font_face: FontFace) -> str:
    """Returns the true type font file name without parent directories e.g. "Arial.ttf"."""
    return font_manager.find_font_name(font_face)


def load():
    """Reload all cache files. The cache files are loaded automatically at the import
    of `ezdxf`.
    """
    _load_font_manager()
    # Add font name synonyms, see discussion #1002
    # Find macOS fonts on Windows/Linux and vice versa.
    font_manager.add_synonyms(FONT_SYNONYMS, reverse=True)


def _get_font_manager_path():
    cache_path = options.xdg_path("XDG_CACHE_HOME", CACHE_DIRECTORY)
    return cache_path / FONT_MANAGER_CACHE_FILE


def _load_font_manager() -> None:
    fm_path = _get_font_manager_path()
    if fm_path.exists():
        try:
            font_manager.loads(fm_path.read_text())
            return
        except IOError as e:
            logger.info(f"Error loading cache file: {str(e)}")
    build_font_manager_cache(fm_path)


def build_sut_font_manager_cache(repo_font_path: pathlib.Path) -> None:
    """Load font manger cache for system under test (sut).

    Load the fonts included in the repository folder "./fonts" to guarantee the tests
    have the same fonts available on all systems.

    This function should be called from "conftest.py".

    """
    global SUT_FONT_MANAGER_CACHE
    SUT_FONT_MANAGER_CACHE = True
    font_manager.clear()
    cache_file = repo_font_path / "font_manager_cache.json"
    if cache_file.exists():
        try:
            font_manager.loads(cache_file.read_text())
            return
        except IOError as e:
            print(f"Error loading cache file: {str(e)}")
    font_manager.build([str(repo_font_path)], support_dirs=False)
    s = font_manager.dumps()
    try:
        cache_file.write_text(s)
    except IOError as e:
        print(f"Error writing cache file: {str(e)}")


def make_cache_directory(path: pathlib.Path) -> None:
    if not path.exists():
        try:
            path.mkdir(parents=True)
        except IOError:
            pass


def build_font_manager_cache(path: pathlib.Path) -> None:
    font_manager.clear()
    font_manager.build()
    s = font_manager.dumps()
    cache_dir = path.parent
    make_cache_directory(cache_dir)
    if not cache_dir.exists():
        logger.warning(
            f"Cannot create cache home directory: '{str(cache_dir)}', cache files will "
            f"not be saved.\nSee also issue https://github.com/mozman/ezdxf/issues/923."
        )
        return
    try:
        path.write_text(s)
    except IOError as e:
        logger.warning(f"Error writing cache file: '{str(e)}'")


class FontRenderType(enum.Enum):
    # render glyphs as filled paths: TTF, OTF
    OUTLINE = enum.auto()

    # render glyphs as line strokes: SHX, SHP
    STROKE = enum.auto


class AbstractFont:
    """The `ezdxf` font abstraction for text measurement and text path rendering."""

    font_render_type = FontRenderType.STROKE
    name: str = "undefined"

    def __init__(self, measurements: FontMeasurements):
        self.measurements = measurements

    @abc.abstractmethod
    def text_width(self, text: str) -> float:
        """Returns the text width in drawing units for the given `text` string."""
        pass

    @abc.abstractmethod
    def text_width_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> float:
        """Returns the text width in drawing units, bypasses the stored `cap_height` and
        `width_factor`.
        """
        pass

    @abc.abstractmethod
    def space_width(self) -> float:
        """Returns the width of a "space" character a.k.a. word spacing."""
        pass

    @abc.abstractmethod
    def text_path(self, text: str) -> GlyphPath:
        """Returns the 2D text path for the given text."""
        ...

    @abc.abstractmethod
    def text_path_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> GlyphPath:
        """Returns the 2D text path for the given text, bypasses the stored `cap_height`
        and `width_factor`."""
        ...

    @abc.abstractmethod
    def text_glyph_paths(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        """Returns a list of 2D glyph paths for the given text, bypasses the stored
        `cap_height` and `width_factor`."""
        ...


class MonospaceFont(AbstractFont):
    """Represents a monospaced font where each letter has the same cap- and descender
    height and the same width. The given cap height and width factor are the default
    values for measurements and rendering. The extended methods can override these
    default values.

    This font exists only for generic text measurement in tests and does not render any
    glyphs!

    """

    font_render_type = FontRenderType.STROKE
    name = MONOSPACE

    def __init__(
        self,
        cap_height: float,
        width_factor: float = 1.0,
        baseline: float = 0,
        descender_factor: float = DESCENDER_FACTOR,
        x_height_factor: float = X_HEIGHT_FACTOR,
    ):
        super().__init__(
            FontMeasurements(
                baseline=baseline,
                cap_height=cap_height,
                x_height=cap_height * x_height_factor,
                descender_height=cap_height * descender_factor,
            )
        )
        self._width_factor: float = abs(width_factor)
        self._space_width = self.measurements.cap_height * self._width_factor

    def text_width(self, text: str) -> float:
        """Returns the text width in drawing units for the given `text`."""
        return self.text_width_ex(
            text, self.measurements.cap_height, self._width_factor
        )

    def text_width_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> float:
        """Returns the text width in drawing units, bypasses the stored `cap_height` and
        `width_factor`.
        """
        return len(text) * cap_height * width_factor

    def text_path(self, text: str) -> GlyphPath:
        """Returns the rectangle text width x cap height as :class:`NumpyPath2d` instance."""
        return self.text_path_ex(text, self.measurements.cap_height, self._width_factor)

    def text_path_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> GlyphPath:
        """Returns the rectangle text width x cap height as  :class:`NumpyPath2d`
        instance, bypasses the stored `cap_height` and `width_factor`.
        """
        from ezdxf.path import Path

        text_width = self.text_width_ex(text, cap_height, width_factor)
        p = Path((0, 0))
        p.line_to((text_width, 0))
        p.line_to((text_width, cap_height))
        p.line_to((0, cap_height))
        p.close()
        return GlyphPath(p)

    def text_glyph_paths(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        """Returns the same rectangle as the method :meth:`text_path_ex` in a list."""
        return [self.text_path_ex(text, cap_height, width_factor)]

    def space_width(self) -> float:
        """Returns the width of a "space" char."""
        return self._space_width


class _CachedFont(AbstractFont, abc.ABC):
    """Abstract font with caching support."""

    _glyph_caches: dict[str, Glyphs] = dict()

    def __init__(self, font_name: str, cap_height: float, width_factor: float = 1.0):
        self.name = font_name
        cache = self.create_cache(font_name)
        self.glyph_cache = cache
        self.cap_height = float(cap_height)
        self.width_factor = float(width_factor)
        scale_factor: float = cache.get_scaling_factor(self.cap_height)
        super().__init__(cache.font_measurements.scale(scale_factor))
        self._space_width: float = (
            self.glyph_cache.space_width * scale_factor * width_factor
        )

    @abc.abstractmethod
    def create_cache(self, font_name: str) -> Glyphs:
        ...

    def text_width(self, text: str) -> float:
        """Returns the text width in drawing units for the given `text` string."""
        return self.text_width_ex(text, self.cap_height, self.width_factor)

    def text_width_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> float:
        """Returns the text width in drawing units, bypasses the stored `cap_height` and
        `width_factor`.
        """
        if not text.strip():
            return 0
        return self.glyph_cache.get_text_length(text, cap_height, width_factor)

    def text_path(self, text: str) -> GlyphPath:
        """Returns the 2D text path for the given text."""

        return self.text_path_ex(text, self.cap_height, self.width_factor)

    def text_path_ex(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> GlyphPath:
        """Returns the 2D text path for the given text, bypasses the stored `cap_height`
        and `width_factor`."""
        return self.glyph_cache.get_text_path(text, cap_height, width_factor)

    def text_glyph_paths(
        self, text: str, cap_height: float, width_factor: float = 1.0
    ) -> list[GlyphPath]:
        """Returns a list of 2D glyph paths for the given text, bypasses the stored
        `cap_height` and `width_factor`."""
        return self.glyph_cache.get_text_glyph_paths(text, cap_height, width_factor)

    def space_width(self) -> float:
        """Returns the width of a "space" char."""
        return self._space_width


class TrueTypeFont(_CachedFont):
    """Represents a TrueType font. Font measurement and glyph rendering is done by the
    `fontTools` package. The given cap height and width factor are the default values
    for measurements and glyph rendering. The extended methods can override these
    default values.
    """

    font_render_type = FontRenderType.OUTLINE

    def create_cache(self, ttf: str) -> Glyphs:
        from .ttfonts import TTFontRenderer

        key = pathlib.Path(ttf).name.lower()
        try:
            return self._glyph_caches[key]
        except KeyError:
            pass
        try:
            tt_font = font_manager.get_ttf_font(ttf)
            try:  # see issue #990
                cache = TTFontRenderer(tt_font)
            except Exception:
                raise UnsupportedFont
        except UnsupportedFont:
            fallback_font_name = font_manager.fallback_font_name()
            logger.info(f"replacing unsupported font '{ttf}' by '{fallback_font_name}'")
            cache = TTFontRenderer(font_manager.get_ttf_font(fallback_font_name))
        self._glyph_caches[key] = cache
        return cache


class _UnmanagedTrueTypeFont(_CachedFont):
    font_render_type = FontRenderType.OUTLINE

    def create_cache(self, ttf: str) -> Glyphs:
        from .ttfonts import TTFontRenderer
        from fontTools.ttLib import TTFont

        key = ttf.lower()
        try:
            return self._glyph_caches[key]
        except KeyError:
            pass
        cache = TTFontRenderer(TTFont(ttf, fontNumber=0))
        self._glyph_caches[key] = cache
        return cache


def sideload_ttf(font_path: str | os.PathLike, cap_height) -> AbstractFont:
    """This function bypasses the FontManager and loads the TrueType font straight from
    the file system, requires the absolute font file path e.g. "C:/Windows/Fonts/Arial.ttf".

    .. warning::

        Expert feature, use with care: no fallback font and no error handling.

    """

    return _UnmanagedTrueTypeFont(str(font_path), cap_height)


class ShapeFileFont(_CachedFont):
    """Represents a shapefile font (.shx, .shp). Font measurement and glyph rendering is
    done by the ezdxf.fonts.shapefile module. The given cap height and width factor are
    the default values for measurements and glyph rendering. The extended methods can
    override these default values.

    """

    font_render_type = FontRenderType.STROKE

    def create_cache(self, font_name: str) -> Glyphs:
        key = font_name.lower()
        try:
            return self._glyph_caches[key]
        except KeyError:
            pass
        glyph_cache = font_manager.get_shapefile_glyph_cache(font_name)
        self._glyph_caches[key] = glyph_cache
        return glyph_cache


class LibreCadFont(_CachedFont):
    """Represents a LibreCAD font (.shx, .shp). Font measurement and glyph rendering is
    done by the ezdxf.fonts.lff module. The given cap height and width factor are the
    default values for measurements and glyph rendering. The extended methods can
    override these default values.

    """

    font_render_type = FontRenderType.STROKE

    def create_cache(self, font_name: str) -> Glyphs:
        key = font_name.lower()
        try:
            return self._glyph_caches[key]
        except KeyError:
            pass
        glyph_cache = font_manager.get_lff_glyph_cache(font_name)
        self._glyph_caches[key] = glyph_cache
        return glyph_cache


def make_font(
    font_name: str, cap_height: float, width_factor: float = 1.0
) -> AbstractFont:
    r"""Returns a font abstraction based on class :class:`AbstractFont`.

    Supported font types:

    - .ttf, .ttc and .otf - TrueType fonts
    - .shx, .shp - Autodesk® shapefile fonts
    - .lff - LibreCAD font format

    The special name "\*monospace" returns the fallback font :class:`MonospaceFont` for
    testing and basic measurements.

    .. note:: The font definition files are not included in `ezdxf`.

    Args:
        font_name: font file name as stored in the :class:`~ezdxf.entities.Textstyle`
            entity e.g. "OpenSans-Regular.ttf"
        cap_height: desired cap height in drawing units.
        width_factor: horizontal text stretch factor

    """
    if font_name == MONOSPACE:
        return MonospaceFont(cap_height, width_factor)
    ext = pathlib.Path(font_name).suffix.lower()
    last_resort = MonospaceFont(cap_height, width_factor)
    if ext in SUPPORTED_TTF_TYPES:
        try:
            return TrueTypeFont(font_name, cap_height, width_factor)
        except FontNotFoundError as e:
            logger.warning(f"no default font found: {str(e)}")
            return last_resort
    elif ext == ".shx" or ext == ".shp":
        try:
            return ShapeFileFont(font_name, cap_height, width_factor)
        except FontNotFoundError:
            pass
        except UnsupportedFont:
            # change name - the font exists but is not supported
            font_name = font_manager.fallback_font_name()
    elif ext == ".lff":
        try:
            return LibreCadFont(font_name, cap_height, width_factor)
        except FontNotFoundError:
            pass
    elif ext == "":  # e.g. "TXT"
        font_face = font_manager.find_best_match(
            family=font_name, style=".shx", italic=None
        )
        if font_face is not None:
            return make_font(font_face.filename, cap_height, width_factor)
    else:
        logger.warning(f"unsupported font-name suffix: {font_name}")
        font_name = font_manager.fallback_font_name()

    # return default TrueType font
    try:
        return TrueTypeFont(font_name, cap_height, width_factor)
    except FontNotFoundError as e:
        logger.warning(f"no default font found: {str(e)}")
        return last_resort


def get_entity_font_face(entity: DXFEntity, doc: Optional[Drawing] = None) -> FontFace:
    """Returns the :class:`FontFace` defined by the associated text style.
    Returns the default font face if the `entity` does not have or support
    the DXF attribute "style". Supports the extended font information stored in
    :class:`~ezdxf.entities.Textstyle` table entries.

    Pass a DXF document as argument `doc` to resolve text styles for virtual
    entities which are not assigned to a DXF document. The argument `doc`
    always overrides the DXF document to which the `entity` is assigned to.

    """
    if entity.doc and doc is None:
        doc = entity.doc
    if doc is None:
        return FontFace()

    style_name = ""
    # This works also for entities which do not support "style",
    # where style_name = entity.dxf.get("style") would fail.
    if entity.dxf.is_supported("style"):
        style_name = entity.dxf.style

    font_face = FontFace()
    if style_name:
        style = cast("Textstyle", doc.styles.get(style_name))
        family, italic, bold = style.get_extended_font_data()
        if family:
            text_style = "Italic" if italic else "Regular"
            text_weight = 700 if bold else 400
            font_face = FontFace(family=family, style=text_style, weight=text_weight)
        else:
            ttf = style.dxf.font
            if ttf:
                font_face = get_font_face(ttf)
    return font_face


load()
