#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations

import pathlib
from typing import Iterable, NamedTuple, Optional, Sequence
import os
import platform
import json
import logging

from pathlib import Path
from fontTools.ttLib import TTFont, TTLibError
from .font_face import FontFace
from . import shapefile, lff

logger = logging.getLogger("ezdxf")
WINDOWS = "Windows"
LINUX = "Linux"
MACOS = "Darwin"


WIN_SYSTEM_ROOT = os.environ.get("SystemRoot", "C:/Windows")
WIN_FONT_DIRS = [
    # AutoCAD and BricsCAD do not support fonts installed in the user directory:
    "~/AppData/Local/Microsoft/Windows/Fonts",
    f"{WIN_SYSTEM_ROOT}/Fonts",
]
LINUX_FONT_DIRS = [
    "/usr/share/fonts",
    "/usr/local/share/fonts",
    "~/.fonts",
    "~/.local/share/fonts",
    "~/.local/share/texmf/fonts",
]
MACOS_FONT_DIRS = ["/Library/Fonts/", "/System/Library/Fonts/"]
FONT_DIRECTORIES = {
    WINDOWS: WIN_FONT_DIRS,
    LINUX: LINUX_FONT_DIRS,
    MACOS: MACOS_FONT_DIRS,
}

DEFAULT_FONTS = [
    "ArialUni.ttf",  # for Windows
    "Arial Unicode.ttf",  # for macOS
    "Arial.ttf",  # for the case  "Arial Unicode" does not exist
    "DejaVuSansCondensed.ttf",  # widths of glyphs is similar to Arial
    "DejaVuSans.ttf",
    "LiberationSans-Regular.ttf",
    "OpenSans-Regular.ttf",
]
CURRENT_CACHE_VERSION = 2


class CacheEntry(NamedTuple):
    file_path: Path  # full file path e.g. "C:\Windows\Fonts\DejaVuSans.ttf"
    font_face: FontFace


GENERIC_FONT_FAMILY = {
    "serif": "DejaVu Serif",
    "sans-serif": "DejaVu Sans",
    "monospace": "DejaVu Sans Mono",
}


class FontCache:
    def __init__(self) -> None:
        # cache key is the lowercase ttf font name without parent directories
        # e.g. "arial.ttf" for "C:\Windows\Fonts\Arial.ttf"
        self._cache: dict[str, CacheEntry] = dict()

    def __contains__(self, font_name: str) -> bool:
        return self.key(font_name) in self._cache

    def __getitem__(self, item: str) -> CacheEntry:
        return self._cache[self.key(item)]

    def __setitem__(self, item: str, entry: CacheEntry) -> None:
        self._cache[self.key(item)] = entry

    def __len__(self):
        return len(self._cache)

    def clear(self) -> None:
        self._cache.clear()

    @staticmethod
    def key(font_name: str) -> str:
        return str(font_name).lower()

    def add_entry(self, font_path: Path, font_face: FontFace) -> None:
        self._cache[self.key(font_path.name)] = CacheEntry(font_path, font_face)

    def get(self, font_name: str, fallback: str) -> CacheEntry:
        try:
            return self._cache[self.key(font_name)]
        except KeyError:
            entry = self._cache.get(self.key(fallback))
            if entry is not None:
                return entry
            else:  # no fallback font available
                raise FontNotFoundError("no fonts available, not even fallback fonts")

    def find_best_match(self, font_face: FontFace) -> Optional[FontFace]:
        entry = self._cache.get(self.key(font_face.filename), None)
        if entry:
            return entry.font_face
        return self.find_best_match_ex(
            family=font_face.family,
            style=font_face.style,
            weight=font_face.weight,
            width=font_face.width,
            italic=font_face.is_italic,
        )

    def find_best_match_ex(
        self,
        family: str = "sans-serif",
        style: str = "Regular",
        weight: int = 400,
        width: int = 5,
        italic: Optional[bool] = False,
    ) -> Optional[FontFace]:
        # italic == None ... ignore italic flag
        family = GENERIC_FONT_FAMILY.get(family, family)
        entries = filter_family(family, self._cache.values())
        if len(entries) == 0:
            return None
        elif len(entries) == 1:
            return entries[0].font_face
        entries_ = filter_style(style, entries)
        if len(entries_) == 1:
            return entries_[0].font_face
        elif len(entries_):
            entries = entries_
        # best match by weight, italic, width
        # Note: the width property is used to prioritize shapefile types:
        # 1st .shx; 2nd: .shp; 3rd: .lff
        result = sorted(
            entries,
            key=lambda e: (
                abs(e.font_face.weight - weight),
                e.font_face.is_italic is not italic,
                abs(e.font_face.width - width),
            ),
        )
        return result[0].font_face

    def loads(self, s: str) -> None:
        cache: dict[str, CacheEntry] = dict()
        try:
            content = json.loads(s)
        except json.JSONDecodeError:
            raise IOError("invalid JSON file format")
        try:
            version = content["version"]
            content = content["font-faces"]
        except KeyError:
            raise IOError("invalid cache file format")
        if version == CURRENT_CACHE_VERSION:
            for entry in content:
                try:
                    file_path, family, style, weight, width = entry
                except ValueError:
                    raise IOError("invalid cache file format")
                path = Path(file_path)  # full path, e.g. "C:\Windows\Fonts\Arial.ttf"
                font_face = FontFace(
                    filename=path.name,  # file name without parent dirs, e.g. "Arial.ttf"
                    family=family,  # Arial
                    style=style,  # Regular
                    weight=weight,  # 400 (Normal)
                    width=width,  # 5 (Normal)
                )
                cache[self.key(path.name)] = CacheEntry(path, font_face)
        else:
            raise IOError("invalid cache file version")
        self._cache = cache

    def dumps(self) -> str:
        faces = [
            (
                str(entry.file_path),
                entry.font_face.family,
                entry.font_face.style,
                entry.font_face.weight,
                entry.font_face.width,
            )
            for entry in self._cache.values()
        ]
        data = {"version": CURRENT_CACHE_VERSION, "font-faces": faces}
        return json.dumps(data, indent=2)

    def print_available_fonts(self, verbose=False) -> None:
        for entry in self._cache.values():
            print(f"{entry.file_path}")
            if not verbose:
                continue
            font_type = entry.file_path.suffix.lower()
            ff = entry.font_face
            if font_type in (".shx", ".shp"):
                print(f"  Shape font file: '{ff.filename}'")
            elif font_type == ".lff":
                print(f"  LibreCAD font file: '{ff.filename}'")
            else:
                print(f"  TrueType/OpenType font file: '{ff.filename}'")
                print(f"  family: {ff.family}")
                print(f"  style: {ff.style}")
                print(f"  weight: {ff.weight}, {ff.weight_str}")
                print(f"  width: {ff.width}, {ff.width_str}")
        print(f"\nfound {len(self._cache)} fonts")


def filter_family(family: str, entries: Iterable[CacheEntry]) -> list[CacheEntry]:
    key = str(family).lower()
    return [e for e in entries if e.font_face.family.lower().startswith(key)]


def filter_style(style: str, entries: Iterable[CacheEntry]) -> list[CacheEntry]:
    key = str(style).lower()
    return [e for e in entries if key in e.font_face.style.lower()]


# TrueType and OpenType fonts:
# Note: CAD applications like AutoCAD/BricsCAD do not support OpenType fonts!
SUPPORTED_TTF_TYPES = {".ttf", ".ttc", ".otf"}
# Basic stroke-fonts included in CAD applications:
SUPPORTED_SHAPE_FILES = {".shx", ".shp", ".lff"}
NO_FONT_FACE = FontFace()
FALLBACK_SHAPE_FILES = ["txt.shx", "txt.shp", "iso.shx", "iso.shp"]
FALLBACK_LFF = ["standard.lff", "iso.lff", "simplex.lff"]


class FontNotFoundError(Exception):
    pass


class UnsupportedFont(Exception):
    pass


class FontManager:
    def __init__(self) -> None:
        self.platform = platform.system()
        self._font_cache: FontCache = FontCache()
        self._match_cache: dict[int, Optional[FontFace]] = dict()
        self._loaded_ttf_fonts: dict[str, TTFont] = dict()
        self._loaded_shape_file_glyph_caches: dict[str, shapefile.GlyphCache] = dict()
        self._loaded_lff_glyph_caches: dict[str, lff.GlyphCache] = dict()
        self._fallback_font_name = ""
        self._fallback_shape_file = ""
        self._fallback_lff = ""

    def print_available_fonts(self, verbose=False) -> None:
        self._font_cache.print_available_fonts(verbose=verbose)

    def has_font(self, font_name: str) -> bool:
        return font_name in self._font_cache

    def clear(self) -> None:
        self._font_cache = FontCache()
        self._loaded_ttf_fonts.clear()
        self._fallback_font_name = ""

    def fallback_font_name(self) -> str:
        fallback_name = self._fallback_font_name
        if fallback_name:
            return fallback_name
        fallback_name = DEFAULT_FONTS[0]
        for name in DEFAULT_FONTS:
            try:
                cache_entry = self._font_cache.get(name, fallback_name)
                fallback_name = cache_entry.file_path.name
                break
            except FontNotFoundError:
                pass
        self._fallback_font_name = fallback_name
        return fallback_name

    def fallback_shapefile(self) -> str:
        fallback_shape_file = self._fallback_shape_file
        if fallback_shape_file:
            return fallback_shape_file

        for name in FALLBACK_SHAPE_FILES:
            if name in self._font_cache:
                self._fallback_shape_file = name
                return name
        return ""

    def fallback_lff(self) -> str:
        fallback_lff = self._fallback_lff
        if fallback_lff:
            return fallback_lff

        for name in FALLBACK_SHAPE_FILES:
            if name in self._font_cache:
                self._fallback_shape_file = name
                return name
        return ""

    def get_ttf_font(self, font_name: str, font_number: int = 0) -> TTFont:
        try:
            return self._loaded_ttf_fonts[font_name]
        except KeyError:
            pass
        fallback_name = self.fallback_font_name()
        try:
            font = TTFont(
                self._font_cache.get(font_name, fallback_name).file_path,
                fontNumber=font_number,
            )
        except IOError as e:
            raise FontNotFoundError(str(e))
        except TTLibError as e:
            raise FontNotFoundError(str(e))
        self._loaded_ttf_fonts[font_name] = font
        return font

    def ttf_font_from_font_face(self, font_face: FontFace) -> TTFont:
        return self.get_ttf_font(Path(font_face.filename).name)

    def get_shapefile_glyph_cache(self, font_name: str) -> shapefile.GlyphCache:
        try:
            return self._loaded_shape_file_glyph_caches[font_name]
        except KeyError:
            pass
        fallback_name = self.fallback_shapefile()
        try:
            file_path = self._font_cache.get(font_name, fallback_name).file_path
        except KeyError:
            raise FontNotFoundError(f"shape font '{font_name}' not found")
        try:
            file = shapefile.readfile(str(file_path))
        except IOError:
            raise FontNotFoundError(f"shape file '{file_path}' not found")
        except shapefile.UnsupportedShapeFile as e:
            raise UnsupportedFont(f"unsupported font '{file_path}': {str(e)}")
        try:
            glyph_cache = shapefile.GlyphCache(file)
        except Exception:
            raise UnsupportedFont(f"can't create glyph-cache for font '{file_path}'.")
        self._loaded_shape_file_glyph_caches[font_name] = glyph_cache
        return glyph_cache

    def get_lff_glyph_cache(self, font_name: str) -> lff.GlyphCache:
        try:
            return self._loaded_lff_glyph_caches[font_name]
        except KeyError:
            pass
        fallback_name = self.fallback_lff()
        try:
            file_path = self._font_cache.get(font_name, fallback_name).file_path
        except KeyError:
            raise FontNotFoundError(f"LibreCAD font '{font_name}' not found")
        try:
            s = pathlib.Path(file_path).read_text(encoding="utf8")
            font = lff.loads(s)
        except IOError:
            raise FontNotFoundError(f"LibreCAD font file '{file_path}' not found")
        try:
            glyph_cache = lff.GlyphCache(font)
        except Exception:
            raise UnsupportedFont(f"can't create glyph-cache for font '{file_path}'.")

        self._loaded_lff_glyph_caches[font_name] = glyph_cache
        return glyph_cache

    def get_font_face(self, font_name: str) -> FontFace:
        cache_entry = self._font_cache.get(font_name, self.fallback_font_name())
        return cache_entry.font_face

    def find_best_match(
        self,
        family: str = "sans-serif",
        style: str = "Regular",
        weight=400,
        width=5,
        italic: Optional[bool] = False,
    ) -> Optional[FontFace]:
        key = hash((family, style, weight, width, italic))
        try:
            return self._match_cache[key]
        except KeyError:
            pass
        font_face = self._font_cache.find_best_match_ex(
            family, style, weight, width, italic
        )
        self._match_cache[key] = font_face
        return font_face

    def find_font_name(self, font_face: FontFace) -> str:
        """Returns the font file name of the font without parent directories
        e.g. "LiberationSans-Regular.ttf".
        """
        font_face = self._font_cache.find_best_match(font_face)  # type: ignore
        if font_face is None:
            font_face = self.get_font_face(self.fallback_font_name())
            return font_face.filename
        else:
            return font_face.filename

    def build(self, folders: Optional[Sequence[str]] = None, support_dirs=True) -> None:
        """Adds all supported font types located in the given `folders` to the font
        manager. If no directories are specified, the known font folders for Windows,
        Linux and macOS are searched by default, except `support_dirs` is ``False``.
        Searches recursively all subdirectories.

        The folders stored in the config SUPPORT_DIRS option are scanned recursively for
        .shx, .shp and .lff fonts, the basic stroke fonts included in CAD applications.

        """
        from ezdxf._options import options

        if folders:
            dirs = list(folders)
        else:
            dirs = FONT_DIRECTORIES.get(self.platform, LINUX_FONT_DIRS)
        if support_dirs:
            dirs = dirs + list(options.support_dirs)
        self.scan_all(dirs)

    def add_synonyms(self, synonyms: dict[str, str], reverse=True) -> None:
        font_cache = self._font_cache
        for font_name, synonym in synonyms.items():
            if not font_name in font_cache:
                continue
            if synonym in font_cache:
                continue
            cache_entry = font_cache[font_name]
            font_cache[synonym] = cache_entry
        if reverse:
            self.add_synonyms({v: k for k, v in synonyms.items()}, reverse=False)

    def scan_all(self, folders: Iterable[str]) -> None:
        for folder in folders:
            folder = folder.strip("'\"")  # strip quotes
            if not folder:
                continue
            try:
                self.scan_folder(Path(folder).expanduser())
            except PermissionError as e:
                print(str(e))
                continue

    def scan_folder(self, folder: Path):
        if not folder.exists():
            return
        for file in folder.iterdir():
            if file.is_dir():
                self.scan_folder(file)
                continue
            ext = file.suffix.lower()
            if ext in SUPPORTED_TTF_TYPES:
                try:
                    font_face = get_ttf_font_face(file)
                except Exception as e:
                    logger.warning(f"cannot open font '{file}': {str(e)}")
                else:
                    self._font_cache.add_entry(file, font_face)
            elif ext in SUPPORTED_SHAPE_FILES:
                font_face = get_shape_file_font_face(file)
                self._font_cache.add_entry(file, font_face)

    def dumps(self) -> str:
        return self._font_cache.dumps()

    def loads(self, s: str) -> None:
        self._font_cache.loads(s)


def normalize_style(style: str) -> str:
    if style in {"Book"}:
        style = "Regular"
    return style


def get_ttf_font_face(font_path: Path) -> FontFace:
    """The caller should catch ALL exception (see scan_folder function above) - strange
    things can happen when reading TTF files.
    """
    ttf = TTFont(font_path, fontNumber=0)
    names = ttf["name"].names
    family = ""
    style = ""
    for record in names:
        if record.nameID == 1:
            family = record.string.decode(record.getEncoding())
        elif record.nameID == 2:
            style = record.string.decode(record.getEncoding())
        if family and style:
            break

    try:
        os2_table = ttf["OS/2"]
    except Exception:  # e.g. ComickBook_Simple.ttf has an invalid "OS/2" table
        logger.info(f"cannot load OS/2 table of font '{font_path.name}'")
        weight = 400
        width = 5
    else:
        weight = os2_table.usWeightClass
        width = os2_table.usWidthClass
    return FontFace(
        filename=font_path.name,
        family=family,
        style=normalize_style(style),
        width=width,
        weight=weight,
    )


def get_shape_file_font_face(font_path: Path) -> FontFace:
    ext = font_path.suffix.lower()
    # Note: the width property is not defined in shapefiles and is used to
    # prioritize the shapefile types for find_best_match():
    # 1st .shx; 2nd: .shp; 3rd: .lff

    width = 5
    if ext == ".shp":
        width = 6
    if ext == ".lff":
        width = 7

    return FontFace(
        filename=font_path.name,  # "txt.shx", "simplex.shx", ...
        family=font_path.stem.lower(),  # "txt", "simplex", ...
        style=font_path.suffix.lower(),  # ".shx", ".shp" or ".lff"
        width=width,
        weight=400,
    )
