"""Font management utilities for converting SVG to ReportLab graphics.

This module provides font mapping and registration functionality for converting
SVG fonts to ReportLab-compatible fonts. It handles font discovery, registration,
and mapping between SVG font specifications and ReportLab font names.

The module includes:
- FontMap class for managing font mappings
- Font discovery using system fontconfig
- Support for standard PDF fonts
- Automatic font file detection and registration
"""

import os
import subprocess
import sys
from typing import Dict, Optional, Tuple, Union

from reportlab.pdfbase.pdfmetrics import registerFont
from reportlab.pdfbase.ttfonts import TTFError, TTFont

STANDARD_FONT_NAMES = (
    "Times-Roman",
    "Times-Italic",
    "Times-Bold",
    "Times-BoldItalic",
    "Helvetica",
    "Helvetica-Oblique",
    "Helvetica-Bold",
    "Helvetica-BoldOblique",
    "Courier",
    "Courier-Oblique",
    "Courier-Bold",
    "Courier-BoldOblique",
    "Symbol",
    "ZapfDingbats",
)
DEFAULT_FONT_NAME = "Helvetica"
DEFAULT_FONT_WEIGHT = "normal"
DEFAULT_FONT_STYLE = "normal"
DEFAULT_FONT_SIZE = 12


class FontMap:
    """Manages mapping of SVG font names to ReportLab fonts and handles registration.

    This class provides a centralized way to map SVG font specifications (family,
    weight, style) to ReportLab-compatible font names. It supports automatic font
    discovery, registration of custom fonts, and fallback to standard PDF fonts.

    The internal font map uses normalized font names as keys for efficient lookup
    and supports both exact and approximate font matching.
    """

    def __init__(self) -> None:
        """Initialize the FontMap with an empty font registry.

        Creates an empty internal font map and registers all default font mappings
        for standard PDF fonts and common font family aliases.

        The internal font map structure:
            'internal_name': {
                'svg_family': 'family_name',
                'svg_weight': 'font_weight',
                'svg_style': 'font_style',
                'rlgFont': 'reportlab_font_name',
                'exact': True/False
            }

        Internal names are normalized for efficient lookup and follow the pattern:
        'Family-WeightStyle' (e.g., 'Arial-BoldItalic').
        """
        self._map: Dict[str, Dict[str, Union[str, bool, int]]] = {}

        self.register_default_fonts()

    @staticmethod
    def build_internal_name(
        family: str, weight: str = "normal", style: str = "normal"
    ) -> str:
        """Build normalized internal font name from family, weight, and style.

        Creates a standardized font name for internal mapping by combining the
        font family with capitalized weight and style variants. This follows
        the standard naming convention used by most font systems.

        Args:
            family: Font family name (e.g., "Arial", "Times New Roman").
            weight: Font weight ("normal", "bold", or numeric value).
            style: Font style ("normal" or "italic").

        Returns:
            Normalized font name string (e.g., "Arial-BoldItalic").

        Examples:
            >>> FontMap.build_internal_name("Arial", "bold", "italic")
            'Arial-BoldItalic'
            >>> FontMap.build_internal_name("Times", "normal", "normal")
            'Times'
        """
        result_name = family
        if weight != "normal" or style != "normal":
            result_name += "-"
        if weight != "normal":
            if isinstance(weight, int):
                result_name += f"{weight}"
            else:
                result_name += weight.lower().capitalize()
        if style != "normal":
            result_name += style.lower().capitalize()
        return result_name

    @staticmethod
    def guess_font_filename(
        basename: str,
        weight: str = "normal",
        style: str = "normal",
        extension: str = "ttf",
    ) -> str:
        """Guess font filename based on family, weight, and style parameters.

        Attempts to construct a likely font filename using common naming conventions
        for TrueType fonts. This works well for standard system fonts on Windows and
        many Unix-like systems.

        Args:
            basename: Base font family name (e.g., "arial", "times").
            weight: Font weight ("normal" or "bold").
            style: Font style ("normal" or "italic").
            extension: File extension (default "ttf").

        Returns:
            Guessed filename with appropriate weight/style suffix.

        Examples:
            >>> FontMap.guess_font_filename("arial", "bold", "italic")
            'arialbi.ttf'
            >>> FontMap.guess_font_filename("times", "normal", "normal")
            'times.ttf'
        """
        prefix = ""
        is_bold = weight.lower() == "bold"
        is_italic = style.lower() == "italic"
        if is_bold and not is_italic:
            prefix = "bd"
        elif is_bold and is_italic:
            prefix = "bi"
        elif not is_bold and is_italic:
            prefix = "i"
        filename = f"{basename}{prefix}.{extension}"
        return filename

    def use_fontconfig(
        self, font_name: str, weight: str = "normal", style: str = "normal"
    ) -> Tuple[Optional[str], bool]:
        """Find and register a font using system fontconfig.

        Uses the system's fontconfig utility to locate and register fonts that
        match the given specifications. This provides access to system-installed
        fonts that aren't part of the standard PDF font set.

        Args:
            font_name: Name of the font family to search for.
            weight: Font weight specification ("normal" or "bold").
            style: Font style specification ("normal" or "italic").

        Returns:
            Tuple of (font_name, is_exact_match). Returns (None, False) if
            fontconfig is unavailable or no suitable font is found.

        Raises:
            OSError: If fontconfig command is not available on the system.

        Note:
            Fontconfig may return a default fallback font if the exact font
            is not found. The exact_match flag indicates whether the returned
            font is an exact match for the requested font.
        """
        NOT_FOUND = (None, False)
        # Searching with Fontconfig
        try:
            pipe = subprocess.Popen(
                ["fc-match", "-s", "--format=%{file}\\n", font_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            output = pipe.communicate()[0].decode(sys.getfilesystemencoding())
        except OSError:
            return NOT_FOUND
        font_paths = output.split("\n")
        for font_path in font_paths:
            try:
                registerFont(TTFont(font_name, font_path))
            except TTFError:
                continue
            else:
                success_font_path = font_path
                break
        else:
            return NOT_FOUND
        # Fontconfig may return a default font totally unrelated with font_name
        exact = font_name.lower() in os.path.basename(success_font_path).lower()
        internal_name = FontMap.build_internal_name(font_name, weight, style)
        self._map[internal_name] = {
            "svg_family": font_name,
            "svg_weight": weight,
            "svg_style": style,
            "rlgFont": font_name,
            "exact": exact,
        }
        return font_name, exact

    def register_default_fonts(self) -> None:
        """Register mappings for standard PDF fonts and common font families.

        Sets up the default font mappings that are always available in ReportLab.
        This includes the 14 standard PDF fonts and common font family aliases
        like "serif", "sans-serif", and "monospace" that map to appropriate
        standard fonts.

        This method is called automatically during FontMap initialization and
        establishes the baseline font support for SVG to PDF conversion.
        """
        self.register_font("Times New Roman", rlgFontName="Times-Roman")
        self.register_font("Times New Roman", weight="bold", rlgFontName="Times-Bold")
        self.register_font(
            "Times New Roman", style="italic", rlgFontName="Times-Italic"
        )
        self.register_font(
            "Times New Roman",
            weight="bold",
            style="italic",
            rlgFontName="Times-BoldItalic",
        )

        self.register_font("Helvetica", rlgFontName="Helvetica")
        self.register_font("Helvetica", weight="bold", rlgFontName="Helvetica-Bold")
        self.register_font("Helvetica", style="italic", rlgFontName="Helvetica-Oblique")
        self.register_font(
            "Helvetica",
            weight="bold",
            style="italic",
            rlgFontName="Helvetica-BoldOblique",
        )

        self.register_font("Courier New", rlgFontName="Courier")
        self.register_font("Courier New", weight="bold", rlgFontName="Courier-Bold")
        self.register_font("Courier New", style="italic", rlgFontName="Courier-Oblique")
        self.register_font(
            "Courier New",
            weight="bold",
            style="italic",
            rlgFontName="Courier-BoldOblique",
        )
        self.register_font("Courier", style="italic", rlgFontName="Courier-Oblique")
        self.register_font(
            "Courier", weight="bold", style="italic", rlgFontName="Courier-BoldOblique"
        )

        self.register_font("sans-serif", rlgFontName="Helvetica")
        self.register_font("sans-serif", weight="bold", rlgFontName="Helvetica-Bold")
        self.register_font(
            "sans-serif", style="italic", rlgFontName="Helvetica-Oblique"
        )
        self.register_font(
            "sans-serif",
            weight="bold",
            style="italic",
            rlgFontName="Helvetica-BoldOblique",
        )

        self.register_font("serif", rlgFontName="Times-Roman")
        self.register_font("serif", weight="bold", rlgFontName="Times-Bold")
        self.register_font("serif", style="italic", rlgFontName="Times-Italic")
        self.register_font(
            "serif", weight="bold", style="italic", rlgFontName="Times-BoldItalic"
        )

        self.register_font("times", rlgFontName="Times-Roman")
        self.register_font("times", weight="bold", rlgFontName="Times-Bold")
        self.register_font("times", style="italic", rlgFontName="Times-Italic")
        self.register_font(
            "times", weight="bold", style="italic", rlgFontName="Times-BoldItalic"
        )

        self.register_font("monospace", rlgFontName="Courier")
        self.register_font("monospace", weight="bold", rlgFontName="Courier-Bold")
        self.register_font("monospace", style="italic", rlgFontName="Courier-Oblique")
        self.register_font(
            "monospace",
            weight="bold",
            style="italic",
            rlgFontName="Courier-BoldOblique",
        )

    def register_font_family(
        self,
        family: str,
        normal: str,
        bold: Optional[str] = None,
        italic: Optional[str] = None,
        bolditalic: Optional[str] = None,
    ) -> None:
        """Register a complete font family with all style variants.

        Convenience method to register an entire font family at once by providing
        the font paths for different style combinations. This automatically creates
        mappings for normal, bold, italic, and bold-italic variants.

        Args:
            family: Font family name (e.g., "MyCustomFont").
            normal: Path or name for the normal weight/style variant.
            bold: Optional path or name for bold variant.
            italic: Optional path or name for italic variant.
            bolditalic: Optional path or name for bold-italic variant.

        Example:
            >>> font_map.register_font_family(
            ...     "MyFont",
            ...     "/path/to/myfont-regular.ttf",
            ...     "/path/to/myfont-bold.ttf",
            ...     "/path/to/myfont-italic.ttf",
            ...     "/path/to/myfont-bolditalic.ttf"
            ... )
        """
        self.register_font(family, normal)
        if bold is not None:
            self.register_font(family, bold, weight="bold")
        if italic is not None:
            self.register_font(family, italic, style="italic")
        if bolditalic is not None:
            self.register_font(family, bolditalic, weight="bold", style="italic")

    def register_font(
        self,
        font_family: str,
        font_path: Optional[str] = None,
        weight: str = "normal",
        style: str = "normal",
        rlgFontName: Optional[str] = None,
    ) -> Tuple[Optional[str], bool]:
        """Register a font or create a mapping to a ReportLab font name.

        This method handles two scenarios:
        1. Registering a custom TrueType font file with ReportLab
        2. Creating a mapping from SVG font specifications to existing ReportLab fonts

        For standard PDF fonts, only the mapping is created. For custom fonts,
        the font file is registered with ReportLab and then mapped.

        Args:
            font_family: SVG font family name (e.g., "Arial", "Times New Roman").
            font_path: Path to TrueType font file (.ttf). If None, assumes this
                is a mapping to an existing ReportLab font.
            weight: Font weight ("normal" or "bold").
            style: Font style ("normal" or "italic").
            rlgFontName: ReportLab font name to map to. If None, uses the
                normalized internal name.

        Returns:
            Tuple of (internal_font_name, success_flag). Returns (None, False)
            if registration fails.

        Raises:
            TTFError: If the font file cannot be loaded or registered.

        Examples:
            >>> # Map to existing ReportLab font
            >>> font_map.register_font("MyArial", rlgFontName="Helvetica")
            ('MyArial', True)

            >>> # Register custom font file
            >>> font_map.register_font("MyFont", "/path/to/font.ttf")
            ('MyFont', True)
        """
        NOT_FOUND = (None, False)
        internal_name = FontMap.build_internal_name(font_family, weight, style)
        if rlgFontName is None:
            # if no reportlabs font name is given, use the internal fontname to
            # register the reportlab font
            rlgFontName = internal_name

        if rlgFontName in STANDARD_FONT_NAMES:
            # mapping to one of the standard fonts, no need to register
            self._map[internal_name] = {
                "svg_family": font_family,
                "svg_weight": weight,
                "svg_style": style,
                "rlgFont": rlgFontName,
                "exact": True,
            }
            return internal_name, True

        if internal_name not in STANDARD_FONT_NAMES and font_path is not None:
            try:
                registerFont(TTFont(rlgFontName, font_path))
                self._map[internal_name] = {
                    "svg_family": font_family,
                    "svg_weight": weight,
                    "svg_style": style,
                    "rlgFont": rlgFontName,
                    "exact": True,
                }
                return internal_name, True
            except TTFError:
                return NOT_FOUND

        # If we reach here, no registration was possible
        return NOT_FOUND

    def find_font(
        self, font_name: str, weight: str = "normal", style: str = "normal"
    ) -> Tuple[str, bool]:
        """Find the best matching ReportLab font for given specifications.

        Searches through the font registry to find the most appropriate font match.
        Uses a multi-step fallback strategy: exact match, standard fonts, file-based
        registration, fontconfig discovery, and finally default fallback.

        Args:
            font_name: SVG font family name to search for.
            weight: Font weight ("normal" or "bold").
            style: Font style ("normal" or "italic").

        Returns:
            Tuple of (reportlab_font_name, is_exact_match). The exact_match flag
            indicates whether the returned font is an exact match for the request.

        Note:
            If no suitable font is found, falls back to DEFAULT_FONT_NAME (Helvetica).
            The search prioritizes exact matches over approximate ones.
        """
        internal_name = FontMap.build_internal_name(font_name, weight, style)
        # Step 1 check if the font is one of the buildin standard fonts
        if internal_name in STANDARD_FONT_NAMES:
            return internal_name, True
        # Step 2 Check if font is already registered
        if internal_name in self._map:
            font_entry = self._map[internal_name]
            return (
                str(font_entry["rlgFont"]),
                bool(font_entry["exact"]),
            )
        # Step 3 Try to auto register the font
        # Try first to register the font if it exists as ttf
        guessed_filename = FontMap.guess_font_filename(font_name, weight, style)
        reg_name, exact = self.register_font(font_name, guessed_filename)
        if reg_name is not None:
            return reg_name, exact
        fontconfig_result = self.use_fontconfig(font_name, weight, style)
        if fontconfig_result[0] is not None:
            return fontconfig_result[0], fontconfig_result[1]
        # Fallback to default font if nothing found
        return DEFAULT_FONT_NAME, False


_font_map = FontMap()  # the global font map


def register_font(
    font_name: str,
    font_path: Optional[str] = None,
    weight: str = "normal",
    style: str = "normal",
    rlgFontName: Optional[str] = None,
) -> Tuple[Optional[str], bool]:
    """Register a font with the global font map.

    Convenience function that delegates to the global FontMap instance.
    Registers a custom font or creates a mapping to an existing ReportLab font.

    Args:
        font_name: SVG font family name (e.g., "Arial", "Times New Roman").
        font_path: Path to TrueType font file (.ttf). Optional for mappings
            to existing fonts.
        weight: Font weight ("normal" or "bold").
        style: Font style ("normal" or "italic").
        rlgFontName: ReportLab font name to map to.

    Returns:
        Tuple of (internal_font_name, success_flag).

    See Also:
        FontMap.register_font: The underlying implementation method.
    """
    return _font_map.register_font(font_name, font_path, weight, style, rlgFontName)


def find_font(
    font_name: str, weight: str = "normal", style: str = "normal"
) -> Tuple[str, bool]:
    """Find the best matching font from the global font registry.

    Convenience function that delegates to the global FontMap instance.
    Searches for fonts using a multi-step fallback strategy.

    Args:
        font_name: SVG font family name to search for.
        weight: Font weight ("normal" or "bold").
        style: Font style ("normal" or "italic").

    Returns:
        Tuple of (reportlab_font_name, is_exact_match).

    See Also:
        FontMap.find_font: The underlying implementation method.
    """
    return _font_map.find_font(font_name, weight, style)


def register_font_family(
    family: str,
    normal: str,
    bold: Optional[str] = None,
    italic: Optional[str] = None,
    bolditalic: Optional[str] = None,
) -> None:
    """Register a complete font family with the global font map.

    Convenience function that delegates to the global FontMap instance.
    Registers an entire font family with all style variants at once.

    Args:
        family: Font family name (e.g., "MyCustomFont").
        normal: Path or name for the normal weight/style variant.
        bold: Optional path or name for bold variant.
        italic: Optional path or name for italic variant.
        bolditalic: Optional path or name for bold-italic variant.

    See Also:
        FontMap.register_font_family: The underlying implementation method.
    """
    _font_map.register_font_family(family, normal, bold, italic, bolditalic)


def get_global_font_map() -> FontMap:
    """Get the global FontMap instance used by the module.

    Returns the singleton FontMap instance that manages all font registrations
    and mappings for the svglib module. This is the same instance used by all
    module-level font functions.

    Returns:
        The global FontMap instance.

    Note:
        Direct access to the FontMap allows for advanced font management
        operations not available through the convenience functions.
    """
    return _font_map
