from dataclasses import dataclass, field
from typing import Dict, Optional

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.writer import BasePdfFileWriter

__all__ = [
    'ShapeResult',
    'FontEngine',
    'FontSubsetCollection',
    'FontEngineFactory',
]

ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'


def generate_subset_prefix():
    import random

    return ''.join(ALPHABET[random.randint(0, 25)] for _ in range(6))


@dataclass(frozen=True)
class FontSubsetCollection:
    base_postscript_name: str
    """
    Base postscript name of the font.
    """

    subsets: Dict[Optional[str], 'FontEngine'] = field(default_factory=dict)
    """
    Dictionary mapping prefixes to subsets. ``None`` represents the full font.
    """

    def add_subset(self) -> str:
        while True:
            prefix = generate_subset_prefix()
            if prefix not in self.subsets:
                return prefix


@dataclass(frozen=True)
class ShapeResult:
    """Result of shaping a Unicode string."""

    graphics_ops: bytes
    """
    PDF graphics operators to render the glyphs.
    """

    x_advance: float
    """Total horizontal advance in em units."""

    y_advance: float
    """Total vertical advance in em units."""


class FontEngine:
    """General interface for text shaping and font metrics."""

    def __init__(
        self,
        writer: BasePdfFileWriter,
        base_postscript_name: str,
        embedded_subset: bool,
        obj_stream=None,
    ):
        fsc = writer.get_subset_collection(base_postscript_name)
        if embedded_subset:
            self.subset_prefix = prefix = fsc.add_subset()
        else:
            self.subset_prefix = prefix = None
        fsc.subsets[prefix] = self
        self.writer = writer
        self.obj_stream = obj_stream

    @property
    def uses_complex_positioning(self):
        """
        If ``True``, this font engine expects the line matrix to always be equal
        to the text matrix when exiting and entering :meth:`shape`.
        In other words, the current text position is where ``0 0 Td`` would
        move to.

        If ``False``, this method does not use any text positioning operators,
        and therefore uses the PDF standard's 'natural' positioning rules
        for text showing operators.

        The default is ``True`` unless overridden.
        """
        return True

    def shape(self, txt: str) -> ShapeResult:
        """Render a string to a format suitable for inclusion in a content
        stream and measure its total cursor advancement vector in em units.

        :param txt:
            String to shape.
        :return:
            A shaping result.
        """
        raise NotImplementedError

    def as_resource(self) -> generic.PdfObject:
        """Convert a :class:`.FontEngine` to a PDF object suitable for embedding
        inside a resource dictionary.

        .. note::
            If the PDF object is an indirect reference, the caller must not
            attempt to dereference it. In other words, implementations can
            use preallocated references to delay subsetting until the last
            possible moment (this is even encouraged, see
            :meth:`prepare_write`).

        :return:
            A PDF dictionary.
        """
        raise NotImplementedError

    def prepare_write(self):
        """
        Called by the writer that manages this font resource before the PDF
        content is written to a stream.

        Subsetting operations and the like should be carried out as part of
        this method.
        """
        pass


class FontEngineFactory:
    def create_font_engine(
        self, writer: 'BasePdfFileWriter', obj_stream=None
    ) -> FontEngine:
        raise NotImplementedError
