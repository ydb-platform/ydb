from dataclasses import dataclass
from io import BytesIO
from typing import List, Optional

from pyhanko.pdf_utils import generic

from ..writer import BasePdfFileWriter
from .api import FontEngine, FontEngineFactory, ShapeResult

pdf_name = generic.NameObject

__all__ = [
    'SimpleFontEngineFactory',
    'SimpleFontEngine',
    'SimpleFontMeta',
    'get_courier',
]


@dataclass(frozen=True)
class SimpleFontMeta:
    first_char: int
    last_char: int
    widths: List[int]
    descriptor: generic.DictionaryObject


COURIER_META = SimpleFontMeta(
    # only define one width, let everything default to MissingWidth
    first_char=32,
    last_char=32,
    widths=[600],
    descriptor=generic.DictionaryObject(
        {
            pdf_name('/Type'): pdf_name('/FontDescriptor'),
            pdf_name('/FontName'): pdf_name('/Courier'),
            # set fixed pitch, serif and nonsymbolic
            pdf_name('/Flags'): generic.NumberObject(0b100011),
            pdf_name('/FontBBox'): generic.ArrayObject(
                map(generic.NumberObject, [-23, -250, 715, 805])
            ),
            pdf_name('/Ascent'): generic.NumberObject(629),
            pdf_name('/Descent'): generic.NumberObject(-157),
            pdf_name('/CapHeight'): generic.NumberObject(629),
            pdf_name('/ItalicAngle'): generic.NumberObject(0),
            pdf_name('/StemV'): generic.NumberObject(51),
            pdf_name('/MissingWidth'): generic.NumberObject(600),
            pdf_name('/AvgWidth'): generic.NumberObject(600),
            pdf_name('/MaxWidth'): generic.NumberObject(600),
        }
    ),
)


class SimpleFontEngine(FontEngine):
    """
    Simplistic font engine that effectively only works with PDF standard fonts,
    and does not care about font metrics. Best used with monospaced fonts such
    as Courier.
    """

    @property
    def uses_complex_positioning(self):
        return False

    def __init__(
        self,
        writer: BasePdfFileWriter,
        name: str,
        avg_width: float,
        meta: Optional[SimpleFontMeta] = None,
    ):
        self.avg_width = avg_width
        self.name = name
        self.meta = meta
        super().__init__(writer, name, embedded_subset=False)

    def shape(self, txt) -> ShapeResult:
        ops = BytesIO()
        generic.TextStringObject(txt).write_to_stream(ops)
        ops.write(b" Tj")
        total_len = len(txt) * self.avg_width

        return ShapeResult(
            graphics_ops=ops.getvalue(), x_advance=total_len, y_advance=0
        )

    def as_resource(self):
        font_dict = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/Font'),
                pdf_name('/BaseFont'): pdf_name('/' + self.name),
                pdf_name('/Subtype'): pdf_name('/Type1'),
                pdf_name('/Encoding'): pdf_name('/WinAnsiEncoding'),
            }
        )
        # TODO maybe we could be a bit more clever to avoid duplication
        #  (cf. how GlyphAccumulator does it)
        meta = self.meta
        if meta is not None:
            font_dict['/FirstChar'] = generic.NumberObject(meta.first_char)
            font_dict['/LastChar'] = generic.NumberObject(meta.last_char)
            font_dict['/Widths'] = generic.ArrayObject(
                map(generic.NumberObject, meta.widths)
            )
            font_dict['/FontDescriptor'] = self.writer.add_object(
                generic.DictionaryObject(meta.descriptor)
            )

        return font_dict


class SimpleFontEngineFactory(FontEngineFactory):
    def __init__(
        self, name: str, avg_width: float, meta: Optional[SimpleFontMeta] = None
    ):
        self.avg_width = avg_width
        self.name = name
        self.meta = meta

    def create_font_engine(self, writer: BasePdfFileWriter, obj_stream=None):
        return SimpleFontEngine(writer, self.name, self.avg_width, self.meta)

    @staticmethod
    def default_factory():
        """
        :return:
            A :class:`.FontEngineFactory` instance representing the Courier
            standard font.
        """
        return SimpleFontEngineFactory('Courier', 0.6, COURIER_META)


def get_courier(pdf_writer: BasePdfFileWriter):
    """
    Quick-and-dirty way to obtain a Courier font resource.

    :param pdf_writer:
        A PDF writer.
    :return:
        A resource dictionary representing the standard Courier font
        (or one of its metric equivalents).
    """

    return (
        SimpleFontEngineFactory.default_factory()
        .create_font_engine(pdf_writer)
        .as_resource()
    )
