try:
    import barcode
    from barcode.writer import BaseWriter, mm2px
except ImportError as e:  # pragma: nocover
    raise ImportError(
        "pyhanko.pdf_utils.barcodes requires pyHanko to be installed with "
        "the [image-support] option. You can install missing "
        "dependencies by running "
        "\"pip install 'pyHanko[image-support]'\".",
        e,
    )

from pyhanko.pdf_utils.content import PdfContent
from pyhanko.pdf_utils.layout import BoxConstraints
from pyhanko.pdf_utils.misc import rd

__all__ = ['BarcodeBox', 'PdfStreamBarcodeWriter']


def barcode_colour_to_pdf(colour) -> bytes:
    # TODO there has to be an index of common colour names somewhere, use
    #  that instead.
    # default to black
    return b"1 1 1" if colour == "white" else b"0 0 0"


# convert mm to PDF user units
# 72 PDF user units to an inch is the standard
PDF_UUPI = 72


# ...so one "pixel" in python-barcode is one PDF user unit
def mm2uu(mm_len):
    return rd((mm_len / 25.4) * PDF_UUPI)


class PdfStreamBarcodeWriter(BaseWriter):
    """
    Implementation of writer class for the python-barcode library to output
    PDF graphics operators.
    Note: _paint_text is intentionally dummied out.
    Please use the functionality implemented in pyhanko.pdf_utils.text instead.
    """

    def __init__(self):
        def dummy(*_args, **_kwargs):
            pass

        # The architecture of the BaseWriter class is a little bizarre IMO.
        # It's not clear to me why the author didn't just put in some abstract
        # methods instead, but let's roll with it.
        BaseWriter.__init__(
            self, self._init, self._paint_module, dummy, self._finish
        )
        self._command_stream = None

    def _init(self, code):
        width, height = self.calculate_size(len(code[0]), len(code))
        self.size = (int(mm2px(width, PDF_UUPI)), int(mm2px(height, PDF_UUPI)))
        self._command_stream = [b'q']

    def _paint_module(self, xpos, ypos, width, color):
        self._command_stream.append(
            b'%s rg %g %g %g %g re f'
            % (
                barcode_colour_to_pdf(color),
                mm2uu(xpos),
                mm2uu(ypos),
                mm2uu(width),
                mm2uu(self.module_height),
            )
        )

    def _finish(self) -> bytes:
        self._command_stream.append(b'Q')
        return self.command_stream

    @property
    def command_stream(self) -> bytes:
        return b'\n'.join(self._command_stream)

    # we only define this method because python-barcode  demands it
    def save(self, filename, output):  # pragma: nocover
        pass


class BarcodeBox(PdfContent):
    """
    Thin wrapper around python-barcode functionality.

    This will render a barcode of the specified type as PDF graphics operators.
    """

    def __init__(self, barcode_type, code):
        self.barcode_type = barcode_type
        self.code = code

        writer = PdfStreamBarcodeWriter()

        # render everything here, since we need part of the rendering
        # operation's results to determine the box parameters to pass to
        # the parent
        b = barcode.get_barcode(self.barcode_type, code=code, writer=writer)
        self._barcode_commands = b.render()
        (w, h) = writer.size
        super().__init__(box=BoxConstraints(width=w, height=h))

    def render(self) -> bytes:
        return self._barcode_commands
