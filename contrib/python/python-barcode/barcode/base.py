"""barcode.base

"""
from __future__ import annotations

from typing import TYPE_CHECKING
from typing import ClassVar

from barcode.writer import BaseWriter
from barcode.writer import SVGWriter

if TYPE_CHECKING:
    from typing import BinaryIO


class Barcode:
    name = ""

    digits = 0

    default_writer = SVGWriter

    default_writer_options: ClassVar[dict] = {
        "module_width": 0.2,
        "module_height": 15.0,
        "quiet_zone": 6.5,
        "font_size": 10,
        "text_distance": 5.0,
        "background": "white",
        "foreground": "black",
        "write_text": True,
        "text": "",
    }

    writer: BaseWriter

    def __init__(self, code: str, writer: BaseWriter | None = None, **options) -> None:
        raise NotImplementedError

    def to_ascii(self) -> str:
        code_list = self.build()
        if not len(code_list) == 1:
            raise RuntimeError("Code list must contain a single element.")
        code = code_list[0]
        return code.replace("1", "X").replace("0", " ")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.get_fullcode()!r})>"

    def build(self) -> list[str]:
        """Return a single-element list with a string encoding the barcode.

        Typically the string consists of 1s and 0s, although it can contain
        other characters such as G for guard lines (e.g. in EAN13)."""
        raise NotImplementedError

    def get_fullcode(self):
        """Returns the full code, encoded in the barcode.

        :returns: Full human readable code.
        :rtype: String
        """
        raise NotImplementedError

    def save(
        self, filename: str, options: dict | None = None, text: str | None = None
    ) -> str:
        """Renders the barcode and saves it in `filename`.

        :param filename: Filename to save the barcode in (without filename extension).
        :param options: The same as in `self.render`.
        :param text: Text to render under the barcode.

        :returns: The full filename with extension.
        """
        output = self.render(options, text) if text else self.render(options)

        return self.writer.save(filename, output)

    def write(
        self,
        fp: BinaryIO,
        options: dict | None = None,
        text: str | None = None,
    ) -> None:
        """Renders the barcode and writes it to the file like object
        `fp`.

        :param fp: Object to write the raw data in.
        :param options: The same as in `self.render`.
        :param text: Text to render under the barcode.
        """
        output = self.render(options, text)
        self.writer.write(output, fp)

    def render(self, writer_options: dict | None = None, text: str | None = None):
        """Renders the barcode using `self.writer`.

        :param writer_options: Options for `self.writer`, see writer docs for details.
        :param text: Text to render under the barcode.

        :returns: Output of the writers render method.
        """
        options = self.default_writer_options.copy()
        options.update(writer_options or {})
        if options["write_text"] or text is not None:
            if text is not None:
                options["text"] = text
            else:
                options["text"] = self.get_fullcode()
        self.writer.set_options(options)
        code_list = self.build()
        if not len(code_list) == 1:
            raise RuntimeError("Code list must contain a single element.")
        code = code_list[0]
        return self.writer.render([code])
