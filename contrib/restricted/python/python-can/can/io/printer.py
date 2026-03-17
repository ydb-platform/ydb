"""
This Listener simply prints to stdout / the terminal or a file.
"""

import logging
import sys
from io import TextIOWrapper
from typing import Any, TextIO, Union

from ..message import Message
from ..typechecking import StringPathLike
from .generic import TextIOMessageWriter

log = logging.getLogger("can.io.printer")


class Printer(TextIOMessageWriter):
    """
    The Printer class is a subclass of :class:`~can.Listener` which simply prints
    any messages it receives to the terminal (stdout). A message is turned into a
    string using :meth:`~can.Message.__str__`.

    :attr write_to_file: `True` if this instance prints to a file instead of
                         standard out
    """

    def __init__(
        self,
        file: Union[StringPathLike, TextIO, TextIOWrapper] = sys.stdout,
        append: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        :param file: An optional path-like object or a file-like object to "print"
                     to instead of writing to standard out (stdout).
                     If this is a file-like object, it has to be opened in text
                     write mode, not binary write mode.
        :param append: If set to `True` messages, are appended to the file,
                       else the file is truncated
        """
        super().__init__(file, mode="a" if append else "w")

    def on_message_received(self, msg: Message) -> None:
        self.file.write(str(msg) + "\n")

    def file_size(self) -> int:
        """Return an estimate of the current file size in bytes."""
        if self.file is not sys.stdout:
            return self.file.tell()
        return 0

    def stop(self) -> None:
        if self.file is not sys.stdout:
            super().stop()
