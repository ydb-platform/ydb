"""
See the :class:`Logger` class.
"""

import gzip
import os
import pathlib
from abc import ABC, abstractmethod
from datetime import datetime
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    Final,
    Literal,
    Optional,
)

from typing_extensions import Self

from .._entry_points import read_entry_points
from ..message import Message
from ..typechecking import StringPathLike
from .asc import ASCWriter
from .blf import BLFWriter
from .canutils import CanutilsLogWriter
from .csv import CSVWriter
from .generic import (
    BinaryIOMessageWriter,
    FileIOMessageWriter,
    MessageWriter,
    SizedMessageWriter,
    TextIOMessageWriter,
)
from .mf4 import MF4Writer
from .printer import Printer
from .sqlite import SqliteWriter
from .trc import TRCWriter

#: A map of file suffixes to their corresponding
#: :class:`can.io.generic.MessageWriter` class
MESSAGE_WRITERS: Final[dict[str, type[MessageWriter]]] = {
    ".asc": ASCWriter,
    ".blf": BLFWriter,
    ".csv": CSVWriter,
    ".db": SqliteWriter,
    ".log": CanutilsLogWriter,
    ".mf4": MF4Writer,
    ".trc": TRCWriter,
    ".txt": Printer,
}


def _update_writer_plugins() -> None:
    """Update available message writer plugins from entry points."""
    for entry_point in read_entry_points("can.io.message_writer"):
        if entry_point.key in MESSAGE_WRITERS:
            continue

        writer_class = entry_point.load()
        if issubclass(writer_class, MessageWriter):
            MESSAGE_WRITERS[entry_point.key] = writer_class


def _get_logger_for_suffix(suffix: str) -> type[MessageWriter]:
    try:
        return MESSAGE_WRITERS[suffix]
    except KeyError:
        raise ValueError(
            f'No write support for unknown log format "{suffix}"'
        ) from None


def _compress(filename: StringPathLike, **kwargs: Any) -> FileIOMessageWriter[Any]:
    """
    Return the suffix and io object of the decompressed file.
    File will automatically recompress upon close.
    """
    suffixes = pathlib.Path(filename).suffixes
    if len(suffixes) != 2:
        raise ValueError(
            f"No write support for unknown log format \"{''.join(suffixes)}\""
        ) from None

    real_suffix = suffixes[-2].lower()
    if real_suffix in (".blf", ".db"):
        raise ValueError(
            f"The file type {real_suffix} is currently incompatible with gzip."
        )
    logger_type = _get_logger_for_suffix(real_suffix)
    append = kwargs.get("append", False)

    if issubclass(logger_type, BinaryIOMessageWriter):
        return logger_type(
            file=gzip.open(filename=filename, mode="ab" if append else "wb"), **kwargs
        )

    elif issubclass(logger_type, TextIOMessageWriter):
        return logger_type(
            file=gzip.open(filename=filename, mode="at" if append else "wt"), **kwargs
        )

    raise ValueError(
        f"The file type {real_suffix} is currently incompatible with gzip."
    )


def Logger(  # noqa: N802
    filename: Optional[StringPathLike], **kwargs: Any
) -> MessageWriter:
    """Find and return the appropriate :class:`~can.io.generic.MessageWriter` instance
    for a given file suffix.

    The format is determined from the file suffix which can be one of:
      * .asc :class:`can.ASCWriter`
      * .blf :class:`can.BLFWriter`
      * .csv: :class:`can.CSVWriter`
      * .db :class:`can.SqliteWriter`
      * .log :class:`can.CanutilsLogWriter`
      * .mf4 :class:`can.MF4Writer`
        (optional, depends on `asammdf <https://github.com/danielhrisca/asammdf>`_)
      * .trc :class:`can.TRCWriter`
      * .txt :class:`can.Printer`

    Any of these formats can be used with gzip compression by appending
    the suffix .gz (e.g. filename.asc.gz). However, third-party tools might not
    be able to read these files.

    The **filename** may also be *None*, to fall back to :class:`can.Printer`.

    The log files may be incomplete until `stop()` is called due to buffering.

    :param filename:
        the filename/path of the file to write to,
        may be a path-like object or None to
        instantiate a :class:`~can.Printer`
    :raises ValueError:
        if the filename's suffix is of an unknown file type

    .. note::
        This function itself is just a dispatcher, and any positional and keyword
        arguments are passed on to the returned instance.
    """

    if filename is None:
        return Printer(**kwargs)

    _update_writer_plugins()

    suffix = pathlib.PurePath(filename).suffix.lower()
    if suffix == ".gz":
        return _compress(filename, **kwargs)

    logger_type = _get_logger_for_suffix(suffix)
    return logger_type(file=filename, **kwargs)


class BaseRotatingLogger(MessageWriter, ABC):
    """
    Base class for rotating CAN loggers. This class is not meant to be
    instantiated directly. Subclasses must implement the :meth:`should_rollover`
    and :meth:`do_rollover` methods according to their rotation strategy.

    The rotation behavior can be further customized by the user by setting
    the :attr:`namer` and :attr:`rotator` attributes after instantiating the subclass.

    These attributes as well as the methods :meth:`rotation_filename` and :meth:`rotate`
    and the corresponding docstrings are carried over from the python builtin
    :class:`~logging.handlers.BaseRotatingHandler`.

    Subclasses must set the `_writer` attribute upon initialization.
    """

    _supported_formats: ClassVar[set[str]] = set()

    #: If this attribute is set to a callable, the :meth:`~BaseRotatingLogger.rotation_filename`
    #: method delegates to this callable. The parameters passed to the callable are
    #: those passed to :meth:`~BaseRotatingLogger.rotation_filename`.
    namer: Optional[Callable[[StringPathLike], StringPathLike]] = None

    #: If this attribute is set to a callable, the :meth:`~BaseRotatingLogger.rotate` method
    #: delegates to this callable. The parameters passed to the callable are those
    #: passed to :meth:`~BaseRotatingLogger.rotate`.
    rotator: Optional[Callable[[StringPathLike, StringPathLike], None]] = None

    #: An integer counter to track the number of rollovers.
    rollover_count: int = 0

    def __init__(self, **kwargs: Any) -> None:
        self.writer_kwargs = kwargs

    @property
    @abstractmethod
    def writer(self) -> MessageWriter:
        """This attribute holds an instance of a writer class which manages the actual file IO."""
        raise NotImplementedError

    def rotation_filename(self, default_name: StringPathLike) -> StringPathLike:
        """Modify the filename of a log file when rotating.

        This is provided so that a custom filename can be provided.
        The default implementation calls the :attr:`namer` attribute of the
        handler, if it's callable, passing the default name to
        it. If the attribute isn't callable (the default is :obj:`None`), the name
        is returned unchanged.

        :param default_name:
            The default name for the log file.
        """
        if not callable(self.namer):
            return default_name

        return self.namer(default_name)  # pylint: disable=not-callable

    def rotate(self, source: StringPathLike, dest: StringPathLike) -> None:
        """When rotating, rotate the current log.

        The default implementation calls the :attr:`rotator` attribute of the
        handler, if it's callable, passing the `source` and `dest` arguments to
        it. If the attribute isn't callable (the default is :obj:`None`), the source
        is simply renamed to the destination.

        :param source:
            The source filename. This is normally the base
            filename, e.g. `"test.log"`
        :param dest:
            The destination filename. This is normally
            what the source is rotated to, e.g. `"test_#001.log"`.
        """
        if not callable(self.rotator):
            if os.path.exists(source):
                os.rename(source, dest)
        else:
            self.rotator(source, dest)  # pylint: disable=not-callable

    def on_message_received(self, msg: Message) -> None:
        """This method is called to handle the given message.

        :param msg:
            the delivered message
        """
        if self.should_rollover(msg):
            self.do_rollover()
            self.rollover_count += 1

        self.writer.on_message_received(msg)

    def _get_new_writer(self, filename: StringPathLike) -> MessageWriter:
        """Instantiate a new writer.

        .. note::
            The :attr:`self.writer` should be closed prior to calling this function.

        :param filename:
            Path-like object that specifies the location and name of the log file.
            The log file format is defined by the suffix of `filename`.
        :return:
            An instance of a writer class.
        """
        suffixes = pathlib.Path(filename).suffixes
        for suffix_length in range(len(suffixes), 0, -1):
            suffix = "".join(suffixes[-suffix_length:]).lower()
            if suffix not in self._supported_formats:
                continue
            logger = Logger(filename=filename, **self.writer_kwargs)
            return logger

        raise ValueError(
            f'The log format of "{pathlib.Path(filename).name}" '
            f"is not supported by {self.__class__.__name__}. "
            f"{self.__class__.__name__} supports the following formats: "
            f"{', '.join(self._supported_formats)}"
        )

    def stop(self) -> None:
        """Stop handling new messages.

        Carry out any final tasks to ensure
        data is persisted and cleanup any open resources.
        """
        self.writer.stop()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        self.stop()
        return False

    @abstractmethod
    def should_rollover(self, msg: Message) -> bool:
        """Determine if the rollover conditions are met."""

    @abstractmethod
    def do_rollover(self) -> None:
        """Perform rollover."""


class SizedRotatingLogger(BaseRotatingLogger):
    """Log CAN messages to a sequence of files with a given maximum size.

    The logger creates a log file with the given `base_filename`. When the
    size threshold is reached the current log file is closed and renamed
    by adding a timestamp and the rollover count. A new log file is then
    created and written to.

    This behavior can be customized by setting the
    :attr:`~can.io.BaseRotatingLogger.namer` and
    :attr:`~can.io.BaseRotatingLogger.rotator`
    attribute.

    Example::

        from can import Notifier, SizedRotatingLogger
        from can.interfaces.vector import VectorBus

        bus = VectorBus(channel=[0], app_name="CANape", fd=True)

        logger = SizedRotatingLogger(
            base_filename="my_logfile.asc",
            max_bytes=5 * 1024 ** 2,  # =5MB
        )
        logger.rollover_count = 23  # start counter at 23

        notifier = Notifier(bus=bus, listeners=[logger])

    The SizedRotatingLogger currently supports the formats
      * .asc: :class:`can.ASCWriter`
      * .blf :class:`can.BLFWriter`
      * .csv: :class:`can.CSVWriter`
      * .log :class:`can.CanutilsLogWriter`
      * .txt :class:`can.Printer` (if pointing to a file)

    .. note::
        The :class:`can.SqliteWriter` is not supported yet.

    The log files on disk may be incomplete due to buffering until
    :meth:`~can.Listener.stop` is called.
    """

    _supported_formats: ClassVar[set[str]] = {".asc", ".blf", ".csv", ".log", ".txt"}

    def __init__(
        self,
        base_filename: StringPathLike,
        max_bytes: int = 0,
        **kwargs: Any,
    ) -> None:
        """
        :param base_filename:
            A path-like object for the base filename. The log file format is defined by
            the suffix of `base_filename`.
        :param max_bytes:
            The size threshold at which a new log file shall be created. If set to 0, no
            rollover will be performed.
        """
        super().__init__(**kwargs)

        self.base_filename = os.path.abspath(base_filename)
        self.max_bytes = max_bytes

        self._writer = self._get_new_writer(self.base_filename)

    def _get_new_writer(self, filename: StringPathLike) -> SizedMessageWriter:
        writer = super()._get_new_writer(filename)
        if isinstance(writer, SizedMessageWriter):
            return writer
        raise TypeError

    @property
    def writer(self) -> SizedMessageWriter:
        return self._writer

    def should_rollover(self, msg: Message) -> bool:
        if self.max_bytes <= 0:
            return False

        file_size = self.writer.file_size()
        if file_size is None:
            return False

        return file_size >= self.max_bytes

    def do_rollover(self) -> None:
        if self.writer:
            self.writer.stop()

        sfn = self.base_filename
        dfn = self.rotation_filename(self._default_name())
        self.rotate(sfn, dfn)

        self._writer = self._get_new_writer(self.base_filename)

    def _default_name(self) -> StringPathLike:
        """Generate the default rotation filename."""
        path = pathlib.Path(self.base_filename)
        new_name = (
            path.stem.split(".")[0]
            + "_"
            + datetime.now().strftime("%Y-%m-%dT%H%M%S")
            + "_"
            + f"#{self.rollover_count:03}"
            + "".join(path.suffixes[-2:])
        )
        return str(path.parent / new_name)
