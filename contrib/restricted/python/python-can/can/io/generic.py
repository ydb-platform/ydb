"""This module provides abstract base classes for CAN message reading and writing operations
to various file formats.

.. note::
    All classes in this module are abstract and should be subclassed to implement
    specific file format handling.
"""

import locale
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from io import BufferedIOBase, TextIOWrapper
from pathlib import Path
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Generic,
    Literal,
    Optional,
    TextIO,
    TypeVar,
    Union,
)

from typing_extensions import Self

from ..listener import Listener
from ..message import Message
from ..typechecking import FileLike, StringPathLike

if TYPE_CHECKING:
    from _typeshed import (
        OpenBinaryModeReading,
        OpenBinaryModeUpdating,
        OpenBinaryModeWriting,
        OpenTextModeReading,
        OpenTextModeUpdating,
        OpenTextModeWriting,
    )


#: type parameter used in generic classes :class:`MessageReader` and :class:`MessageWriter`
_IoTypeVar = TypeVar("_IoTypeVar", bound=FileLike)


class MessageWriter(AbstractContextManager["MessageWriter"], Listener, ABC):
    """Abstract base class for all CAN message writers.

    This class serves as a foundation for implementing different message writer formats.
    It combines context manager capabilities with the message listener interface.

    :param file: Path-like object or string representing the output file location
    :param kwargs: Additional keyword arguments for specific writer implementations
    """

    @abstractmethod
    def __init__(self, file: StringPathLike, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop handling messages and cleanup any resources."""

    def __enter__(self) -> Self:
        """Enter the context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        """Exit the context manager and ensure proper cleanup."""
        self.stop()
        return False


class SizedMessageWriter(MessageWriter, ABC):
    """Abstract base class for message writers that can report their file size.

    This class extends :class:`MessageWriter` with the ability to determine the size
    of the output file.
    """

    @abstractmethod
    def file_size(self) -> int:
        """Get the current size of the output file in bytes.

        :return: The size of the file in bytes
        :rtype: int
        """


class FileIOMessageWriter(SizedMessageWriter, Generic[_IoTypeVar]):
    """Base class for writers that operate on file descriptors.

    This class provides common functionality for writers that work with file objects.

    :param file: A path-like object or file object to write to
    :param kwargs: Additional keyword arguments for specific writer implementations

    :ivar file: The file object being written to
    """

    file: _IoTypeVar

    @abstractmethod
    def __init__(self, file: Union[StringPathLike, _IoTypeVar], **kwargs: Any) -> None:
        pass

    def stop(self) -> None:
        """Close the file and stop writing."""
        self.file.close()

    def file_size(self) -> int:
        """Get the current file size."""
        return self.file.tell()


class TextIOMessageWriter(FileIOMessageWriter[Union[TextIO, TextIOWrapper]], ABC):
    """Text-based message writer implementation.

    :param file: Text file to write to
    :param mode: File open mode for text operations
    :param kwargs: Additional arguments like encoding
    """

    def __init__(
        self,
        file: Union[StringPathLike, TextIO, TextIOWrapper],
        mode: "Union[OpenTextModeUpdating, OpenTextModeWriting]" = "w",
        **kwargs: Any,
    ) -> None:
        if isinstance(file, (str, os.PathLike)):
            encoding: str = kwargs.get("encoding", locale.getpreferredencoding(False))
            # pylint: disable=consider-using-with
            self.file = Path(file).open(mode=mode, encoding=encoding)
        else:
            self.file = file


class BinaryIOMessageWriter(FileIOMessageWriter[Union[BinaryIO, BufferedIOBase]], ABC):
    """Binary file message writer implementation.

    :param file: Binary file to write to
    :param mode: File open mode for binary operations
    :param kwargs: Additional implementation specific arguments
    """

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO, BufferedIOBase],
        mode: "Union[OpenBinaryModeUpdating, OpenBinaryModeWriting]" = "wb",
        **kwargs: Any,
    ) -> None:
        if isinstance(file, (str, os.PathLike)):
            # pylint: disable=consider-using-with,unspecified-encoding
            self.file = Path(file).open(mode=mode)
        else:
            self.file = file


class MessageReader(AbstractContextManager["MessageReader"], Iterable[Message], ABC):
    """Abstract base class for all CAN message readers.

    This class serves as a foundation for implementing different message reader formats.
    It combines context manager capabilities with iteration interface.

    :param file: Path-like object or string representing the input file location
    :param kwargs: Additional keyword arguments for specific reader implementations
    """

    @abstractmethod
    def __init__(self, file: StringPathLike, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stop reading messages and cleanup any resources."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.stop()
        return False


class FileIOMessageReader(MessageReader, Generic[_IoTypeVar]):
    """Base class for readers that operate on file descriptors.

    This class provides common functionality for readers that work with file objects.

    :param file: A path-like object or file object to read from
    :param kwargs: Additional keyword arguments for specific reader implementations

    :ivar file: The file object being read from
    """

    file: _IoTypeVar

    @abstractmethod
    def __init__(self, file: Union[StringPathLike, _IoTypeVar], **kwargs: Any) -> None:
        pass

    def stop(self) -> None:
        self.file.close()


class TextIOMessageReader(FileIOMessageReader[Union[TextIO, TextIOWrapper]], ABC):
    """Text-based message reader implementation.

    :param file: Text file to read from
    :param mode: File open mode for text operations
    :param kwargs: Additional arguments like encoding
    """

    def __init__(
        self,
        file: Union[StringPathLike, TextIO, TextIOWrapper],
        mode: "OpenTextModeReading" = "r",
        **kwargs: Any,
    ) -> None:
        if isinstance(file, (str, os.PathLike)):
            encoding: str = kwargs.get("encoding", locale.getpreferredencoding(False))
            # pylint: disable=consider-using-with
            self.file = Path(file).open(mode=mode, encoding=encoding)
        else:
            self.file = file


class BinaryIOMessageReader(FileIOMessageReader[Union[BinaryIO, BufferedIOBase]], ABC):
    """Binary file message reader implementation.

    :param file: Binary file to read from
    :param mode: File open mode for binary operations
    :param kwargs: Additional implementation specific arguments
    """

    def __init__(
        self,
        file: Union[StringPathLike, BinaryIO, BufferedIOBase],
        mode: "OpenBinaryModeReading" = "rb",
        **kwargs: Any,
    ) -> None:
        if isinstance(file, (str, os.PathLike)):
            # pylint: disable=consider-using-with,unspecified-encoding
            self.file = Path(file).open(mode=mode)
        else:
            self.file = file
