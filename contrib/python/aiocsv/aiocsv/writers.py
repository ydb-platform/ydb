# © Copyright 2020-2025 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

# cSpell: words asyncfile extrasaction fieldnames restval

import csv
import io
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence

from typing_extensions import Unpack

from .protocols import CsvDialectArg, CsvDialectKwargs, DialectLike, WithAsyncWrite


class AsyncWriter:
    """An object that writes csv rows to the given asynchronous file.
    In this object "row" is a sequence of values.

    Additional keyword arguments are passed to the underlying csv.writer instance.
    """

    def __init__(
        self,
        asyncfile: WithAsyncWrite,
        dialect: CsvDialectArg = "excel",
        **csv_dialect_kwargs: Unpack[CsvDialectKwargs],
    ) -> None:
        self._file = asyncfile
        self._buffer = io.StringIO(newline="")
        self._csv_writer = csv.writer(self._buffer, dialect=dialect, **csv_dialect_kwargs)

    @property
    def dialect(self) -> DialectLike:
        return self._csv_writer.dialect

    async def _rewrite_buffer(self) -> None:
        """Writes the current value of self._buffer to the actual target file."""
        # Write buffer value to the file
        await self._file.write(self._buffer.getvalue())

        # Clear buffer
        self._buffer.seek(0)
        self._buffer.truncate(0)

    async def writerow(self, row: Iterable[Any]) -> None:
        """Writes one row to the specified file."""
        # Pass row to underlying csv.writer instance
        self._csv_writer.writerow(row)

        # Write to actual file
        await self._rewrite_buffer()

    async def writerows(self, rows: Iterable[Iterable[Any]]) -> None:
        """Writes multiple rows to the specified file."""
        for row in rows:
            # Pass row to underlying csv.writer instance
            self._csv_writer.writerow(row)

            # Flush occasionally io prevent buffering too much data
            if self._buffer.tell() >= io.DEFAULT_BUFFER_SIZE:
                await self._rewrite_buffer()

        # Write to actual file
        await self._rewrite_buffer()


class AsyncDictWriter:
    """An object that writes csv rows to the given asynchronous file.
    In this object "row" is a mapping from fieldnames to values.

    Additional arguments have the same meaning as with csv.DictWriter.
    """

    def __init__(
        self,
        asyncfile: WithAsyncWrite,
        fieldnames: Sequence[str],
        restval: Optional[Any] = "",
        extrasaction: Literal["raise", "ignore"] = "raise",
        dialect: CsvDialectArg = "excel",
        **csv_dialect_kwargs: Unpack[CsvDialectKwargs],
    ) -> None:
        self.fieldnames = fieldnames
        self.restval = restval
        self.extrasaction = extrasaction
        self.writer = AsyncWriter(asyncfile, dialect, **csv_dialect_kwargs)

    @property
    def dialect(self) -> DialectLike:
        return self.writer.dialect

    def _dict_to_iterable(self, row_dict: Mapping[str, Any]) -> Iterable[Any]:
        if self.extrasaction == "raise":
            wrong_fields = row_dict.keys() - self.fieldnames
            if wrong_fields:
                wrong_fields_repr = ", ".join(map(repr, wrong_fields))
                raise ValueError(f"dict contains fields not in fieldnames: {wrong_fields_repr}")
        return (row_dict.get(fieldname, self.restval) for fieldname in self.fieldnames)

    async def writeheader(self) -> None:
        """Writes header row to the specified file."""
        await self.writer.writerow(self.fieldnames)

    async def writerow(self, row: Mapping[str, Any]) -> None:
        """Writes one row to the specified file."""
        await self.writer.writerow(self._dict_to_iterable(row))

    async def writerows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Writes multiple rows to the specified file."""
        await self.writer.writerows(map(self._dict_to_iterable, rows))
