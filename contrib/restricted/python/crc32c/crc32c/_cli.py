"""CLI utility to compute crc32c on an input file."""

import argparse
import functools
import mmap
import os
import sys
import time
import typing

from ._crc32c import crc32c

DEFAULT_BLOCK_SIZE_FREAD = 64 * 1024
DEFAULT_BLOCK_SIZE_MMAP = 10 * 1024 * 1024


class _ChecksumingResult(typing.NamedTuple):
    filename: str
    file_size: int
    checksum: int
    duration: float


class _FormattingOptions(typing.NamedTuple):
    show_filename: bool
    show_speed: bool


def _mmap_iterator(
    fileno: int, file_size: int, block_size: int
) -> typing.Generator[mmap.mmap, None, None]:
    for offset in range(0, file_size, block_size):
        length = min(block_size, file_size - offset)
        with mmap.mmap(
            fileno, length=length, offset=offset, access=mmap.ACCESS_READ
        ) as mm:
            yield mm


def _get_checksum(filename: str, block_size: int, use_mmap: bool) -> _ChecksumingResult:
    with open(filename, "rb") as input_file:
        fileno = input_file.fileno()
        file_size = os.stat(fileno).st_size
        data_iterator: typing.Iterator[mmap.mmap | bytes]
        if use_mmap:
            data_iterator = _mmap_iterator(fileno, file_size, block_size)
        else:
            read = functools.partial(input_file.read, block_size)
            data_iterator = iter(read, b"")

        checksum = 0
        start = time.monotonic()
        for data in data_iterator:
            checksum = crc32c(data, checksum)
        end = time.monotonic()
    return _ChecksumingResult(filename, file_size, checksum, (end - start))


def _format_result(
    result: _ChecksumingResult, formatting_opts: _FormattingOptions
) -> str:
    report = f"{result.checksum:08x}"
    if formatting_opts.show_filename:
        report += f" {result.filename}"
    if formatting_opts.show_speed:
        speed = result.file_size / result.duration
        report += f" ({speed / 1024 / 1024:.2f} MB/s)"
    return report


def main(name: str = sys.argv[0], args: typing.List[str] = sys.argv[1:]) -> None:
    """Main application entry-point."""

    parser = argparse.ArgumentParser(
        sys.argv[0], description="Calculates and prints crc32c on input file(s)"
    )
    parser.add_argument("filenames", nargs="+", help="input file(s)")

    io_group = parser.add_argument_group("I/O options")
    io_group.add_argument(
        "-M", "--disable-mmap", help="Avoid reading file with mmap", action="store_true"
    )
    io_group.add_argument(
        "-b",
        "--block-size",
        type=int,
        help=f"Block size for iterative reading",
    )

    formatting_group = parser.add_argument_group("Formatting options")
    formatting_group.add_argument(
        "-s",
        "--show-speed",
        help="Report checksumming speed on each file",
        action="store_true",
    )
    formatting_group.add_argument(
        "-N",
        "--hide-filename",
        help="Do not print filename in result",
        action="store_true",
    )

    opts = parser.parse_args(args)

    use_mmap = not opts.disable_mmap
    block_size = DEFAULT_BLOCK_SIZE_MMAP if use_mmap else DEFAULT_BLOCK_SIZE_FREAD
    get_checksum = functools.partial(
        _get_checksum, block_size=block_size, use_mmap=use_mmap
    )
    formatting_opts = _FormattingOptions(not opts.hide_filename, opts.show_speed)
    for result in map(get_checksum, opts.filenames):
        print(_format_result(result, formatting_opts))


if __name__ == "__main__":
    main()
