from __future__ import annotations

import argparse
import contextlib
import pathlib
import sys
import typing
from pathlib import Path
from typing import IO, BinaryIO, TextIO

import progressbar


def size_to_bytes(size_str: str) -> int:
    """
    Convert a size string with suffixes 'k', 'm', etc., to bytes.

    Note: This function also supports '@' as a prefix to a file path to get the
    file size.

    >>> size_to_bytes('1024k')
    1048576
    >>> size_to_bytes('1024m')
    1073741824
    >>> size_to_bytes('1024g')
    1099511627776
    >>> size_to_bytes('1024')
    1024
    >>> size_to_bytes('1024p')
    1125899906842624
    """

    # Define conversion rates
    suffix_exponent = {
        'k': 1,
        'm': 2,
        'g': 3,
        't': 4,
        'p': 5,
    }

    # Initialize the default exponent to 0 (for bytes)
    exponent = 0

    # Check if the size starts with '@' (for file sizes, not handled here)
    if size_str.startswith('@'):
        return pathlib.Path(size_str[1:]).stat().st_size

    # Check if the last character is a known suffix and adjust the multiplier
    if size_str[-1].lower() in suffix_exponent:
        # Update exponent based on the suffix
        exponent = suffix_exponent[size_str[-1].lower()]
        # Remove the suffix from the size_str
        size_str = size_str[:-1]

    # Convert the size_str to an integer and apply the exponent
    return int(size_str) * (1024**exponent)


def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create the argument parser for the `progressbar` command.
    """

    parser = argparse.ArgumentParser(
        description="""
        Monitor the progress of data through a pipe.

        Note that this is a Python implementation of the original `pv` command
        that is functional but not yet feature complete.
    """
    )

    # Display switches
    parser.add_argument(
        '-p',
        '--progress',
        action='store_true',
        help='Turn the progress bar on.',
    )
    parser.add_argument(
        '-t', '--timer', action='store_true', help='Turn the timer on.'
    )
    parser.add_argument(
        '-e', '--eta', action='store_true', help='Turn the ETA timer on.'
    )
    parser.add_argument(
        '-I',
        '--fineta',
        action='store_true',
        help='Display the ETA as local time of arrival.',
    )
    parser.add_argument(
        '-r', '--rate', action='store_true', help='Turn the rate counter on.'
    )
    parser.add_argument(
        '-a',
        '--average-rate',
        action='store_true',
        help='Turn the average rate counter on.',
    )
    parser.add_argument(
        '-b',
        '--bytes',
        action='store_true',
        help='Turn the total byte counter on.',
    )
    parser.add_argument(
        '-8',
        '--bits',
        action='store_true',
        help='Display total bits instead of bytes.',
    )
    parser.add_argument(
        '-T',
        '--buffer-percent',
        action='store_true',
        help='Turn on the transfer buffer percentage display.',
    )
    parser.add_argument(
        '-A',
        '--last-written',
        type=int,
        help='Show the last NUM bytes written.',
    )
    parser.add_argument(
        '-F',
        '--format',
        type=str,
        help='Use the format string FORMAT for output format.',
    )
    parser.add_argument(
        '-n', '--numeric', action='store_true', help='Numeric output.'
    )
    parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help='No output.',
    )

    # Output modifiers
    parser.add_argument(
        '-W',
        '--wait',
        action='store_true',
        help='Wait until the first byte has been transferred.',
    )
    parser.add_argument('-D', '--delay-start', type=float, help='Delay start.')
    parser.add_argument(
        '-s', '--size', type=str, help='Assume total data size is SIZE.'
    )
    parser.add_argument(
        '-l',
        '--line-mode',
        action='store_true',
        help='Count lines instead of bytes.',
    )
    parser.add_argument(
        '-0',
        '--null',
        action='store_true',
        help='Count lines terminated with a zero byte.',
    )
    parser.add_argument(
        '-i', '--interval', type=float, help='Interval between updates.'
    )
    parser.add_argument(
        '-m',
        '--average-rate-window',
        type=int,
        help='Window for average rate calculation.',
    )
    parser.add_argument(
        '-w',
        '--width',
        type=int,
        help='Assume terminal is WIDTH characters wide.',
    )
    parser.add_argument(
        '-H', '--height', type=int, help='Assume terminal is HEIGHT rows high.'
    )
    parser.add_argument(
        '-N', '--name', type=str, help='Prefix output information with NAME.'
    )
    parser.add_argument(
        '-f', '--force', action='store_true', help='Force output.'
    )
    parser.add_argument(
        '-c',
        '--cursor',
        action='store_true',
        help='Use cursor positioning escape sequences.',
    )

    # Data transfer modifiers
    parser.add_argument(
        '-L',
        '--rate-limit',
        type=str,
        help='Limit transfer to RATE bytes per second.',
    )
    parser.add_argument(
        '-B',
        '--buffer-size',
        type=str,
        help='Use transfer buffer size of BYTES.',
    )
    parser.add_argument(
        '-C', '--no-splice', action='store_true', help='Never use splice.'
    )
    parser.add_argument(
        '-E', '--skip-errors', action='store_true', help='Ignore read errors.'
    )
    parser.add_argument(
        '-Z',
        '--error-skip-block',
        type=str,
        help='Skip block size when ignoring errors.',
    )
    parser.add_argument(
        '-S',
        '--stop-at-size',
        action='store_true',
        help='Stop transferring after SIZE bytes.',
    )
    parser.add_argument(
        '-Y',
        '--sync',
        action='store_true',
        help='Synchronise buffer caches to disk after writes.',
    )
    parser.add_argument(
        '-K',
        '--direct-io',
        action='store_true',
        help='Set O_DIRECT flag on all inputs/outputs.',
    )
    parser.add_argument(
        '-X',
        '--discard',
        action='store_true',
        help='Discard input data instead of transferring it.',
    )
    parser.add_argument(
        '-d', '--watchfd', type=str, help='Watch file descriptor of process.'
    )
    parser.add_argument(
        '-R',
        '--remote',
        type=int,
        help='Remote control another running instance of pv.',
    )

    # General options
    parser.add_argument(
        '-P', '--pidfile', type=pathlib.Path, help='Save process ID in FILE.'
    )
    parser.add_argument(
        'input',
        help='Input file path. Uses stdin if not specified.',
        default='-',
        nargs='*',
    )
    parser.add_argument(
        '-o',
        '--output',
        default='-',
        help='Output file path. Uses stdout if not specified.',
    )

    return parser


def main(argv: list[str] | None = None) -> None:  # noqa: C901
    """
    Main function for the `progressbar` command.

    Args:
        argv (list[str] | None): Command-line arguments passed to the script.

    Returns:
        None
    """
    parser: argparse.ArgumentParser = create_argument_parser()
    args: argparse.Namespace = parser.parse_args(argv)

    with contextlib.ExitStack() as stack:
        output_stream: typing.IO[typing.Any] = _get_output_stream(
            args.output, args.line_mode, stack
        )

        input_paths: list[BinaryIO | TextIO | Path | IO[typing.Any]] = []
        total_size: int = 0
        filesize_available: bool = True
        for filename in args.input:
            input_path: typing.IO[typing.Any] | pathlib.Path
            if filename == '-':
                if args.line_mode:
                    input_path = sys.stdin
                else:
                    input_path = sys.stdin.buffer

                filesize_available = False
            else:
                input_path = pathlib.Path(filename)
                if not input_path.exists():
                    parser.error(f'File not found: {filename}')

                if not args.size:
                    total_size += input_path.stat().st_size

            input_paths.append(input_path)

        # Determine the size for the progress bar (if provided)
        if args.size:
            total_size = size_to_bytes(args.size)
            filesize_available = True

        if filesize_available:
            # Create the progress bar components
            widgets = [
                progressbar.Percentage(),
                ' ',
                progressbar.Bar(),
                ' ',
                progressbar.Timer(),
                ' ',
                progressbar.FileTransferSpeed(),
            ]
        else:
            widgets = [
                progressbar.SimpleProgress(),
                ' ',
                progressbar.DataSize(),
                ' ',
                progressbar.Timer(),
            ]

        if args.eta:
            widgets.append(' ')
            widgets.append(progressbar.AdaptiveETA())

        # Initialize the progress bar
        bar = progressbar.ProgressBar(
            # widgets=widgets,
            max_value=total_size or None,
            max_error=False,
        )

        # Data processing and updating the progress bar
        buffer_size = (
            size_to_bytes(args.buffer_size) if args.buffer_size else 1024
        )
        total_transferred = 0

        bar.start()
        with contextlib.suppress(KeyboardInterrupt):
            for input_path in input_paths:
                if isinstance(input_path, pathlib.Path):
                    input_stream = stack.enter_context(
                        input_path.open('r' if args.line_mode else 'rb')
                    )
                else:
                    input_stream = input_path

                while True:
                    data: str | bytes
                    if args.line_mode:
                        data = input_stream.readline(buffer_size)
                    else:
                        data = input_stream.read(buffer_size)

                    if not data:
                        break

                    output_stream.write(data)
                    total_transferred += len(data)
                    bar.update(total_transferred)

        bar.finish(dirty=True)


def _get_output_stream(
    output: str | None,
    line_mode: bool,
    stack: contextlib.ExitStack,
) -> typing.IO[typing.Any]:
    if output and output != '-':
        mode = 'w' if line_mode else 'wb'
        return stack.enter_context(open(output, mode))  # noqa: SIM115
    elif line_mode:
        return sys.stdout
    else:
        return sys.stdout.buffer


if __name__ == '__main__':
    main()
