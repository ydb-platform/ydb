import argparse
import time
import typing

from ._crc32c import crc32c

DEFAULT_SIZE = 100 * 1024 * 1024
DEFAULT_ITERATIONS = 10


def run(size: int, iterations: int) -> typing.Tuple[float, int]:
    data = b" " * size
    start = time.monotonic()
    evaluations = 0
    while True:
        evaluations += iterations
        [crc32c(data) for _ in range(iterations)]
        duration = time.monotonic() - start
        if duration > 0:
            break
    return duration, evaluations


def main() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--size",
        type=int,
        help=f"Amount of bytes to checksum, defaults to {DEFAULT_SIZE}",
        default=DEFAULT_SIZE,
    )
    parser.add_argument(
        "-i",
        "--iterations",
        type=int,
        help=f"Number of times the checksum should we run over the data, defaults to {DEFAULT_ITERATIONS}",
        default=DEFAULT_ITERATIONS,
    )

    options = parser.parse_args()
    duration, evaluations = run(options.size, options.iterations)
    size_mb = options.size / 1024 / 1024
    avg_speed_gbs = size_mb / 1024 * evaluations / duration
    print(
        f"crc32c ran at {avg_speed_gbs:.3f} [GB/s] when checksuming {size_mb:.3f} [MB] {evaluations} times"
    )


if __name__ == "__main__":
    main()
