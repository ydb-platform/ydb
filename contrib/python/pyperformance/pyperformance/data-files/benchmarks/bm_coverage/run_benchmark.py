"""
Benchmark coverage performance with a recursive fibonacci function.
"""

import coverage
import pyperf


def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def bench_coverage(loops: int) -> None:
    range_it = range(loops)
    cov = coverage.Coverage()
    cov.start()
    t0 = pyperf.perf_counter()
    for _ in range_it:
        fibonacci(25)
    cov.stop()
    return pyperf.perf_counter() - t0


if __name__ == "__main__":
    runner = pyperf.Runner()
    runner.metadata['description'] = "Benchmark coverage"
    runner.bench_time_func('coverage', bench_coverage)
