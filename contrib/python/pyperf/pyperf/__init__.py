from time import perf_counter

VERSION = (2, 10, 0)
__version__ = '.'.join(map(str, VERSION))

# Export pyperf.perf_counter for backward compatibility with pyperf 1.7
# which supports Python 2 and Python 3
__all__ = ['perf_counter']

from pyperf._utils import python_implementation, python_has_jit  # noqa
__all__.extend(('python_implementation', 'python_has_jit'))

from pyperf._metadata import format_metadata  # noqa
__all__.append('format_metadata')

from pyperf._bench import Run, Benchmark, BenchmarkSuite, add_runs  # noqa
__all__.extend(('Run', 'Benchmark', 'BenchmarkSuite', 'add_runs'))

from pyperf._runner import Runner   # noqa
__all__.append('Runner')
