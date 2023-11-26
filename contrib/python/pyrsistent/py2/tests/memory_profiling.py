"""
Script to try do detect any memory leaks that may be lurking in the C implementation of the PVector.
"""
import inspect
import sys
import time
import memory_profiler
import vector_test
from pyrsistent import pvector

try:
    import pvectorc
except ImportError:
    print("No C implementation of PVector available, terminating")
    sys.exit()


PROFILING_DURATION = 2.0

def run_function(fn):
    stop = time.time() + PROFILING_DURATION
    while time.time() < stop:
        fn(pvector)

def detect_memory_leak(samples):
    # Do not allow a memory usage difference larger than 5% between the beginning and the end.
    # Skip the first samples to get rid of the build up period and the last sample since it seems
    # a little less precise
    return abs(1 - (sum(samples[5:8]) / sum(samples[-4:-1]))) > 0.05

def profile_tests():
    test_functions = [fn for fn in inspect.getmembers(vector_test, inspect.isfunction)
                      if fn[0].startswith('test_')]

    for name, fn in test_functions:
        # There are a couple of tests that are not run for the C implementation, skip those
        fn_args = inspect.getargspec(fn)[0]
        if 'pvector' in fn_args:
            print('Executing %s' % name)
            result = memory_profiler.memory_usage((run_function, (fn,), {}), interval=.1)
            assert not detect_memory_leak(result), (name, result)

if __name__ == "__main__":
    profile_tests()