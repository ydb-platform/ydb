"""
A script used in test_complex_case.py

python ~/code/line_profiler/tests/complex_example.py
LINE_PROFILE=1 python ~/code/line_profiler/tests/complex_example.py
python -m kernprof -v ~/code/line_profiler/tests/complex_example.py
python -m kernprof -lv ~/code/line_profiler/tests/complex_example.py


cd ~/code/line_profiler/tests/


# Run the code by itself without any profiling
PROFILE_TYPE=none python complex_example.py


# NOTE: this fails because we are not running with kernprof
PROFILE_TYPE=implicit python complex_example.py


# Kernprof with the implicit profile decorator
# NOTE: multiprocessing breaks this invocation, so set process size to zero
PROFILE_TYPE=implicit python -m kernprof -b complex_example.py --process_size=0
python -m pstats ./complex_example.py.prof


# Explicit decorator with line kernprof
# NOTE: again, multiprocessing breaks when using kernprof
PROFILE_TYPE=explicit python -m kernprof -l complex_example.py --process_size=0
python -m line_profiler complex_example.py.lprof


# Explicit decorator with cProfile kernprof
# NOTE: again, multiprocessing breaks when using kernprof (does this happen in older verions?)
PROFILE_TYPE=explicit python -m kernprof -b complex_example.py --process_size=0
python -m pstats ./complex_example.py.prof

# Explicit decorator with environment enabling
PROFILE_TYPE=explicit LINE_PROFILE=1 python complex_example.py


# Explicit decorator without enabling it
PROFILE_TYPE=explicit LINE_PROFILE=0 python complex_example.py


# Use a custom defined line profiler object
PROFILE_TYPE=custom python complex_example.py

"""
import os

# The test will define how we expect the profile decorator to exist
PROFILE_TYPE = os.environ.get('PROFILE_TYPE', '')


if PROFILE_TYPE == 'implicit':
    # Do nothing, assume kernprof will inject profile in for us
    ...
elif PROFILE_TYPE == 'none':
    # Define a no-op profiler
    def profile(func):
        return func
elif PROFILE_TYPE == 'explicit':
    # Use the explicit profile decorator
    import line_profiler
    profile = line_profiler.profile
elif PROFILE_TYPE == 'custom':
    # Create a custom profile decorator
    import line_profiler
    import atexit
    profile = line_profiler.LineProfiler()

    @atexit.register
    def _show_profile_on_end():
        ...
        profile.print_stats(summarize=1, sort=1, stripzeros=1, rich=1)
else:
    raise KeyError('')


@profile
def fib(n):
    a, b = 0, 1
    while a < n:
        a, b = b, a + b


@profile
def fib_only_called_by_thread(n):
    a, b = 0, 1
    while a < n:
        a, b = b, a + b


@profile
def fib_only_called_by_process(n):
    a, b = 0, 1
    while a < n:
        a, b = b, a + b


@profile
def main():
    """
    Run a lot of different Fibonacci jobs
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--serial_size', type=int, default=10)
    parser.add_argument('--thread_size', type=int, default=10)
    parser.add_argument('--process_size', type=int, default=10)
    args = parser.parse_args()

    for i in range(args.serial_size):
        fib(i)
        funcy_fib(
            i)
        fib(i)

    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=4)
    with executor:
        jobs = []
        for i in range(args.thread_size):
            job = executor.submit(fib, i)
            jobs.append(job)

            job = executor.submit(funcy_fib, i)
            jobs.append(job)

            job = executor.submit(fib_only_called_by_thread, i)
            jobs.append(job)

        for job in jobs:
            job.result()

    from concurrent.futures import ProcessPoolExecutor
    executor = ProcessPoolExecutor(max_workers=4)
    with executor:
        jobs = []
        for i in range(args.process_size):
            job = executor.submit(fib, i)
            jobs.append(job)

            job = executor.submit(funcy_fib, i)
            jobs.append(job)

            job = executor.submit(fib_only_called_by_process, i)
            jobs.append(job)

        for job in jobs:
            job.result()


@profile
def funcy_fib(n):
    """
    Alternative fib function where code splits out over multiple lines
    """
    a, b = (
        0, 1
    )
    while a < n:
        # print(
        #     a, end=' ')
        a, b = b, \
                a + b
    # print(
    # )


if __name__ == '__main__':
    """
    CommandLine:
        cd ~/code/line_profiler/tests/
        python complex_example.py --size 10
        python complex_example.py --serial_size 100000 --thread_size 0 --process_size 0
    """
    main()
