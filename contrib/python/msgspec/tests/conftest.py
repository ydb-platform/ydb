import os
import re
import sys
from pathlib import Path


def pytest_addoption(parser):
    # Declaring additional command-line options within test suite-specific `conftest.py`
    # files is documented as unsupported by pytest. See:
    # https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_addoption.
    #
    # However, it seems to work in practice with the following limitations:
    #   - The options only appear in the help text for suites that are configured in
    #     the default `testpaths` configuration. This is undesirable because we don't
    #     want a missing argument to trigger heavy tests like performance profiling.
    #   - The options are parsed differently such that an equals sign is required for
    #     options with a value rather than the more natural space-separated format.
    #
    # As a workaround, we parse the command line for options that match the patterns of
    # the configurable test suites and add their options to the parser.
    configurable_test_suites = {
        "tests/prof/perf": add_performance_profiling_options,
    }

    cwd = Path.cwd()
    project_root = Path(__file__).parent.parent
    for test_suite_relpath, add_options_func in list(configurable_test_suites.items()):
        test_suite_path = project_root.joinpath(test_suite_relpath)
        if test_suite_path == cwd or test_suite_path in cwd.parents:
            add_options_func(parser)
            configurable_test_suites.pop(test_suite_relpath)
            break

    # Always accept forward slashes and the system separator, escaping the latter in case of backslashes (Windows).
    allowed_separators = {"/", re.escape(os.sep)}
    separator_pattern = f"[{''.join(sorted(allowed_separators))}]"
    configurable_test_suites_patterns = {
        re.compile(
            rf"\b{test_suite_relpath.replace('/', separator_pattern)}\b"
        ): add_options_func
        for test_suite_relpath, add_options_func in configurable_test_suites.items()
    }

    for arg in sys.argv:
        if not configurable_test_suites_patterns:
            break

        for pattern, add_options_func in list(
            configurable_test_suites_patterns.items()
        ):
            if arg in {"-h", "--help"} or pattern.search(arg):
                add_options_func(parser)
                configurable_test_suites_patterns.pop(pattern)
                break


def add_performance_profiling_options(parser):
    group = parser.getgroup("performance profiling")
    group.addoption(
        "--calibrate",
        action="store_true",
        help=(
            "Override automatic calibration of benchmarks and enable the following options: ",
            "--rounds, --warmup-rounds, --iterations",
        ),
    )
    group.addoption(
        "--rounds",
        type=int,
        default=1000,
        metavar="ROUNDS",
        help="Number of rounds for benchmarks (default: 1000)",
    )
    group.addoption(
        "--warmup-rounds",
        type=int,
        default=50,
        metavar="WARMUP_ROUNDS",
        help="Number of warmup rounds for benchmarks (default: 50)",
    )
    group.addoption(
        "--iterations",
        type=int,
        default=10,
        metavar="ITERATIONS",
        help="Number of iterations in each round of benchmarks (default: 10)",
    )
    group.addoption(
        "--size",
        type=int,
        default=1000,
        metavar="SIZE",
        help="Size of the benchmark data (default: 1000)",
    )
