from __future__ import print_function

import sys
import os
import types
import traceback
import pprint
import argparse
from srsly.ruamel_yaml.compat import PY3

# DATA = 'tests/data'
# determine the position of data dynamically relative to program
# this allows running test while the current path is not the top of the
# repository, e.g. from the tests/data directory: python ../test_yaml.py
DATA = __file__.rsplit(os.sep, 2)[0] + "/data"


def find_test_functions(collections):
    if not isinstance(collections, list):
        collections = [collections]
    functions = []
    for collection in collections:
        if not isinstance(collection, dict):
            collection = vars(collection)
        for key in sorted(collection):
            value = collection[key]
            if isinstance(value, types.FunctionType) and hasattr(value, "unittest"):
                functions.append(value)
    return functions


def find_test_filenames(directory):
    filenames = {}
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            base, ext = os.path.splitext(filename)
            if base.endswith("-py2" if PY3 else "-py3"):
                continue
            filenames.setdefault(base, []).append(ext)
    filenames = sorted(filenames.items())
    return filenames


def parse_arguments(args):
    """"""
    parser = argparse.ArgumentParser(
        usage=""" run the yaml tests. By default
    all functions on all appropriate test_files are run. Functions have
    unittest attributes that determine the required extensions to filenames
    that need to be available in order to run that test. E.g.\n\n
    python test_yaml.py test_constructor_types\n
    python test_yaml.py --verbose test_tokens spec-02-05\n\n
    The presence of an extension in the .skip attribute of a function
    disables the test for that function."""
    )
    # ToDo: make into int and test > 0 in functions
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default="YAML_TEST_VERBOSE" in os.environ,
        help="set verbosity output",
    )
    parser.add_argument(
        "--list-functions",
        action="store_true",
        help="""list all functions with required file extensions for test files
        """,
    )
    parser.add_argument("function", nargs="?", help="""restrict function to run""")
    parser.add_argument(
        "filenames",
        nargs="*",
        help="""basename of filename set, extensions (.code, .data) have to
        be a superset of those in the unittest attribute of the selected
        function""",
    )
    args = parser.parse_args(args)
    # print('args', args)
    verbose = args.verbose
    include_functions = [args.function] if args.function else []
    include_filenames = args.filenames
    # if args is None:
    #     args = sys.argv[1:]
    # verbose = False
    # if '-v' in args:
    #     verbose = True
    #     args.remove('-v')
    # if '--verbose' in args:
    #     verbose = True
    #     args.remove('--verbose')  # never worked without this
    # if 'YAML_TEST_VERBOSE' in os.environ:
    #     verbose = True
    # include_functions = []
    # if args:
    #    include_functions.append(args.pop(0))
    if "YAML_TEST_FUNCTIONS" in os.environ:
        include_functions.extend(os.environ["YAML_TEST_FUNCTIONS"].split())
    # include_filenames = []
    # include_filenames.extend(args)
    if "YAML_TEST_FILENAMES" in os.environ:
        include_filenames.extend(os.environ["YAML_TEST_FILENAMES"].split())
    return include_functions, include_filenames, verbose, args


def execute(function, filenames, verbose):
    if PY3:
        name = function.__name__
    else:
        if hasattr(function, "unittest_name"):
            name = function.unittest_name
        else:
            name = function.func_name
    if verbose:
        sys.stdout.write("=" * 75 + "\n")
        sys.stdout.write("%s(%s)...\n" % (name, ", ".join(filenames)))
    try:
        function(verbose=verbose, *filenames)
    except Exception as exc:
        info = sys.exc_info()
        if isinstance(exc, AssertionError):
            kind = "FAILURE"
        else:
            kind = "ERROR"
        if verbose:
            traceback.print_exc(limit=1, file=sys.stdout)
        else:
            sys.stdout.write(kind[0])
            sys.stdout.flush()
    else:
        kind = "SUCCESS"
        info = None
        if not verbose:
            sys.stdout.write(".")
    sys.stdout.flush()
    return (name, filenames, kind, info)


def display(results, verbose):
    if results and not verbose:
        sys.stdout.write("\n")
    total = len(results)
    failures = 0
    errors = 0
    for name, filenames, kind, info in results:
        if kind == "SUCCESS":
            continue
        if kind == "FAILURE":
            failures += 1
        if kind == "ERROR":
            errors += 1
        sys.stdout.write("=" * 75 + "\n")
        sys.stdout.write("%s(%s): %s\n" % (name, ", ".join(filenames), kind))
        if kind == "ERROR":
            traceback.print_exception(file=sys.stdout, *info)
        else:
            sys.stdout.write("Traceback (most recent call last):\n")
            traceback.print_tb(info[2], file=sys.stdout)
            sys.stdout.write("%s: see below\n" % info[0].__name__)
            sys.stdout.write("~" * 75 + "\n")
            for arg in info[1].args:
                pprint.pprint(arg, stream=sys.stdout)
        for filename in filenames:
            sys.stdout.write("-" * 75 + "\n")
            sys.stdout.write("%s:\n" % filename)
            if PY3:
                with open(filename, "r", errors="replace") as fp:
                    data = fp.read()
            else:
                with open(filename, "rb") as fp:
                    data = fp.read()
            sys.stdout.write(data)
            if data and data[-1] != "\n":
                sys.stdout.write("\n")
    sys.stdout.write("=" * 75 + "\n")
    sys.stdout.write("TESTS: %s\n" % total)
    ret_val = 0
    if failures:
        sys.stdout.write("FAILURES: %s\n" % failures)
        ret_val = 1
    if errors:
        sys.stdout.write("ERRORS: %s\n" % errors)
        ret_val = 2
    return ret_val


def run(collections, args=None):
    test_functions = find_test_functions(collections)
    test_filenames = find_test_filenames(DATA)
    include_functions, include_filenames, verbose, a = parse_arguments(args)
    if a.list_functions:
        print("test functions:")
        for f in test_functions:
            print("  {:30s} {}".format(f.__name__, f.unittest))
        return
    results = []
    for function in test_functions:
        if include_functions and function.__name__ not in include_functions:
            continue
        if function.unittest:
            for base, exts in test_filenames:
                if include_filenames and base not in include_filenames:
                    continue
                filenames = []
                for ext in function.unittest:
                    if ext not in exts:
                        break
                    filenames.append(os.path.join(DATA, base + ext))
                else:
                    skip_exts = getattr(function, "skip", [])
                    for skip_ext in skip_exts:
                        if skip_ext in exts:
                            break
                    else:
                        result = execute(function, filenames, verbose)
                        results.append(result)
        else:
            result = execute(function, [], verbose)
            results.append(result)
    return display(results, verbose=verbose)
