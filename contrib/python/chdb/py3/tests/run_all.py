#!python3

import sys
import unittest

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    END = '\033[0m'

test_loader = unittest.TestLoader()
test_suite = test_loader.discover('./')

test_runner = unittest.TextTestRunner(verbosity=2)
ret = test_runner.run(test_suite)

total = ret.testsRun
failures = len(ret.failures)
errors = len(ret.errors)
success = total - failures - errors

if failures + errors == 0:
    print(f"\n{Colors.GREEN}{Colors.BOLD}✓ ALL TESTS PASSED{Colors.END}")
    print(f"{Colors.GREEN}Success: {success}, Total: {total}{Colors.END}")
else:
    print(f"\n{Colors.RED}{Colors.BOLD}✖ TEST FAILURES{Colors.END}")
    print(f"{Colors.RED}Failed: {failures}, Errors: {errors}, Success: {success}, Total: {total}{Colors.END}")

    if failures > 0:
        print(f"\n{Colors.YELLOW}Failed Tests:{Colors.END}")
        for failure in ret.failures:
            test_case, traceback = failure
            print(f"{Colors.RED}• {test_case.id()}{Colors.END}")

    if errors > 0:
        print(f"\n{Colors.YELLOW}Errored Tests:{Colors.END}")
        for error in ret.errors:
            test_case, traceback = error
            print(f"{Colors.RED}• {test_case.id()}{Colors.END}")

# if any test fails, exit with non-zero code
if len(ret.failures) > 0 or len(ret.errors) > 0:
    exit(1)
else:
    exit(0)
