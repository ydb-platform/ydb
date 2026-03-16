# -*- coding: utf-8 -*-
import os
import unittest

# Set the environment variable to use PyVISA-py as backend
os.environ["PYVISA_LIBRARY"] = "@py"


def testsuite():
    """A testsuite that has all the pyvisa-py tests."""
    return unittest.TestLoader().discover(os.path.dirname(__file__))


def main():
    """Runs the testsuite as command line application."""
    try:
        unittest.main()
    except Exception as e:
        print("Error: %s" % e)


def run() -> unittest.TestResult:
    """Run all tests."""
    test_runner = unittest.TextTestRunner()
    return test_runner.run(testsuite())
