import logging
import os.path
import sys

try:
    import unittest2 as unittest
except ImportError:
    import unittest


logging.basicConfig(format='%(levelname)s:%(funcName)s:%(message)s',
                    level=logging.DEBUG)
