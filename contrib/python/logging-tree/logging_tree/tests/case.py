"""Common test class for `logging` tests."""

import logging.handlers
import unittest

class LoggingTestCase(unittest.TestCase):
    """Test case that knows the secret: how to reset the logging module."""

    def setUp(self):
        reset_logging()
        super(LoggingTestCase, self).setUp()

    def tearDown(self):
        reset_logging()
        super(LoggingTestCase, self).tearDown()

def reset_logging():
    logging.root = logging.RootLogger(logging.WARNING)
    logging.Logger.root = logging.root
    logging.Logger.manager = logging.Manager(logging.Logger.root)
