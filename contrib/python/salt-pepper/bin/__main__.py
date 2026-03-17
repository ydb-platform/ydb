#!/usr/bin/env python

# Import Python Libraries
import logging

# Import Pepper Libraries
import pepper.script

try:
    from logging import NullHandler
except ImportError:  # Python < 2.7
    class NullHandler(logging.Handler):
        def emit(self, record): pass

logging.basicConfig(format='%(levelname)s %(asctime)s %(module)s: %(message)s')
logger = logging.getLogger('pepper')
logger.addHandler(NullHandler())


if __name__ == '__main__':
    exit_code = pepper.script.Pepper()()
    raise SystemExit(exit_code if exit_code is not None else 0)
