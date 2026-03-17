"""
This file supports `python -m sarif` invocation.
"""

import sys

from sarif.cmdline import main

sys.exit(main.main())
