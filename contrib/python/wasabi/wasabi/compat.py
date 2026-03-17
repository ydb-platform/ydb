import sys


# Use typing_extensions for Python versions < 3.8
if sys.version_info < (3, 8):
    from typing_extensions import Protocol, Literal
else:
    from typing import Protocol, Literal  # noqa: F401
