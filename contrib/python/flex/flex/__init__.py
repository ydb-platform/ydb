"""
Flex
"""

# Standard libraries
import pkg_resources

__version__ = pkg_resources.get_distribution("flex").version
VERSION = __version__

from flex.core import load  # NOQA
