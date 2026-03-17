'''
Pepper is a CLI front-end to salt-api
'''
import pkg_resources

from pepper.libpepper import Pepper, PepperException

__all__ = ('__version__', 'Pepper', 'PepperException')

try:
    __version__ = pkg_resources.get_distribution('salt_pepper').version
except pkg_resources.DistributionNotFound:
    # package is not installed
    __version__ = None

# For backwards compatibility
version = __version__
sha = None
