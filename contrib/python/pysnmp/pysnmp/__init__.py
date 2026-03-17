"""
This module initializes the versioning information for the pysnmp package.

Attributes:
    __version__ (str): The current version of the pysnmp package.
    main_version (str): The main version string, used to prevent semantic release from updating the version in multiple places.
    version (tuple): A tuple representation of the version, where each part of the version string is converted to an integer.
    major_version_id (int): The major version number extracted from the version tuple.

Notes:
    - For backward compatibility, if the version string contains "beta", the string part is removed before converting to a tuple.
"""

# http://www.python.org/dev/peps/pep-0396/
__version__ = "7.1.22"
# another variable is required to prevent semantic release from updating version in more than one place
main_version = __version__
# backward compatibility
# for beta versions, integer casting throws an exception, so string part must be cut off
if "beta" in __version__:
    main_version = __version__.split("-beta")[0]
version = tuple(int(x) for x in main_version.split("."))
major_version_id = version[0]
