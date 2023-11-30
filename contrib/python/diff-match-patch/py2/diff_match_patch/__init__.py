import sys

if sys.version_info >= (3, 0):
    from .diff_match_patch import __author__, __doc__, diff_match_patch, patch_obj
else:
    from .diff_match_patch_py2 import __author__, __doc__, diff_match_patch, patch_obj

__version__ = "20200713"
__packager__ = "John Reese (john@noswap.com)"
