__version__ = '0.3.0'

import sys

major, minor = sys.version_info.major, sys.version_info.minor

if major < 3:
    sys.exit("Sorry, Python 2 is not supported. You need Python >= 3.6 for Pampy.")
elif minor < 6:
    sys.exit("Sorry, You need Python >= 3.6 for Pampy.")


from pampy.pampy import match, _, ANY, HEAD, TAIL, REST, MatchError
from pampy.pampy import match_value, match_iterable, match_dict

