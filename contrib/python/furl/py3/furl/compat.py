# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

import sys


if sys.version_info[0] == 2:
    string_types = basestring  # noqa
else:
    string_types = (str, bytes)


if list(sys.version_info[:2]) >= [2, 7]:
    from collections import OrderedDict  # noqa
else:
    from ordereddict import OrderedDict  # noqa


class UnicodeMixin(object):
    """
    Mixin that defines proper __str__/__unicode__ methods in Python 2 or 3.
    """
    if sys.version_info[0] >= 3:  # Python 3
        def __str__(self):
            return self.__unicode__()
    else:  # Python 2
        def __str__(self):
            return self.__unicode__().encode('utf8')
