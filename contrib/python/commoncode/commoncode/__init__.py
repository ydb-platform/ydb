#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#


def set_re_max_cache(max_cache=1000000):
    """
    Set re and fnmatch _MAXCACHE to 1Million items to cache compiled regex
    aggressively. Their default is a maximum of 100 items and many utilities and
    libraries use a lot of regexes: therefore 100 is not enough to benefit from
    caching.
    """
    import re
    import fnmatch

    remax = getattr(re, '_MAXCACHE', 0)
    if remax < max_cache:
        setattr(re, '_MAXCACHE', max_cache)

    fnmatchmax = getattr(fnmatch, '_MAXCACHE', 0)
    if fnmatchmax < max_cache:
        setattr(fnmatch, '_MAXCACHE', max_cache)


set_re_max_cache()
