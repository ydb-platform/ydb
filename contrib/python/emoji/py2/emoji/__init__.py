# -*- coding: UTF-8 -*-


"""
emoji for Python
~~~~~~~~~~~~~~~~

emoji terminal output for Python.

    >>> import emoji
    >>> print(emoji.emojize('Python is :thumbsup:', language='alias'))
    Python is 👍
    >>> print(emoji.emojize('Python is :thumbs_up:'))
    Python is 👍
"""


import sys
from emoji.core import *
if sys.version_info[0] == 3 and sys.version_info[1] >= 7:
    # On Python>3.7 the __getattr__() method (see below) will be used to show
    # a deprecation warning for module variables like EMOJI_UNICODE_* and UNICODE_EMOJI_*
    import emoji.unicode_codes
else:
    # On Python<3.7 __getattr__() is not supported
    # Normal import of all module variables
    from emoji.unicode_codes import *


__all__ = [
    # emoji.core
    'emojize', 'demojize', 'get_emoji_regexp', 'emoji_count', 'emoji_lis',
    'distinct_emoji_lis', 'replace_emoji', 'version', 'is_emoji',
    'emoji_list', 'distinct_emoji_list',
    # emoji.unicode_codes
    'EMOJI_UNICODE_ENGLISH', 'EMOJI_UNICODE_SPANISH', 'EMOJI_UNICODE_PORTUGUESE',
    'EMOJI_UNICODE_ITALIAN', 'EMOJI_UNICODE_FRENCH', 'EMOJI_UNICODE_GERMAN',
    'UNICODE_EMOJI_ENGLISH', 'UNICODE_EMOJI_SPANISH', 'UNICODE_EMOJI_PORTUGUESE',
    'UNICODE_EMOJI_ITALIAN', 'UNICODE_EMOJI_FRENCH', 'UNICODE_EMOJI_GERMAN',
    'EMOJI_ALIAS_UNICODE_ENGLISH', 'UNICODE_EMOJI_ALIAS_ENGLISH', 'EMOJI_DATA',
]

__version__ = '1.7.0'
__author__ = 'Taehoon Kim, Kevin Wurster and Tahir Jalilov'
__email__ = 'carpedm20@gmail.com'
# and wursterk@gmail.com, tahir.jalilov@gmail.com
__source__ = 'https://github.com/carpedm20/emoji/'
__license__ = '''
New BSD License

Copyright (c) 2014-2021, Taehoon Kim, Kevin Wurster and Tahir Jalilov
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* The names of its contributors may not be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''


_DEPRECATED = [
    'EMOJI_UNICODE_ENGLISH', 'EMOJI_UNICODE_SPANISH', 'EMOJI_UNICODE_PORTUGUESE',
    'EMOJI_UNICODE_ITALIAN', 'EMOJI_UNICODE_FRENCH', 'EMOJI_UNICODE_GERMAN',

    'UNICODE_EMOJI_ENGLISH', 'UNICODE_EMOJI_SPANISH', 'UNICODE_EMOJI_PORTUGUESE',
    'UNICODE_EMOJI_ITALIAN', 'UNICODE_EMOJI_FRENCH', 'UNICODE_EMOJI_GERMAN',

    'EMOJI_ALIAS_UNICODE_ENGLISH', 'UNICODE_EMOJI_ALIAS_ENGLISH',
]


def __getattr__(varname):
    if varname in emoji.unicode_codes.__all__:
        if varname in _DEPRECATED:
            # Show a deprecation warning
            emoji.core._deprecation_removed(varname, "Use 'emoji.EMOJI_DATA' instead.")
        return getattr(emoji.unicode_codes, varname)
    raise AttributeError("module 'emoji' has no attribute '%s'" % (varname, ))
