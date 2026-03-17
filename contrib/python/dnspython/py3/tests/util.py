# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import enum
import inspect
import os.path
import yatest.common

def here(filename):
    return yatest.common.test_source_path(filename)

def here_out(filename):
    return yatest.common.test_output_path(filename)

def enumerate_module(module, super_class):
    """Yield module attributes which are subclasses of given class"""
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if inspect.isclass(attr) and issubclass(attr, super_class):
            yield attr

def check_enum_exports(module, eq_callback, only=None):
    """Make sure module exports all mnemonics from enums"""
    for attr in enumerate_module(module, enum.Enum):
        if only is not None and attr not in only:
            #print('SKIP', attr)
            continue
        for flag, value in attr.__members__.items():
            #print(module, flag, value)
            eq_callback(getattr(module, flag), value)
