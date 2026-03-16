# Copyright (c) 2015-2019, Activision Publishing, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
import traceback
from assertpy import assert_that, fail


def test_traceback():
    try:
        assert_that('foo').is_equal_to('bar')
        fail('should have raised error')
    except AssertionError as ex:
        assert_that(str(ex)).is_equal_to('Expected <foo> to be equal to <bar>, but was not.')
        assert_that(ex).is_type_of(AssertionError)

        # extract all stack frames from the traceback
        _, _, tb = sys.exc_info()
        assert_that(tb).is_not_none()

        # walk_tb added in 3.5
        if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
            frames = [(f.f_code.co_filename, f.f_code.co_name, lineno) for f, lineno in traceback.walk_tb(tb)]

            assert_that(frames).is_length(3)

            assert_that(frames[0][0]).ends_with('test_traceback.py')
            assert_that(frames[0][1]).is_equal_to('test_traceback')
            assert_that(frames[0][2]).is_equal_to(36)

            assert_that(frames[1][0]).ends_with('base.py')
            assert_that(frames[1][1]).is_equal_to('is_equal_to')
            assert_that(frames[1][2]).is_greater_than(40)

            assert_that(frames[2][0]).ends_with('assertpy.py')
            assert_that(frames[2][1]).is_equal_to('error')
            assert_that(frames[2][2]).is_greater_than(100)
