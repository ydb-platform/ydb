##############################################################################
# Copyright 2009, Gerhard Weis
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#  * Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#  * Neither the name of the authors nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT
##############################################################################
'''
Test cases for the isotime module.
'''
from datetime import time

import pytest

from isodate import parse_time, UTC, FixedOffset, ISO8601Error, time_isoformat
from isodate import TIME_BAS_COMPLETE, TIME_BAS_MINUTE
from isodate import TIME_EXT_COMPLETE, TIME_EXT_MINUTE
from isodate import TIME_HOUR
from isodate import TZ_BAS, TZ_EXT, TZ_HOUR

# the following list contains tuples of ISO time strings and the expected
# result from the parse_time method. A result of None means an ISO8601Error
# is expected.
TEST_CASES = [('232050', time(23, 20, 50), TIME_BAS_COMPLETE + TZ_BAS),
              ('23:20:50', time(23, 20, 50), TIME_EXT_COMPLETE + TZ_EXT),
              ('2320', time(23, 20), TIME_BAS_MINUTE),
              ('23:20', time(23, 20), TIME_EXT_MINUTE),
              ('23', time(23), TIME_HOUR),
              ('232050,5', time(23, 20, 50, 500000), None),
              ('23:20:50.5', time(23, 20, 50, 500000), None),
              # test precision
              ('15:33:42.123456', time(15, 33, 42, 123456), None),
              ('15:33:42.1234564', time(15, 33, 42, 123456), None),
              ('15:33:42.1234557', time(15, 33, 42, 123456), None),
              ('2320,8', time(23, 20, 48), None),
              ('23:20,8', time(23, 20, 48), None),
              ('23,3', time(23, 18), None),
              ('232030Z', time(23, 20, 30, tzinfo=UTC),
               TIME_BAS_COMPLETE + TZ_BAS),
              ('2320Z', time(23, 20, tzinfo=UTC), TIME_BAS_MINUTE + TZ_BAS),
              ('23Z', time(23, tzinfo=UTC), TIME_HOUR + TZ_BAS),
              ('23:20:30Z', time(23, 20, 30, tzinfo=UTC),
               TIME_EXT_COMPLETE + TZ_EXT),
              ('23:20Z', time(23, 20, tzinfo=UTC), TIME_EXT_MINUTE + TZ_EXT),
              ('152746+0100', time(15, 27, 46,
               tzinfo=FixedOffset(1, 0, '+0100')), TIME_BAS_COMPLETE + TZ_BAS),
              ('152746-0500', time(15, 27, 46,
                                   tzinfo=FixedOffset(-5, 0, '-0500')),
               TIME_BAS_COMPLETE + TZ_BAS),
              ('152746+01', time(15, 27, 46,
                                 tzinfo=FixedOffset(1, 0, '+01:00')),
               TIME_BAS_COMPLETE + TZ_HOUR),
              ('152746-05', time(15, 27, 46,
                                 tzinfo=FixedOffset(-5, -0, '-05:00')),
               TIME_BAS_COMPLETE + TZ_HOUR),
              ('15:27:46+01:00', time(15, 27, 46,
                                      tzinfo=FixedOffset(1, 0, '+01:00')),
               TIME_EXT_COMPLETE + TZ_EXT),
              ('15:27:46-05:00', time(15, 27, 46,
                                      tzinfo=FixedOffset(-5, -0, '-05:00')),
               TIME_EXT_COMPLETE + TZ_EXT),
              ('15:27:46+01', time(15, 27, 46,
                                   tzinfo=FixedOffset(1, 0, '+01:00')),
               TIME_EXT_COMPLETE + TZ_HOUR),
              ('15:27:46-05', time(15, 27, 46,
                                   tzinfo=FixedOffset(-5, -0, '-05:00')),
               TIME_EXT_COMPLETE + TZ_HOUR),
              ('15:27:46-05:30', time(15, 27, 46,
                                      tzinfo=FixedOffset(-5, -30, '-05:30')),
               TIME_EXT_COMPLETE + TZ_EXT),
              ('15:27:46-0545', time(15, 27, 46,
                                     tzinfo=FixedOffset(-5, -45, '-0545')),
               TIME_EXT_COMPLETE + TZ_BAS),
              ('1:17:30', None, TIME_EXT_COMPLETE)]


@pytest.mark.parametrize("timestring,expectation,format", TEST_CASES)
def test_parse(timestring, expectation, format):
    '''
    Parse an ISO time string and compare it to the expected value.
    '''
    if expectation is None:
        with pytest.raises(ISO8601Error):
            parse_time(timestring)
    else:
        result = parse_time(timestring)
        result == expectation


@pytest.mark.parametrize("timestring,expectation,format", TEST_CASES)
def test_format(timestring, expectation, format):
    '''
    Take time object and create ISO string from it.
    This is the reverse test to test_parse.
    '''
    if expectation is None:
        with pytest.raises(AttributeError):
            time_isoformat(expectation, format)
    elif format is not None:
        assert time_isoformat(expectation, format) == timestring
