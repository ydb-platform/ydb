"""
Copyright 2012, 2014 Dave Brondsema

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging

from datadiff import diff, DiffTypeError

log = logging.getLogger('datadiff.tools')


# drop-in replacements for http://somethingaboutorange.com/mrl/projects/nose/doc/module_nose.tools.html

def assert_equal(first, second, msg=None):
    if first == second:
        return
    if msg is None:
        try:
            ddiff = diff(first, second)
        except DiffTypeError:
            msg = '%r != %r' % (first, second)
        else:
            msg = "\n" + str(ddiff)
    raise AssertionError(msg)


assert_equals = assert_equal

def assertAlmostEqual(first, second, places=7, msg=None):
    ddiff = diff(first, second, places=places)
    if ddiff:
        raise AssertionError(msg or "\n" + str(ddiff))

