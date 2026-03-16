# Copyright 2008 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License')
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""User Agent Parser Unit Tests.
Run:
# python -m user_agent_parser_test  (runs all the tests, takes awhile)
or like:
# python -m user_agent_parser_test ParseTest.testBrowserscopeStrings
"""


from __future__ import unicode_literals, absolute_import

__author__ = "slamm@google.com (Stephen Lamm)"

import logging
import os
import platform
import re
import sys
import unittest
import warnings
import yaml

if platform.python_implementation() == "PyPy":
    from yaml import SafeLoader
else:
    try:
        from yaml import CSafeLoader as SafeLoader
    except ImportError:
        logging.getLogger(__name__).warning(
            "PyYaml C extension not available to run tests, this will result "
            "in dramatic tests slowdown."
        )
        from yaml import SafeLoader


from ua_parser import user_agent_parser

import yatest.common as yc
TEST_RESOURCES_DIR = yc.work_path('test_data/')


class ParseTest(unittest.TestCase):
    def testBrowserscopeStrings(self):
        self.runUserAgentTestsFromYAML(
            os.path.join(TEST_RESOURCES_DIR, "tests/test_ua.yaml")
        )

    def testBrowserscopeStringsOS(self):
        self.runOSTestsFromYAML(os.path.join(TEST_RESOURCES_DIR, "tests/test_os.yaml"))

    def testStringsOS(self):
        self.runOSTestsFromYAML(
            os.path.join(TEST_RESOURCES_DIR, "test_resources/additional_os_tests.yaml")
        )

    def testStringsDevice(self):
        self.runDeviceTestsFromYAML(
            os.path.join(TEST_RESOURCES_DIR, "tests/test_device.yaml")
        )

    def testMozillaStrings(self):
        self.runUserAgentTestsFromYAML(
            os.path.join(
                TEST_RESOURCES_DIR, "test_resources/firefox_user_agent_strings.yaml"
            )
        )

    # NOTE: The YAML file used here is one output by makePGTSComparisonYAML()
    # below, as opposed to the pgts_browser_list-orig.yaml file.  The -orig
    # file is by no means perfect, but identifies many browsers that we
    # classify as "Other".  This test itself is mostly useful to know when
    # somthing in UA parsing changes.  An effort should be made to try and
    # reconcile the differences between the two YAML files.
    def testPGTSStrings(self):
        self.runUserAgentTestsFromYAML(
            os.path.join(TEST_RESOURCES_DIR, "test_resources/pgts_browser_list.yaml")
        )

    def testParseAll(self):
        user_agent_string = "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; fr; rv:1.9.1.5) Gecko/20091102 Firefox/3.5.5,gzip(gfe),gzip(gfe)"
        expected = {
            "device": {"family": "Mac", "brand": "Apple", "model": "Mac"},
            "os": {
                "family": "Mac OS X",
                "major": "10",
                "minor": "4",
                "patch": None,
                "patch_minor": None,
            },
            "user_agent": {
                "family": "Firefox",
                "major": "3",
                "minor": "5",
                "patch": "5",
            },
            "string": user_agent_string,
        }

        result = user_agent_parser.Parse(user_agent_string)
        self.assertEqual(
            result,
            expected,
            "UA: {0}\n expected<{1}> != actual<{2}>".format(
                user_agent_string, expected, result
            ),
        )

    # Run a set of test cases from a YAML file
    def runUserAgentTestsFromYAML(self, file_name):
        yamlFile = open(os.path.join(TEST_RESOURCES_DIR, file_name))
        yamlContents = yaml.load(yamlFile, Loader=SafeLoader)
        yamlFile.close()

        for test_case in yamlContents["test_cases"]:
            # Inputs to Parse()
            user_agent_string = test_case["user_agent_string"]

            # The expected results
            expected = {
                "family": test_case["family"],
                "major": test_case["major"],
                "minor": test_case["minor"],
                "patch": test_case["patch"],
            }

            result = {}
            result = user_agent_parser.ParseUserAgent(user_agent_string)
            self.assertEqual(
                result,
                expected,
                "UA: {0}\n expected<{1}, {2}, {3}, {4}> != actual<{5}, {6}, {7}, {8}>".format(
                    user_agent_string,
                    expected["family"],
                    expected["major"],
                    expected["minor"],
                    expected["patch"],
                    result["family"],
                    result["major"],
                    result["minor"],
                    result["patch"],
                ),
            )
            self.assertLessEqual(
                len(user_agent_parser._PARSE_CACHE),
                user_agent_parser.MAX_CACHE_SIZE,
                "verify that the cache size never exceeds the configured setting",
            )

    def runOSTestsFromYAML(self, file_name):
        yamlFile = open(os.path.join(TEST_RESOURCES_DIR, file_name))
        yamlContents = yaml.load(yamlFile, Loader=SafeLoader)
        yamlFile.close()

        for test_case in yamlContents["test_cases"]:
            # Inputs to Parse()
            user_agent_string = test_case["user_agent_string"]

            # The expected results
            expected = {
                "family": test_case["family"],
                "major": test_case["major"],
                "minor": test_case["minor"],
                "patch": test_case["patch"],
                "patch_minor": test_case["patch_minor"],
            }

            result = user_agent_parser.ParseOS(user_agent_string)
            self.assertEqual(
                result,
                expected,
                "UA: {0}\n expected<{1} {2} {3} {4} {5}> != actual<{6} {7} {8} {9} {10}>".format(
                    user_agent_string,
                    expected["family"],
                    expected["major"],
                    expected["minor"],
                    expected["patch"],
                    expected["patch_minor"],
                    result["family"],
                    result["major"],
                    result["minor"],
                    result["patch"],
                    result["patch_minor"],
                ),
            )

    def runDeviceTestsFromYAML(self, file_name):
        yamlFile = open(os.path.join(TEST_RESOURCES_DIR, file_name))
        yamlContents = yaml.load(yamlFile, Loader=SafeLoader)
        yamlFile.close()

        for test_case in yamlContents["test_cases"]:
            # Inputs to Parse()
            user_agent_string = test_case["user_agent_string"]

            # The expected results
            expected = {
                "family": test_case["family"],
                "brand": test_case["brand"],
                "model": test_case["model"],
            }

            result = user_agent_parser.ParseDevice(user_agent_string)
            self.assertEqual(
                result,
                expected,
                "UA: {0}\n expected<{1} {2} {3}> != actual<{4} {5} {6}>".format(
                    user_agent_string,
                    expected["family"],
                    expected["brand"],
                    expected["model"],
                    result["family"],
                    result["brand"],
                    result["model"],
                ),
            )


class GetFiltersTest(unittest.TestCase):
    def testGetFiltersNoMatchesGiveEmptyDict(self):
        user_agent_string = "foo"
        filters = user_agent_parser.GetFilters(
            user_agent_string, js_user_agent_string=None
        )
        self.assertEqual({}, filters)

    def testGetFiltersJsUaPassedThrough(self):
        user_agent_string = "foo"
        filters = user_agent_parser.GetFilters(
            user_agent_string, js_user_agent_string="bar"
        )
        self.assertEqual({"js_user_agent_string": "bar"}, filters)

    def testGetFiltersJsUserAgentFamilyAndVersions(self):
        user_agent_string = (
            "Mozilla/4.0 (compatible; MSIE 8.0; "
            "Windows NT 5.1; Trident/4.0; GTB6; .NET CLR 2.0.50727; "
            ".NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
        )
        filters = user_agent_parser.GetFilters(
            user_agent_string, js_user_agent_string="bar", js_user_agent_family="foo"
        )
        self.assertEqual(
            {"js_user_agent_string": "bar", "js_user_agent_family": "foo"}, filters
        )


class TestDeprecationWarnings(unittest.TestCase):
    def setUp(self):
        """In Python 2.7, catch_warnings apparently does not do anything if
        the warning category is not active, whereas in 3(.6 and up) it
        seems to work out of the box.
        """
        super(TestDeprecationWarnings, self).setUp()
        warnings.simplefilter("always", DeprecationWarning)

    def tearDown(self):
        # not ideal as it discards all other warnings updates from the
        # process, should really copy the contents of
        # `warnings.filters`, then reset-it.
        warnings.resetwarnings()
        super(TestDeprecationWarnings, self).tearDown()

    def test_parser_deprecation(self):
        with warnings.catch_warnings(record=True) as ws:
            user_agent_parser.ParseWithJSOverrides("")
        self.assertEqual(len(ws), 1)
        self.assertEqual(ws[0].category, DeprecationWarning)

    def test_printer_deprecation(self):
        with warnings.catch_warnings(record=True) as ws:
            user_agent_parser.Pretty("")
        self.assertEqual(len(ws), 1)
        self.assertEqual(ws[0].category, DeprecationWarning)

    def test_js_bits_deprecation(self):
        for parser, count in [
            (user_agent_parser.Parse, 3),
            (user_agent_parser.ParseUserAgent, 1),
            (user_agent_parser.ParseOS, 1),
            (user_agent_parser.ParseDevice, 1),
        ]:
            user_agent_parser._PARSE_CACHE.clear()
            with warnings.catch_warnings(record=True) as ws:
                parser("some random thing", js_attribute=True)
            self.assertEqual(len(ws), count)
            for w in ws:
                self.assertEqual(w.category, DeprecationWarning)


class ErrTest(unittest.TestCase):
    @unittest.skipIf(
        sys.version_info < (3,), "bytes and str are not differentiated in P2"
    )
    def test_bytes(self):
        with self.assertRaises(TypeError):
            user_agent_parser.Parse(b"")

    def test_int(self):
        with self.assertRaises(TypeError):
            user_agent_parser.Parse(0)

    def test_list(self):
        with self.assertRaises(TypeError):
            user_agent_parser.Parse([])

    def test_tuple(self):
        with self.assertRaises(TypeError):
            user_agent_parser.Parse(())


if __name__ == "__main__":
    unittest.main()
