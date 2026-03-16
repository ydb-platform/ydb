import logging
import os
import platform

import pytest  # type: ignore
import yaml

if platform.python_implementation() == "PyPy":
    from yaml import SafeLoader
else:
    try:
        from yaml import CSafeLoader as SafeLoader  # type: ignore
    except ImportError:
        logging.getLogger(__name__).warning(
            "PyYaml C extension not available to run tests, this will result "
            "in dramatic tests slowdown."
        )
        from yaml import SafeLoader


from ua_parser import user_agent_parser

import yatest.common as yc
TEST_RESOURCES_DIR = yc.work_path("test_data/")


class TestParse:
    def testBrowserscopeStrings(self):
        self.runUserAgentTestsFromYAML(
            os.path.join(TEST_RESOURCES_DIR, "tests/test_ua.yaml")
        )

    @pytest.mark.xfail
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
        assert result == expected, "UA: {0}\n expected<{1}> != actual<{2}>".format(
            user_agent_string, expected, result
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
            assert result == expected, (
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
                )
            )
            assert (
                len(user_agent_parser._PARSE_CACHE) <= user_agent_parser.MAX_CACHE_SIZE
            ), "verify that the cache size never exceeds the configured setting"

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
            assert result == expected, (
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
                )
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
            assert result == expected, (
                "UA: {0}\n expected<{1} {2} {3}> != actual<{4} {5} {6}>".format(
                    user_agent_string,
                    expected["family"],
                    expected["brand"],
                    expected["model"],
                    result["family"],
                    result["brand"],
                    result["model"],
                )
            )


class TestGetFilters:
    def testGetFiltersNoMatchesGiveEmptyDict(self):
        user_agent_string = "foo"
        with pytest.warns(DeprecationWarning):
            filters = user_agent_parser.GetFilters(
                user_agent_string, js_user_agent_string=None
            )
        assert {} == filters

    def testGetFiltersJsUaPassedThrough(self):
        user_agent_string = "foo"
        with pytest.warns(DeprecationWarning):
            filters = user_agent_parser.GetFilters(
                user_agent_string, js_user_agent_string="bar"
            )
        assert {"js_user_agent_string": "bar"} == filters

    def testGetFiltersJsUserAgentFamilyAndVersions(self):
        user_agent_string = (
            "Mozilla/4.0 (compatible; MSIE 8.0; "
            "Windows NT 5.1; Trident/4.0; GTB6; .NET CLR 2.0.50727; "
            ".NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
        )
        with pytest.warns(DeprecationWarning):
            filters = user_agent_parser.GetFilters(
                user_agent_string,
                js_user_agent_string="bar",
                js_user_agent_family="foo",
            )
        assert {"js_user_agent_string": "bar", "js_user_agent_family": "foo"} == filters


class TestDeprecationWarnings:
    def test_parser_deprecation(self):
        with pytest.warns(DeprecationWarning) as ws:
            user_agent_parser.ParseWithJSOverrides("")
        assert len(ws) == 1

    def test_printer_deprecation(self):
        with pytest.warns(DeprecationWarning) as ws:
            user_agent_parser.Pretty("")
        assert len(ws) == 1

    def test_js_bits_deprecation(self):
        for parser, count in [
            (user_agent_parser.Parse, 1),
            (user_agent_parser.ParseUserAgent, 1),
            (user_agent_parser.ParseOS, 1),
            (user_agent_parser.ParseDevice, 1),
        ]:
            user_agent_parser._PARSE_CACHE.clear()
            with pytest.warns(DeprecationWarning) as ws:
                parser("some random thing", js_attribute=True)
            assert len(ws) == count
