# coding: utf-8

from __future__ import unicode_literals

import logging
from optparse import OptionGroup
import os

from nose.failure import Failure
from nose.plugins import Plugin
from nose.result import TextTestResult

from flaky._flaky_plugin import _FlakyPlugin


class FlakyPlugin(_FlakyPlugin, Plugin):
    """
    Plugin for nosetests that allows retrying flaky tests.
    """
    name = 'flaky'

    def __init__(self):
        super(FlakyPlugin, self).__init__()
        self._logger = logging.getLogger('nose.plugins.flaky')
        self._flaky_result = None
        self._nose_result = None
        self._flaky_report = True
        self._force_flaky = False
        self._max_runs = None
        self._min_passes = None
        self._test_status = {}
        self._tests_that_reran = set()
        self._tests_that_have_been_reported = set()

    def options(self, parser, env=os.environ):
        """
        Base class override.
        Add options to the nose argument parser.
        """
        # pylint:disable=dangerous-default-value
        super(FlakyPlugin, self).options(parser, env=env)
        self.add_report_option(parser.add_option)
        group = OptionGroup(
            parser, "Force flaky", "Force all tests to be flaky.")
        self.add_force_flaky_options(group.add_option)
        parser.add_option_group(group)

    def _get_stream(self, multiprocess=False):
        """
        Get the stream used to store the flaky report.
        If this nose run is going to use the multiprocess plugin, then use
        a multiprocess-list backed StringIO proxy; otherwise, use the default
        stream.

        :param multiprocess:
            Whether or not this test run is configured for multiprocessing.
        :type multiprocess:
            `bool`
        :return:
            The stream to use for storing the flaky report.
        :rtype:
            :class:`StringIO` or :class:`MultiprocessingStringIO`
        """
        if multiprocess:
            from flaky.multiprocess_string_io import MultiprocessingStringIO
            return MultiprocessingStringIO()
        return self._stream

    def configure(self, options, conf):
        """Base class override."""
        super(FlakyPlugin, self).configure(options, conf)
        if not self.enabled:
            return
        is_multiprocess = int(getattr(options, 'multiprocess_workers', 0)) > 0
        self._stream = self._get_stream(is_multiprocess)
        self._flaky_result = TextTestResult(self._stream, [], 0)
        self._flaky_report = options.flaky_report
        self._flaky_success_report = options.flaky_success_report
        self._force_flaky = options.force_flaky
        self._max_runs = options.max_runs
        self._min_passes = options.min_passes

    def startTest(self, test):
        """
        Base class override. Called before a test is run.

        Add the test to the test status tracker, so it can potentially
        be rerun during afterTest.

        :param test:
            The test that is going to be run.
        :type test:
            :class:`nose.case.Test`
        """
        # pylint:disable=invalid-name
        self._test_status[test] = None

    def afterTest(self, test):
        """
        Base class override. Called after a test is run.

        If the test was marked for rerun, rerun the test.

        :param test:
            The test that has been run.
        :type test:
            :class:`nose.case.Test`
        """
        # pylint:disable=invalid-name
        if self._test_status[test]:
            self._tests_that_reran.add(id(test))
            test.run(self._flaky_result)
        self._test_status.pop(test, None)

    def _mark_test_for_rerun(self, test):
        """
        Base class override. Rerun a flaky test.

        In this case, don't actually rerun the test, but mark it for
        rerun during afterTest.

        :param test:
            The test that is going to be rerun.
        :type test:
            :class:`nose.case.Test`
        """
        self._test_status[test] = True

    def handleError(self, test, err):
        """
        Baseclass override. Called when a test raises an exception.

        If the test isn't going to be rerun again, then report the error
        to the nose test result.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        :return:
            True, if the test will be rerun; False, if nose should handle it.
        :rtype:
            `bool`
        """
        # pylint:disable=invalid-name
        want_error = self._handle_test_error_or_failure(test, err)
        if not want_error and id(test) in self._tests_that_reran:
            self._nose_result.addError(test, err)
        return want_error or None

    def handleFailure(self, test, err):
        """
        Baseclass override. Called when a test fails.

        If the test isn't going to be rerun again, then report the failure
        to the nose test result.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        :return:
            True, if the test will be rerun; False, if nose should handle it.
        :rtype:
            `bool`
        """
        # pylint:disable=invalid-name
        want_failure = self._handle_test_error_or_failure(test, err)
        if not want_failure and id(test) in self._tests_that_reran:
            self._nose_result.addFailure(test, err)
        return want_failure or None

    def addSuccess(self, test):
        """
        Baseclass override. Called when a test succeeds.

        Count remaining retries and compare with number of required successes
        that have not yet been achieved; retry if necessary.

        Returning True from this method keeps the test runner from reporting
        the test as a success; this way we can retry and only report as a
        success if we have achieved the required number of successes.

        :param test:
            The test that has succeeded
        :type test:
            :class:`nose.case.Test`
        :return:
            True, if the test will be rerun; False, if nose should handle it.
        :rtype:
            `bool`
        """
        # pylint:disable=invalid-name
        will_handle = self._handle_test_success(test)
        test_id = id(test)
        # If this isn't a rerun, the builtin reporter is going to report it as a success
        if will_handle and test_id not in self._tests_that_reran:
            self._tests_that_have_been_reported.add(test_id)
        # If this test hasn't already been reported as successful, then do it now
        if not will_handle and test_id in self._tests_that_reran and test_id not in self._tests_that_have_been_reported:
            self._nose_result.addSuccess(test)
        return will_handle or None

    def report(self, stream):
        """
        Baseclass override. Write details about flaky tests to the test report.

        :param stream:
            The test stream to which the report can be written.
        :type stream:
            `file`
        """
        if self._flaky_report:
            self._add_flaky_report(stream)

    def prepareTestResult(self, result):
        """
        Baseclass override. Called right before the first test is run.

        Stores the test result so that errors and failures can be reported
        to the nose test result.

        :param result:
            The nose test result that needs to be informed of test failures.
        :type result:
            :class:`nose.result.TextTestResult`
        """
        # pylint:disable=invalid-name
        self._nose_result = result

    def prepareTestCase(self, test):
        """
        Baseclass override. Called right before a test case is run.

        If the test class is marked flaky and the test callable is not, copy
        the flaky attributes from the test class to the test callable.

        :param test:
            The test that is being prepared to run
        :type test:
            :class:`nose.case.Test`
        """
        # pylint:disable=invalid-name
        if not isinstance(test.test, Failure):
            test_class = test.test
            self._copy_flaky_attributes(test, test_class)
            if self._force_flaky and not self._has_flaky_attributes(test):
                self._make_test_flaky(
                    test, self._max_runs, self._min_passes)

    @staticmethod
    def _get_test_callable_name(test):
        """
        Base class override.
        """
        _, _, class_and_callable_name = test.address()
        first_dot_index = class_and_callable_name.find('.')
        test_callable_name = class_and_callable_name[first_dot_index + 1:]
        return test_callable_name

    @classmethod
    def _get_test_callable(cls, test):
        """
        Base class override.

        :param test:
            The test that has raised an error or succeeded
        :type test:
            :class:`nose.case.Test`
        """
        callable_name = cls._get_test_callable_name(test)
        test_callable = getattr(
            test.test,
            callable_name,
            getattr(test.test, 'test', test.test),
        )
        return test_callable
