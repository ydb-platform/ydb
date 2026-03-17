# coding: utf-8

from __future__ import unicode_literals

from io import StringIO
from traceback import format_exception

from flaky import defaults
from flaky.names import FlakyNames
from flaky.utils import ensure_unicode_string


class _FlakyPlugin(object):
    _retry_failure_message = ' failed ({0} runs remaining out of {1}).'
    _failure_message = ' failed; it passed {0} out of the required {1} times.'
    _not_rerun_message = ' failed and was not selected for rerun.'

    def __init__(self):
        super(_FlakyPlugin, self).__init__()
        self._stream = StringIO()
        self._flaky_success_report = True
        self._had_flaky_tests = False

    @property
    def stream(self):
        """
        Returns the stream used for building the flaky report.
        Anything written to this stream before the end of the test run
        will be written to the flaky report.

        :return:
            The stream used for building the flaky report.
        :rtype:
            :class:`StringIO`
        """
        return self._stream

    def _log_test_failure(self, test_callable_name, err, message):
        """
        Add messaging about a test failure to the stream, which will be
        printed by the plugin's report method.
        """
        formatted_exception_info = ''.join(format_exception(*err)).replace('\n', '\n\t').rstrip()
        self._stream.writelines([
            ensure_unicode_string(test_callable_name),
            ensure_unicode_string(message),
            ensure_unicode_string(formatted_exception_info),
            '\n',
        ])

    def _report_final_failure(self, err, flaky, name):
        """
        Report that the test has failed too many times to pass at
        least min_passes times.

        By default, this means that the test has failed twice.

        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        :param flaky:
            Dictionary of flaky attributes
        :type flaky:
            `dict` of `unicode` to varies
        :param name:
            The test name
        :type name:
            `unicode`
        """
        min_passes = flaky[FlakyNames.MIN_PASSES]
        current_passes = flaky[FlakyNames.CURRENT_PASSES]
        message = self._failure_message.format(
            current_passes,
            min_passes,
        )
        self._log_test_failure(name, err, message)

    def _log_intermediate_failure(self, err, flaky, name):
        """
        Report that the test has failed, but still has reruns left.
        Then rerun the test.

        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        :param flaky:
            Dictionary of flaky attributes
        :type flaky:
            `dict` of `unicode` to varies
        :param name:
            The test name
        :type name:
            `unicode`
        """
        max_runs = flaky[FlakyNames.MAX_RUNS]
        runs_left = max_runs - flaky[FlakyNames.CURRENT_RUNS]
        message = self._retry_failure_message.format(
            runs_left,
            max_runs,
        )
        self._log_test_failure(name, err, message)

    def _should_handle_test_error_or_failure(self, test):
        """
        Whether or not flaky should handle a test error or failure.
        Only handle tests marked @flaky.
        Count remaining retries and compare with number of required successes that have not yet been achieved.

        This method may be called multiple times for the same test run, so it has no side effects.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :return:
            True, if the test needs to be rerun; False, otherwise.
        :rtype:
            `bool`
        """
        if not self._has_flaky_attributes(test):
            return False
        flaky_attributes = self._get_flaky_attributes(test)
        flaky_attributes[FlakyNames.CURRENT_RUNS] += 1
        has_failed = self._has_flaky_test_failed(flaky_attributes)
        return not has_failed

    def _will_handle_test_error_or_failure(self, test, name, err):
        """
        Whether or not flaky will handle a test error or failure.
        Returns True if the plugin should handle the test result, and
        the `rerun_filter` returns True.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :param name:
            The name of the test that has raised an error
        :type name:
            `unicode`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `type`, :class:`Exception`, `traceback`
        :return:
            True, if the test will be rerun by flaky; False, otherwise.
        :rtype:
            `bool`
        """
        return self._should_handle_test_error_or_failure(test) and self._should_rerun_test(test, name, err)

    def _handle_test_error_or_failure(self, test, err):
        """
        Handle a flaky test error or failure.

        Returning True from this method keeps the test runner from reporting
        the test as a failure; this way we can retry and only report as a
        failure if we are out of retries.

        This method may only be called once per test run; it changes persisted flaky attributes.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `type`, :class:`Exception`, `traceback`
        :return:
            True, if the test will be rerun;
            False, if the test runner should handle it.
        :rtype:
            `bool`
        """
        try:
            name = self._get_test_callable_name(test)
        except AttributeError:
            return False

        if self._has_flaky_attributes(test):
            self._had_flaky_tests = True
            self._add_flaky_test_failure(test, err)
            should_handle = self._should_handle_test_error_or_failure(test)
            self._increment_flaky_attribute(test, FlakyNames.CURRENT_RUNS)
            if should_handle:
                flaky_attributes = self._get_flaky_attributes(test)
                if self._should_rerun_test(test, name, err):
                    self._log_intermediate_failure(err, flaky_attributes, name)
                    self._mark_test_for_rerun(test)
                    return True
                self._log_test_failure(name, err, self._not_rerun_message)
                return False
            flaky_attributes = self._get_flaky_attributes(test)
            self._report_final_failure(err, flaky_attributes, name)
        return False

    def _should_rerun_test(self, test, name, err):
        """
        Whether or not a test should be rerun.
        This is a pass-through to the test's rerun filter.

        A flaky test will only be rerun if it hasn't failed too many
        times to succeed at least min_passes times, and if
        this method returns True.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :param name:
            The test name
        :type name:
            `unicode`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        :return:
            Whether flaky should rerun this test.
        :rtype:
            `bool`
        """
        rerun_filter = self._get_flaky_attribute(test, FlakyNames.RERUN_FILTER)
        return rerun_filter(err, name, test, self)

    def _mark_test_for_rerun(self, test):
        """
        Mark a flaky test for rerun.

        :param test:
            The test that has raised an error or succeeded
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        """
        raise NotImplementedError  # pragma: no cover

    def _should_handle_test_success(self, test):
        if not self._has_flaky_attributes(test):
            return False
        flaky = self._get_flaky_attributes(test)
        flaky[FlakyNames.CURRENT_PASSES] += 1
        flaky[FlakyNames.CURRENT_RUNS] += 1
        return not self._has_flaky_test_succeeded(flaky)

    def _handle_test_success(self, test):
        """
        Handle a flaky test success.
        Count remaining retries and compare with number of required successes
        that have not yet been achieved; retry if necessary.

        Returning True from this method keeps the test runner from reporting
        the test as a success; this way we can retry and only report as a
        success if the test has passed the required number of times.

        :param test:
            The test that has raised an error
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :return:
            True, if the test will be rerun; False, if the test runner should handle it.
        :rtype:
            `bool`
        """
        try:
            name = self._get_test_callable_name(test)
        except AttributeError:
            return False
        need_reruns = self._should_handle_test_success(test)

        if self._has_flaky_attributes(test):
            self._had_flaky_tests = True
            flaky = self._get_flaky_attributes(test)
            min_passes = flaky[FlakyNames.MIN_PASSES]
            passes = flaky[FlakyNames.CURRENT_PASSES] + 1
            self._set_flaky_attribute(test, FlakyNames.CURRENT_PASSES, passes)
            self._increment_flaky_attribute(test, FlakyNames.CURRENT_RUNS)

            if self._flaky_success_report:
                self._stream.writelines([
                    ensure_unicode_string(name),
                    ' passed {} out of the required {} times. '.format(
                        passes,
                        min_passes,
                    ),
                ])
                if need_reruns:
                    self._stream.write(
                        'Running test again until it passes {} times.\n'.format(
                            min_passes,
                        )
                    )
                else:
                    self._stream.write('Success!\n')

        if need_reruns:
            self._mark_test_for_rerun(test)
        return need_reruns

    @staticmethod
    def add_report_option(add_option):
        """
        Add an option to the test runner to suppress the flaky report.

        :param add_option:
            A function that can add an option to the test runner.
            Its argspec should equal that of argparse.add_option.
        :type add_option:
            `callable`
        """
        add_option(
            '--no-flaky-report',
            action='store_false',
            dest='flaky_report',
            default=True,
            help="Suppress the report at the end of the "
                 "run detailing flaky test results.",
        )
        add_option(
            '--no-success-flaky-report',
            action='store_false',
            dest='flaky_success_report',
            default=True,
            help="Suppress reporting flaky test successes"
                 "in the report at the end of the "
                 "run detailing flaky test results.",
        )

    @staticmethod
    def add_force_flaky_options(add_option):
        """
        Add options to the test runner that force all tests to be flaky.

        :param add_option:
            A function that can add an option to the test runner.
            Its argspec should equal that of argparse.add_option.
        :type add_option:
            `callable`
        """
        add_option(
            '--force-flaky',
            action="store_true",
            dest="force_flaky",
            default=False,
            help="If this option is specified, we will treat all tests as "
                 "flaky."
        )
        add_option(
            '--max-runs',
            action="store",
            dest="max_runs",
            type=int,
            default=2,
            help="If --force-flaky is specified, we will run each test at "
                 "most this many times (unless the test has its own flaky "
                 "decorator)."
        )
        add_option(
            '--min-passes',
            action="store",
            dest="min_passes",
            type=int,
            default=1,
            help="If --force-flaky is specified, we will run each test at "
                 "least this many times (unless the test has its own flaky "
                 "decorator)."
        )

    def _add_flaky_report(self, stream):
        """
        Baseclass override. Write details about flaky tests to the test report.

        :param stream:
            The test stream to which the report can be written.
        :type stream:
            `file`
        """
        value = self._stream.getvalue()

        # Do not print report if there were no tests marked 'flaky' at all.
        if not self._had_flaky_tests and not value:
            return

        # If everything succeeded and --no-success-flaky-report is specified
        # don't print anything.
        if not self._flaky_success_report and not value:
            return

        stream.write('===Flaky Test Report===\n\n')

        # Python 2 will write to the stderr stream as a byte string, whereas
        # Python 3 will write to the stream as text. Only encode into a byte
        # string if the write tries to encode it first and raises a
        # UnicodeEncodeError.
        try:
            stream.write(value)
        except UnicodeEncodeError:
            stream.write(value.encode('utf-8', 'replace'))

        stream.write('\n===End Flaky Test Report===\n')

    @classmethod
    def _copy_flaky_attributes(cls, test, test_class):
        """
        Copy flaky attributes from the test callable or class to the test.

        :param test:
            The test that is being prepared to run
        :type test:
            :class:`nose.case.Test`
        """
        test_callable = cls._get_test_callable(test)
        if test_callable is None:
            return
        for attr, value in cls._get_flaky_attributes(test_class).items():
            already_set = hasattr(test, attr)
            if already_set:
                continue
            attr_on_callable = getattr(test_callable, attr, None)
            if attr_on_callable is not None:
                cls._set_flaky_attribute(test, attr, attr_on_callable)
            elif value is not None:
                cls._set_flaky_attribute(test, attr, value)

    @staticmethod
    def _get_flaky_attribute(test_item, flaky_attribute):
        """
        Gets an attribute describing the flaky test.

        :param test_item:
            The test method from which to get the attribute
        :type test_item:
            `callable` or :class:`nose.case.Test` or :class:`Function`
        :param flaky_attribute:
            The name of the attribute to get
        :type flaky_attribute:
            `unicode`
        :return:
            The test callable's attribute, or None if the test
            callable doesn't have that attribute.
        :rtype:
            varies
        """
        return getattr(test_item, flaky_attribute, None)

    @staticmethod
    def _set_flaky_attribute(test_item, flaky_attribute, value):
        """
        Sets an attribute on a flaky test. Uses magic __dict__ since setattr
        doesn't work for bound methods.

        :param test_item:
            The test callable on which to set the attribute
        :type test_item:
            `callable` or :class:`nose.case.Test` or :class:`Function`
        :param flaky_attribute:
            The name of the attribute to set
        :type flaky_attribute:
            `unicode`
        :param value:
            The value to set the test callable's attribute to.
        :type value:
            varies
        """
        test_item.__dict__[flaky_attribute] = value

    @classmethod
    def _increment_flaky_attribute(cls, test_item, flaky_attribute):
        """
        Increments the value of an attribute on a flaky test.

        :param test_item:
            The test callable on which to set the attribute
        :type test_item:
            `callable` or :class:`nose.case.Test` or :class:`Function`
        :param flaky_attribute:
            The name of the attribute to set
        :type flaky_attribute:
            `unicode`
        """
        cls._set_flaky_attribute(test_item, flaky_attribute, cls._get_flaky_attribute(test_item, flaky_attribute) + 1)

    @classmethod
    def _has_flaky_attributes(cls, test):
        """
        Returns True if the test callable in question is marked as flaky.

        :param test:
            The test that is being prepared to run
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :return:
        :rtype:
            `bool`
        """
        current_runs = cls._get_flaky_attribute(test, FlakyNames.CURRENT_RUNS)
        return current_runs is not None

    @classmethod
    def _get_flaky_attributes(cls, test_item):
        """
        Get all the flaky related attributes from the test.

        :param test_item:
            The test callable from which to get the flaky related attributes.
        :type test_item:
            `callable` or :class:`nose.case.Test` or :class:`Function`
        :return:
        :rtype:
            `dict` of `unicode` to varies
        """
        return {
            attr: cls._get_flaky_attribute(
                test_item,
                attr,
            ) for attr in FlakyNames()
        }

    @classmethod
    def _add_flaky_test_failure(cls, test, err):
        """
        Store test error information on the test callable.

        :param test:
            The flaky test on which to update the flaky attributes.
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :param err:
            Information about the test failure (from sys.exc_info())
        :type err:
            `tuple` of `class`, :class:`Exception`, `traceback`
        """
        errs = getattr(test, FlakyNames.CURRENT_ERRORS, None) or []
        cls._set_flaky_attribute(test, FlakyNames.CURRENT_ERRORS, errs)
        errs.append(err)

    @classmethod
    def _has_flaky_test_failed(cls, flaky):
        """
        Whether or not the flaky test has failed

        :param flaky:
            Dictionary of flaky attributes
        :type flaky:
            `dict` of `unicode` to varies
        :return:
            True if the flaky test should be marked as failure; False if
            it should be rerun.
        :rtype:
            `bool`
        """
        max_runs, current_runs, min_passes, current_passes = (
            flaky[FlakyNames.MAX_RUNS],
            flaky[FlakyNames.CURRENT_RUNS],
            flaky[FlakyNames.MIN_PASSES],
            flaky[FlakyNames.CURRENT_PASSES],
        )
        runs_left = max_runs - current_runs
        passes_needed = min_passes - current_passes
        no_retry = passes_needed > runs_left
        return no_retry and not cls._has_flaky_test_succeeded(flaky)

    @staticmethod
    def _has_flaky_test_succeeded(flaky):
        """
        Whether or not the flaky test has succeeded

        :param flaky:
            Dictionary of flaky attributes
        :type flaky:
            `dict` of `unicode` to varies
        :return:
            True if the flaky test should be marked as success; False if
            it should be rerun.
        :rtype:
            `bool`
        """
        return flaky[FlakyNames.CURRENT_PASSES] >= flaky[FlakyNames.MIN_PASSES]

    @classmethod
    def _get_test_callable(cls, test):
        """
        Get the test callable, from the test.

        :param test:
            The test that has raised an error or succeeded
        :type test:
            :class:`nose.case.Test` or :class:`pytest.Item`
        :return:
            The test declaration, callable and name that is being run
        :rtype:
            `callable`
        """
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def _get_test_callable_name(test):
        """
        Get the name of the test callable from the test.

        :param test:
            The test that has raised an error or succeeded
        :type test:
            :class:`nose.case.Test` or :class:`pytest.Item`
        :return:
            The name of the test callable that is being run by the test
        :rtype:
            `unicode`
        """
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def _make_test_flaky(cls, test, max_runs=None, min_passes=None, rerun_filter=None):
        """
        Make a given test flaky.

        :param test:
            The test in question.
        :type test:
            :class:`nose.case.Test` or :class:`Function`
        :param max_runs:
            The value of the FlakyNames.MAX_RUNS attribute to use.
        :type max_runs:
            `int`
        :param min_passes:
            The value of the FlakyNames.MIN_PASSES attribute to use.
        :type min_passes:
            `int`
        :param rerun_filter:
            Filter function to decide whether a test should be rerun if it fails.
            Function signature is as follows:
                (err, name, test, plugin) -> should_rerun
            - err (`tuple` of `class`, :class:`Exception`, `traceback`):
                Information about the test failure (from sys.exc_info())
            - name (`unicode`):
                The test name
            - test (:class:`nose.case.Test` or :class:`Function`):
                The test that has raised an error
            - plugin (:class:`FlakyNosePlugin` or :class:`FlakyPytestPlugin`):
                The flaky plugin. Has a :prop:`stream` that can be written to in
                order to add to the Flaky Report.
        :type rerun_filter:
            `callable`
        """
        attrib_dict = defaults.default_flaky_attributes(max_runs, min_passes, rerun_filter)
        for attr, value in attrib_dict.items():
            cls._set_flaky_attribute(test, attr, value)
