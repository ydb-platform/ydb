# coding: utf-8

from __future__ import unicode_literals

from _pytest.runner import call_runtest_hook  # pylint:disable=import-error

from flaky._flaky_plugin import _FlakyPlugin
from flaky.utils import ensure_unicode_string


def _get_worker_output(item):
    worker_output = None
    if hasattr(item, 'workeroutput'):
        worker_output = item.workeroutput
    elif hasattr(item, 'slaveoutput'):
        worker_output = item.slaveoutput
    return worker_output


class FlakyXdist(object):

    def __init__(self, plugin):
        super(FlakyXdist, self).__init__()
        self._plugin = plugin

    def pytest_testnodedown(self, node, error):
        """
        Pytest hook for responding to a test node shutting down.
        Copy worker flaky report output so it's available on the master flaky report.
        """
        # pylint: disable=unused-argument, no-self-use
        worker_output = _get_worker_output(node)
        if worker_output is not None and 'flaky_report' in worker_output:
            self._plugin.stream.write(worker_output['flaky_report'])


class FlakyPlugin(_FlakyPlugin):
    """
    Plugin for pytest that allows retrying flaky tests.

    """
    runner = None
    flaky_report = True
    force_flaky = False
    max_runs = None
    min_passes = None
    config = None
    _call_infos = {}
    _PYTEST_WHEN_SETUP = 'setup'
    _PYTEST_WHEN_CALL = 'call'
    _PYTEST_WHENS = (_PYTEST_WHEN_SETUP, _PYTEST_WHEN_CALL)
    _PYTEST_OUTCOME_PASSED = 'passed'
    _PYTEST_OUTCOME_FAILED = 'failed'
    _PYTEST_EMPTY_STATUS = ('', '', '')

    def pytest_runtest_protocol(self, item, nextitem):
        """
        Pytest hook to override how tests are run.

        Runs a test collected by pytest.
        - First, monkey patches the builtin runner module to call back to
        FlakyPlugin.call_runtest_hook rather than its own.
        - Then defers to the builtin runner module to run the test,
        and repeats the process if the test needs to be rerun.
        - Reports test results to the flaky report.

        :param item:
            pytest wrapper for the test function to be run
        :type item:
            :class:`Function`
        :param nextitem:
            pytest wrapper for the next test function to be run
        :type nextitem:
            :class:`Function`
        :return:
            True if no further hook implementations should be invoked.
        :rtype:
            `bool`
        """
        test_instance = self._get_test_instance(item)
        self._copy_flaky_attributes(item, test_instance)
        if self.force_flaky and not self._has_flaky_attributes(item):
            self._make_test_flaky(
                item,
                self.max_runs,
                self.min_passes,
            )
        original_call_and_report = self.runner.call_and_report
        self._call_infos[item] = {}
        should_rerun = True
        try:
            self.runner.call_and_report = self.call_and_report
            while should_rerun:
                self.runner.pytest_runtest_protocol(item, nextitem)
                call_info = None
                excinfo = None
                for when in self._PYTEST_WHENS:
                    call_info = self._call_infos.get(item, {}).get(when, None)
                    excinfo = getattr(call_info, 'excinfo', None)
                    if excinfo is not None:
                        break

                if call_info is None:
                    return False
                passed = excinfo is None
                if passed:
                    should_rerun = self.add_success(item)
                else:
                    skipped = excinfo.typename == 'Skipped'
                    should_rerun = not skipped and self.add_failure(item, excinfo)
                    if not should_rerun:
                        item.excinfo = excinfo
        finally:
            self.runner.call_and_report = original_call_and_report
            del self._call_infos[item]
        return True

    def call_and_report(self, item, when, log=True, **kwds):
        """
        Monkey patched from the runner plugin. Responsible for running
        the test and reporting the outcome.
        Had to be patched to avoid reporting about test retries.

        :param item:
            pytest wrapper for the test function to be run
        :type item:
            :class:`Function`
        :param when:
            The stage of the test being run. Usually one of 'setup', 'call', 'teardown'.
        :type when:
            `str`
        :param log:
            Whether or not to report the test outcome. Ignored for test
            retries; flaky doesn't report test retries, only the final outcome.
        :type log:
            `bool`
        """
        call = call_runtest_hook(item, when, **kwds)
        self._call_infos[item][when] = call
        hook = item.ihook
        report = hook.pytest_runtest_makereport(item=item, call=call)
        # Start flaky modifications
        # only retry on call, not setup or teardown
        if report.when in self._PYTEST_WHENS:
            if report.outcome == self._PYTEST_OUTCOME_PASSED:
                if self._should_handle_test_success(item):
                    log = False
            elif report.outcome == self._PYTEST_OUTCOME_FAILED:
                err, name = self._get_test_name_and_err(item, when)
                if self._will_handle_test_error_or_failure(item, name, err):
                    log = False
        # End flaky modifications
        if log:
            hook.pytest_runtest_logreport(report=report)
        if self.runner.check_interactive_exception(call, report):
            hook.pytest_exception_interact(node=item, call=call, report=report)
        return report

    def _get_test_name_and_err(self, item, when):
        """
        Get the test name and error tuple from a test item.

        :param item:
            pytest wrapper for the test function to be run
        :type item:
            :class:`Function`
        :return:
            The test name and error tuple.
        :rtype:
            ((`type`, :class:`Exception`, :class:`Traceback`) or (None, None, None), `unicode`)
        """
        name = self._get_test_callable_name(item)
        call_info = self._call_infos.get(item, {}).get(when, None)
        if call_info is not None and call_info.excinfo:
            err = (call_info.excinfo.type, call_info.excinfo.value, call_info.excinfo.tb)
        else:
            err = (None, None, None)
        return err, name

    def pytest_terminal_summary(self, terminalreporter):
        """
        Pytest hook to write details about flaky tests to the test report.

        Write details about flaky tests to the test report.

        :param terminalreporter:
            Terminal reporter object. Supports stream writing operations.
        :type terminalreporter:
            :class: `TerminalReporter`
        """
        if self.flaky_report:
            self._add_flaky_report(terminalreporter)

    def pytest_addoption(self, parser):
        """
        Pytest hook to add an option to the argument parser.

        :param parser:
            Parser for command line arguments and ini-file values.
        :type parser:
            :class:`Parser`
        """
        self.add_report_option(parser.addoption)

        group = parser.getgroup(
            "Force flaky", "Force all tests to be flaky.")
        self.add_force_flaky_options(group.addoption)

    def pytest_configure(self, config):
        """
        Pytest hook to get information about how the test run has been configured.

        :param config:
            The pytest configuration object for this test run.
        :type config:
            :class:`Configuration`
        """
        self.flaky_report = config.option.flaky_report
        self.flaky_success_report = config.option.flaky_success_report
        self.force_flaky = config.option.force_flaky
        self.max_runs = config.option.max_runs
        self.min_passes = config.option.min_passes
        self.runner = config.pluginmanager.getplugin("runner")

        if config.pluginmanager.hasplugin('xdist'):
            config.pluginmanager.register(FlakyXdist(self), name='flaky.xdist')
            self.config = config
        worker_output = _get_worker_output(config)
        if worker_output is not None:
            worker_output['flaky_report'] = ''

        config.addinivalue_line('markers', 'flaky: marks tests to be automatically retried upon failure')

    def pytest_runtest_setup(self, item):
        """
        Pytest hook to modify the test before it's run.

        :param item:
            The test item.
        """
        if not self._has_flaky_attributes(item):
            if hasattr(item, 'iter_markers'):
                for marker in item.iter_markers(name='flaky'):
                    self._make_test_flaky(item, *marker.args, **marker.kwargs)
                    break
            elif hasattr(item, 'get_marker'):
                marker = item.get_marker('flaky')
                if marker:
                    self._make_test_flaky(item, *marker.args, **marker.kwargs)

    def pytest_sessionfinish(self):
        """
        Pytest hook to take a final action after the session is complete.
        Copy flaky report contents so that the master process can read it.
        """
        worker_output = _get_worker_output(self.config)
        if worker_output is not None:
            worker_output['flaky_report'] += self.stream.getvalue()

    @property
    def stream(self):
        return self._stream

    @property
    def flaky_success_report(self):
        """
        Property for setting whether or not the plugin will print results about
        flaky tests that were successful.

        :return:
            Whether or not flaky will report on test successes.
        :rtype:
            `bool`
        """
        return self._flaky_success_report

    @flaky_success_report.setter
    def flaky_success_report(self, value):
        """
        Property for setting whether or not the plugin will print results about
        flaky tests that were successful.

        :param value:
            Whether or not flaky will report on test successes.
        :type value:
            `bool`
        """
        self._flaky_success_report = value

    @staticmethod
    def _get_test_instance(item):
        """
        Get the object containing the test. This might be `test.instance`
        or `test.parent.obj`.
        """
        test_instance = getattr(item, 'instance', None)
        if test_instance is None:
            if hasattr(item, 'parent') and hasattr(item.parent, 'obj'):
                test_instance = item.parent.obj
        return test_instance

    def add_success(self, item):
        """
        Called when a test succeeds.

        Count remaining retries and compare with number of required successes
        that have not yet been achieved; retry if necessary.

        :param item:
            pytest wrapper for the test function that has succeeded
        :type item:
            :class:`Function`
        """
        return self._handle_test_success(item)

    def add_failure(self, item, err):
        """
        Called when a test fails.

        Count remaining retries and compare with number of required successes
        that have not yet been achieved; retry if necessary.

        :param item:
            pytest wrapper for the test function that has succeeded
        :type item:
            :class:`Function`
        :param err:
            Information about the test failure
        :type err:
            :class: `ExceptionInfo`
        """
        if err is not None:
            error = (err.type, err.value, err.traceback)
        else:
            error = (None, None, None)
        return self._handle_test_error_or_failure(item, error)

    @staticmethod
    def _get_test_callable_name(test):
        """
        Base class override.
        """
        return test.name

    @classmethod
    def _get_test_callable(cls, test):
        """
        Base class override.

        :param test:
            The test that has raised an error or succeeded
        :type test:
            :class:`Function`
        :return:
            The test declaration, callable and name that is being run
        :rtype:
            `tuple` of `object`, `callable`, `unicode`
        """
        callable_name = cls._get_test_callable_name(test)
        if callable_name.endswith(']') and '[' in callable_name:
            unparametrized_name = callable_name[:callable_name.index('[')]
        else:
            unparametrized_name = callable_name
        test_instance = cls._get_test_instance(test)
        if hasattr(test_instance, callable_name):
            # Test is a method of a class
            def_and_callable = getattr(test_instance, callable_name)
            return def_and_callable
        if hasattr(test_instance, unparametrized_name):
            # Test is a parametrized method of a class
            def_and_callable = getattr(test_instance, unparametrized_name)
            return def_and_callable
        if hasattr(test, 'module'):
            if hasattr(test.module, callable_name):
                # Test is a function in a module
                def_and_callable = getattr(test.module, callable_name)
                return def_and_callable
            if hasattr(test.module, unparametrized_name):
                # Test is a parametrized function in a module
                def_and_callable = getattr(test.module, unparametrized_name)
                return def_and_callable
        elif hasattr(test, 'runtest'):
            # Test is a doctest or other non-Function Item
            return test.runtest
        return None

    def _mark_test_for_rerun(self, test):
        """Base class override. Rerun a flaky test."""

    def _log_test_failure(self, test_callable_name, err, message):
        """
        Add messaging about a test failure to the stream, which will be
        printed by the plugin's report method.
        """
        self._stream.writelines([
            ensure_unicode_string(test_callable_name),
            message,
            '\n\t',
            ensure_unicode_string(err[0]),
            '\n\t',
            ensure_unicode_string(err[1]),
            '\n\t',
            ensure_unicode_string(err[2]),
            '\n',
        ])


PLUGIN = FlakyPlugin()
# pytest only processes hooks defined on the module
# find all hooks defined on the plugin class and copy them to the module globals
for _pytest_hook in dir(PLUGIN):
    if _pytest_hook.startswith('pytest_'):
        globals()[_pytest_hook] = getattr(PLUGIN, _pytest_hook)
