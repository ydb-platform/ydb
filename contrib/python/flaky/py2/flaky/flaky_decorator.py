# coding: utf-8

from __future__ import unicode_literals

from flaky.defaults import default_flaky_attributes


def flaky(max_runs=None, min_passes=None, rerun_filter=None):
    """
    Decorator used to mark a test as "flaky". When used in conjuction with
    the flaky nosetests plugin, will cause the decorated test to be retried
    until min_passes successes are achieved out of up to max_runs test runs.

    :param max_runs:
        The maximum number of times the decorated test will be run.
    :type max_runs:
        `int`
    :param min_passes:
        The minimum number of times the test must pass to be a success.
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
    :return:
        A wrapper function that includes attributes describing the flaky test.
    :rtype:
        `callable`
    """
    # In case @flaky is applied to a function or class without arguments
    # (and without parentheses), max_runs will refer to the wrapped object.
    # In this case, the default value can be used.
    wrapped = None
    if hasattr(max_runs, '__call__'):
        wrapped, max_runs = max_runs, None

    attrib = default_flaky_attributes(max_runs, min_passes, rerun_filter)

    def wrapper(wrapped_object):
        for name, value in attrib.items():
            setattr(wrapped_object, name, value)
        return wrapped_object

    return wrapper(wrapped) if wrapped is not None else wrapper
