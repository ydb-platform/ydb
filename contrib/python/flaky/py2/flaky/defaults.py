# coding: utf-8

from flaky.names import FlakyNames


def _true(*args):
    """
    Default rerun filter function that always returns True.
    """
    # pylint:disable=unused-argument
    return True


class FilterWrapper(object):
    """
    Filter function wrapper. Expected to be called as though it's a filter
    function. Since @flaky adds attributes to a decorated class, Python wants
    to turn a bare function into an unbound method, which is not what we want.
    """
    def __init__(self, rerun_filter):
        self._filter = rerun_filter

    def __call__(self, *args, **kwargs):
        return self._filter(*args, **kwargs)


def default_flaky_attributes(max_runs=None, min_passes=None, rerun_filter=None):
    """
    Returns the default flaky attributes to set on a flaky test.

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
    :type rerun_filter:
        `callable`
    :return:
        Default flaky attributes to set on a flaky test.
    :rtype:
        `dict`
    """
    if max_runs is None:
        max_runs = 2
    if min_passes is None:
        min_passes = 1
    if min_passes <= 0:
        raise ValueError('min_passes must be positive')
    if max_runs < min_passes:
        raise ValueError('min_passes cannot be greater than max_runs!')

    return {
        FlakyNames.MAX_RUNS: max_runs,
        FlakyNames.MIN_PASSES: min_passes,
        FlakyNames.CURRENT_RUNS: 0,
        FlakyNames.CURRENT_PASSES: 0,
        FlakyNames.RERUN_FILTER: FilterWrapper(rerun_filter or _true),
    }
