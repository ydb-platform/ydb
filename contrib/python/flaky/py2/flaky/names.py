# coding: utf-8

from __future__ import unicode_literals


class FlakyNames(object):
    """
    Names of flaky attributes that will be added to flaky tests
    """
    CURRENT_ERRORS = '_flaky_current_errors'
    CURRENT_RUNS = '_flaky_current_runs'
    CURRENT_PASSES = '_flaky_current_passes'
    MAX_RUNS = '_flaky_max_runs'
    MIN_PASSES = '_flaky_min_passes'
    RERUN_FILTER = '_flaky_rerun_filter'

    def items(self):
        return (
            self.CURRENT_ERRORS,
            self.CURRENT_PASSES,
            self.CURRENT_RUNS,
            self.MAX_RUNS,
            self.MIN_PASSES,
            self.RERUN_FILTER,
        )

    def __iter__(self):
        for attr in self.items():
            yield attr
