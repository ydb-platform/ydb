#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

from hamcrest import assert_that
from hamcrest.core.helpers.wrap_matcher import wrap_matcher


logger = logging.getLogger()


def wait_for(predicate, timeout_seconds=5.0, step_seconds=0.5, multiply=2, max_step_seconds=5.0):
    finish_time = time.time() + timeout_seconds
    while time.time() <= finish_time:
        if predicate():
            return True
        step_seconds = min(step_seconds * multiply, max_step_seconds)
        time.sleep(step_seconds)
    return False


def wait_for_matcher(callable_, matcher, timeout_seconds=60.0, step_seconds=1.0, log_progress=False):
    """
    Wait for callable result to match given matcher

    :param callable_: zero argument function
    :param matcher: Hamcrest matcher for callable_ result
    :param timeout_seconds:
    :param step_seconds:
    :param log_progress: do we need to log intermediate progress to debug log

    :return: True if matches before timeout reached, False otherwise
    """
    matcher = wrap_matcher(matcher)
    horizontal_line = '\n' + '=' * 80 + '\n'
    last_result = [None]

    def predicate():
        result = callable_()
        if log_progress:
            logger.info('Result from callable = ' + horizontal_line + str(result) + horizontal_line)
        last_result[0] = result
        return matcher.matches(result)

    wait_for(predicate=predicate, timeout_seconds=timeout_seconds, step_seconds=step_seconds)
    return last_result[0]


def wait_for_and_assert(callable_, matcher, message='', timeout_seconds=60, step_seconds=1.0, log_progress=False):
    """
    Wait for callable result to match given matcher and when asserts that.

    :raise AssertionError: then callable result is not matched by the matcher after timeout

    :param callable_: zero argument function
    :param matcher: Hamcrest matcher for callable_ result
    :param message:
    :param timeout_seconds:
    :param step_seconds:
    :param log_progress: do we need to log intermediate progress to debug log

    :return: return value of last callable_ invocation
    """
    matcher = wrap_matcher(matcher)
    result = wait_for_matcher(
        callable_,
        matcher,
        timeout_seconds=timeout_seconds,
        step_seconds=step_seconds,
        log_progress=log_progress,
    )
    assert_that(result, matcher, message)
    return result
