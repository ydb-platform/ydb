#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import assert_that, raises, not_

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase

throttling_exception_pattern = ".*</Message><Code>ThrottlingException</Code>.*"


class TestSqsThrottlingOnNonexistentQueue(KikimrSqsTestBase):

    def test_throttling_on_nonexistent_queue(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        nonexistent_queue_url = queue_url + "_nonex"

        def get_attributes_of_nonexistent_queue():
            self._sqs_api.get_queue_attributes(nonexistent_queue_url)

        # Draining budget
        for _ in range(16):
            try:
                get_attributes_of_nonexistent_queue()
            except Exception:
                pass

        assert_that(
            get_attributes_of_nonexistent_queue,
            raises(
                RuntimeError,
                pattern=throttling_exception_pattern
            )
        )

        assert_that(
            lambda: self._sqs_api.send_message(nonexistent_queue_url, "foobar"),
            raises(
                RuntimeError,
                pattern=throttling_exception_pattern
            )
        )

        assert_that(
            lambda: self._sqs_api.get_queue_url(self.queue_name + "_nonex"),
            raises(
                RuntimeError,
                pattern=throttling_exception_pattern
            )
        )

    def test_action_which_does_not_requere_existing_queue(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        nonexistent_queue_url = queue_url + "_nonex"

        def get_attributes_of_nonexistent_queue():
            self._sqs_api.get_queue_attributes(nonexistent_queue_url)

        # Draining budget
        for _ in range(16):
            try:
                get_attributes_of_nonexistent_queue()
            except Exception:
                pass

        assert_that(
            lambda: self._sqs_api.get_queue_url(self.queue_name + "_nonex"),
            raises(
                RuntimeError,
                pattern=throttling_exception_pattern
            )
        )

    def test_that_queue_can_be_created_despite_lack_of_throttling_budget(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        nonexistent_queue_url = queue_url + "_nonex"

        def get_attributes_of_nonexistent_queue():
            self._sqs_api.get_queue_attributes(nonexistent_queue_url)

        # Draining budget
        for _ in range(16):
            try:
                get_attributes_of_nonexistent_queue()
            except Exception:
                pass

        assert_that(
            lambda: self._create_queue_and_assert("other_queue_name", False, True),
            not_(raises(RuntimeError))
        )
