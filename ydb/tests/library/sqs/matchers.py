#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import equal_to, less_than_or_equal_to, contains_inanyorder, contains, greater_than_or_equal_to, all_of, \
    has_item
from hamcrest.core.base_matcher import BaseMatcher

from ydb.tests.library.matchers.collection import IsSublistOf


def extract_field(read_response, field):
    if read_response is None:
        return []
    return [i[field] for i in read_response]


def extract_message_ids(read_response):
    return extract_field(read_response, 'MessageId')


def extract_message_data(read_response):
    return extract_field(read_response, 'Body')


class ReadResponseMatcher(BaseMatcher):
    def __init__(self):
        self.__message_count_matcher = None
        self.__message_ids_matcher = None
        self.__msg_id_to_data = {}
        self.__message_ids = None

        self.__messages_data = None
        self.__absent_message_id = None
        self.__description = "read result "
        self.__mismatch_reason = ""

    @staticmethod
    def __list_startswith(full_list, sublist):
        for i, v in enumerate(sublist):
            if full_list[i] != v:
                return False
        return True

    def with_n_messages(self, messages_count):
        # self.__description += " with {} messages".format(messages_count)
        self.__message_count_matcher = equal_to(messages_count)
        return self

    def with_n_of_fewer_messages(self, messages_count):
        # self.__description += " with {} or fewer messages".format(messages_count)
        self.__message_count_matcher = less_than_or_equal_to(messages_count)
        return self

    def with_n_or_more_messages(self, messages_count):
        self.__message_count_matcher = greater_than_or_equal_to(messages_count)
        return self

    def with_message_ids(self, message_ids):
        self.__message_ids_matcher = contains_inanyorder(*message_ids)
        self.__message_ids = message_ids
        return self

    def with_some_of_message_ids(self, message_ids):
        self.__message_ids = message_ids
        self.__message_ids_matcher = IsSublistOf(message_ids)
        return self

    def with_these_or_more_message_ids(self, message_ids):
        self.__message_ids = message_ids
        self.__message_ids_matcher = all_of(
            *[has_item(m_id) for m_id in message_ids]
        )
        return self

    def in_order(self):
        self.__description += " with exact message_id order:\n {}\n"
        if not self.__message_ids:
            raise ValueError("Called in_order, but message_ids to match are not set")
        self.__message_ids_matcher = contains(self.__message_ids)
        return self

    def with_messages_data(self, messages_data):
        if self.__message_ids is None:
            raise ValueError("Must set message_ids to verify message data")
        if len(self.__message_ids) != len(messages_data):
            raise ValueError("Must provide message data for same count of messages as message ids")
        for i, msg_id in enumerate(self.__message_ids):
            self.__msg_id_to_data[msg_id] = messages_data[i]
        # self.__description += " with messages containing following data:\n {}\n".format(';'.join(messages_data))
        return self

    def without_message_id(self, message_id):
        self.__description += " without message_id: {}".format(message_id)
        self.__absent_message_id = message_id
        return self

    def describe_to(self, description):
        description.append_text(self.__description)
        if self.__message_count_matcher is not None:
            description.append_text("with messages count: ")
            self.__message_count_matcher.describe_to(description)
            description.append_text('\n')
        if self.__message_ids_matcher is not None:
            description.append_text("with message_ids: ")
            self.__message_ids_matcher.describe_to(description)
            description.append_text('\n')
        if self.__msg_id_to_data:
            description.append_text("with message_data:\n{}\n".format(self.__msg_id_to_data.values()))

    def describe_mismatch(self, actual_response, mismatch_description):
        mismatch_description.append_text("actual result was: messages\n{}\n".format(
            extract_message_ids(actual_response)
        ))
        # message_ids = extract_message_ids(actual_response)
        # if self.__message_count_matcher is not None:
        #     self.__message_count_matcher.describe_mismatch(message_ids, mismatch_description)
        # if self.__message_ids_matcher is not None:
        #     self.__message_ids_matcher.describe_mismatch(len(message_ids), mismatch_description)
        if self.__msg_id_to_data:
            mismatch_description.append_text("and data: {}\n".format(self.__msg_id_to_data.values()))
        if self.__mismatch_reason:
            mismatch_description.append_text("Mismatch reason was: {}".format(self.__mismatch_reason))

    def _matches(self, actual_response):
        actual_message_ids = extract_message_ids(actual_response)
        if len(actual_message_ids) != len(set(actual_message_ids)):
            self.__mismatch_reason += "Duplicate messages appear in result"
            return False
        if self.__message_count_matcher is not None:
            messages_count = len(actual_message_ids)
            if not self.__message_count_matcher.matches(messages_count):
                self.__mismatch_reason += "Messages count didn't match\n"
                return False
        if self.__absent_message_id is not None and self.__absent_message_id is actual_message_ids:
            self.__mismatch_reason += "Message with {} appeared in results"
            return False
        if self.__message_ids_matcher is not None and not self.__message_ids_matcher.matches(actual_message_ids):
            self.__mismatch_reason += "Message ids didn't match"
            return False
        if self.__msg_id_to_data:
            actual_data = extract_message_data(actual_response)
            expected_data = [self.__msg_id_to_data[i] for i in actual_message_ids]
            if sorted(actual_data) != sorted(expected_data):
                self.__mismatch_reason += "Message data didn't match"
                return False
        return True
