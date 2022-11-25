#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher
from hamcrest.library.collection.is_empty import IsEmpty
from hamcrest.library.collection.issequence_containinginorder import IsSequenceContainingInOrder


class IsEmptyNice(IsEmpty):
    def describe_mismatch(self, item, mismatch_description):
        try:
            mismatch_description.append_text('was list of items:').append_list(
                start=('\n' + '=' * 80 + '\n'), separator='\n\n', end='\n' + '=' * 80, list=item
            )
        except TypeError:
            mismatch_description.append_text('was ').append_description_of(item)


class IsSequenceContainingInOrderNewLine(IsSequenceContainingInOrder):

    def describe_to(self, description):
        description.append_text('a sequence containing ')   \
                   .append_list('[\n', ',\n  ', '\n]', self.matchers)


class SequenceHasUniqueElements(BaseMatcher):

    def describe_mismatch(self, actual_sequence, mismatch_description):
        visited = set()
        non_unique_elements = []
        for item in actual_sequence:
            if item in visited:
                non_unique_elements.append(item)
            visited.add(item)
        non_unique_elements.sort()
        mismatch_description.append_list('non unique elements [', ', ', ']', non_unique_elements)
        return

    def describe_to(self, description):
        description.append_text("Sequence must contain unique elements")

    def _matches(self, actual_sequence):
        return len(set(actual_sequence)) == len(actual_sequence)


class IsSublistOf(BaseMatcher):
    def __init__(self, full_list):
        super(IsSublistOf, self).__init__()
        self.__full_list = full_list
        self.__not_matched = None

    def _matches(self, small_list):
        small = sorted(small_list)
        full = sorted(self.__full_list)
        i = 0
        for item in small:
            while i < len(full):
                i += 1
                if item == full[i-1]:
                    break
            else:
                self.__not_matched = item
                return False
        return True

    def describe_mismatch(self, actual_sequence, mismatch_description):
        mismatch_description.append_text('But was: {}. At least on not-matching item was {}'.format(
            actual_sequence, self.__not_matched
        ))
        return

    def describe_to(self, description):
        description.append_text("Expect a sub-list of following items: {}".format(self.__full_list))


def contains(*items):
    """
    Same as hamcrest.contains() but adds new lines between list elements in description

    :param items:

    :return: hamcrest.contains() matcher with nicer mismatch diagnostics
    """
    matchers = []
    for item in items:
        matchers.append(wrap_matcher(item))
    return IsSequenceContainingInOrderNewLine(matchers)


def is_empty():
    return IsEmptyNice()


def sequence_has_unique_elements():
    return SequenceHasUniqueElements()
