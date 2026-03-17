# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, division


def add_parse_if_not_seen(parse, result_list, seen_parses):
    try:
        para_id = parse[4][0][2]
    except IndexError:
        para_id = None

    word = parse[0]
    tag = parse[1]

    reduced_parse = word, tag, para_id

    if reduced_parse in seen_parses:
        return
    seen_parses.add(reduced_parse)
    result_list.append(parse)


def add_tag_if_not_seen(tag, result_list, seen_tags):
    if tag in seen_tags:
        return
    seen_tags.add(tag)
    result_list.append(tag)


def with_suffix(form, suffix):
    """ Return a new form with ``suffix`` attached """
    word, tag, normal_form, score, methods_stack = form
    return (word+suffix, tag, normal_form+suffix, score, methods_stack)


def without_fixed_suffix(form, suffix_length):
    """ Return a new form with ``suffix_length`` chars removed from right """
    word, tag, normal_form, score, methods_stack = form
    return (word[:-suffix_length], tag, normal_form[:-suffix_length],
            score, methods_stack)


def without_fixed_prefix(form, prefix_length):
    """ Return a new form with ``prefix_length`` chars removed from left """
    word, tag, normal_form, score, methods_stack = form
    return (word[prefix_length:], tag, normal_form[prefix_length:],
            score, methods_stack)


def with_prefix(form, prefix):
    """ Return a new form with ``prefix`` added """
    word, tag, normal_form, score, methods_stack = form
    return (prefix+word, tag, prefix+normal_form, score, methods_stack)


def replace_methods_stack(form, new_methods_stack):
    """
    Return a new form with ``methods_stack``
    replaced with ``new_methods_stack``
    """
    return form[:4] + (new_methods_stack,)


def without_last_method(form):
    """ Return a new form without last method from methods_stack """
    stack = form[4][:-1]
    return form[:4] + (stack,)


def append_method(form, method):
    """ Return a new form with ``method`` added to methods_stack """
    stack = form[4]
    return form[:4] + (stack+(method,),)
