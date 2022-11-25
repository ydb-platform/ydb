#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import string

__hex_digits = '0123456789ABCDEF'


def int_between(lo, hi):
    def closure():
        while True:
            yield random.randint(lo, hi)
    return closure


def one_of(list_of_values):
    def closure():
        while True:
            yield random.choice(list_of_values)
    return closure


def float_in(lo, hi):
    def closure():
        while True:
            yield random.uniform(lo, hi)
    return closure


def float_as_hex(byte_length=4):
    def closure():
        while True:
            yield 'x"' + ''.join([random.choice(__hex_digits) for _ in range(byte_length * 2)]) + '"'
    return closure


# TODO use string.printable for character generation or even wider range (e.g. UTF-8)
def string_with_length(length):
    def closure():
        while True:
            yield ''.join([random.choice(string.ascii_letters) for _ in range(length)])
    return closure


def actor_id():
    def closure():
        while True:
            yield "(%d,%d,%d)" % (int_between(1, 100), int_between(1, 100), int_between(1, 100))
    return closure
