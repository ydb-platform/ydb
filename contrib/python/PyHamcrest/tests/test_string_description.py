#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
 
from hamcrest import assert_that, empty, equal_to 
 
__author__ = 'asatarin@yandex-team.ru' 
 
 
def test_string_description_is_fast(): 
    list_of_very_long_strings = ["aa"*1000 for _ in range(10000)] 
    try: 
        assert_that(list_of_very_long_strings, empty()) 
        x = 0 
    except AssertionError as e: 
        x = len(str(e)) 
 
    assert_that(x, equal_to(20040048)) 
