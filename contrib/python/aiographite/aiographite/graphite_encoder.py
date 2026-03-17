#!/usr/bin/python
# -*- coding: utf-8 -*-

import urllib.parse

"""
    Naming metrics schema:
    <namespace>.<instrumented section>.<target>.<action>
"""

"""
    Limitation and explaination:

    Since we are using IDNA(International Domain Name in Application) rules,
    there are several limitations we should pay attention to.

    The conversions between ASCII and non-ASCII forms of a domain name are
    accomplished by algorithms called ToASCII and ToUnicode.
    These algorithms are not applied to the domain name as a whole, but
    rather to individual labels. For example,
    if the domain name is www.example.com, then the labels are www, example,
    and com. ToASCII or ToUnicode are applied
    to each of these three separately.

    Ref: https://en.wikipedia.org/wiki/Internationalized_domain_name

    FAIL CASES:
    '.fd': starting with 'dot'
    'a..a': continuous 'dot'
    very long string: exceed the 63-character
"""

"""
    For special characters :
    1.  The dot (.) is a special character because it delineates each metric’s
        path component, but this is an easy fix; just substitute all dots for
        underscores or '%2E'.
        For example, www.zillow.com => www_zillow_com / www%2Ezillow%2Ecom
    2   For the rest of the special characters(except dot), just URL any
        metric name with special characters to make it valid for Graphite,
        and then URL decode it when we need to reconstruct the information.
"""


class GraphiteEncoder:
    """
    Graphite expects everything to be just ASCII to split/processing them,
    and then make directories based on metric name. So any special name
    not allow to appear in directory/file name is not supported by Graphite.

    GraphiteEncoder is designed to help users to send valid metric
    name to graphite.

    Metrics:
    <section_name>.<section_name>.<section_name>.<section_name>
    """

    @staticmethod
    def encode(section_name):
        """
        This method helps to encode any input metric name to a valid graphite
        metric name.

        args: section name(could include any character), a string

        returns valid metric name for graphite
        """
        valid_graphite_metric_name = ""
        try:
            valid_graphite_metric_name = urllib.parse\
                .quote(section_name.encode('punycode')).replace(".", "%2E")
        except Exception as e:
            raise e
        return valid_graphite_metric_name

    @staticmethod
    def decode(idna_str):
        """
        This method helps to decode a valid metric name in graphite to its
        original metric name.

        args: a valid metric name in graphite.

        returns the original metric name.
        """
        display_metric_name = ""
        try:
            display_metric_name = \
                bytes(urllib.parse.unquote(idna_str),
                      'utf-8').decode('punycode')
        except Exception as e:
            raise e
        return display_metric_name
