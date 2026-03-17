# -*- coding: utf-8 -*-
from __future__ import absolute_import, division
import re

__all__ = [
    'bias',
    'token_identity',
    'token_lower',
    'token_shape',
    'token_endswith_dot',
    'token_endswith_colon',
    'token_has_copyright',
    'number_pattern',
    'prefixes_and_suffixes',
    'PrefixFeatures',
    'SuffixFeatures',
]


def bias(html_token):
    return {'bias': 1}


def token_identity(html_token):
    return {'token': html_token.token}


def token_lower(html_token):
    return {'lower': html_token.token.lower()}


def token_shape(html_token):
    token = html_token.token
    return {
        'shape': _shape(token),
        'first_upper': token[0].isupper(),
    }


def token_endswith_dot(html_token):
    token = html_token.token
    return {'endswith_dot': token.endswith('.') and token != '.'}


def token_endswith_colon(html_token):
    token = html_token.token
    return {'endswith_colon': token.endswith(':') and token != ':'}


def token_has_copyright(html_token):
    return {'has_copyright': u'©' in html_token.token}


def number_pattern(html_token):
    token = html_token.token
    digit_ratio = sum(1 for ch in token if ch.isdigit()) / len(token)

    if digit_ratio >= 0.3:
        num_pattern = re.sub('\d', 'X', token)
        num_pattern2 = re.sub('[^X\W]', 'C', num_pattern)
        return {
            'num_pattern': num_pattern,
            'num_pattern2': num_pattern2,
        }
    else:
        return {}


class PrefixFeatures(object):
    def __init__(self, lenghts=(2,3,4), featname="prefix", lower=True):
        self.lower = lower
        self.featname = featname
        self.sizes = dict(
            zip(["%s%s" % (featname, i) for i in lenghts], lenghts)
        )

    def __call__(self, html_token):
        token = html_token.token if not self.lower else html_token.token.lower()
        return {key: token[:size] for key, size in self.sizes.items()}


class SuffixFeatures(object):
    def __init__(self, lenghts=(2,3,4), featname="suffix", lower=True):
        self.lower = lower
        self.sizes = dict(
            zip(["%s%s" % (featname, i) for i in lenghts], lenghts)
        )

    def __call__(self, html_token):
        token = html_token.token if not self.lower else html_token.token.lower()
        return {key: token[-size:] for key, size in self.sizes.items()}


def prefixes_and_suffixes(html_token):
    token = html_token.token.lower()
    return {
        'prefix2': token[:2],
        'suffix2': token[-2:],
        'prefix3': token[:3],
        'suffix3': token[-3:],
        'prefix4': token[:4],
        'suffix4': token[-4:],
    }


# stolen from NLTK source (nltk.tag.sequential.ClassifierBasedPOSTagger)
def _shape(token):
    if re.match('[-+]?[0-9]+(\.[0-9]*)?|[0-9]*\.[0-9]+$', token):
        return 'number'
    elif re.match('\W+$', token):
        return 'punct'
    elif re.match("[A-Z][a-z'`]+$", token):
        return 'upcase'
    elif re.match("[A-Z][A-Z'`]+$", token):
        return 'caps'
    elif re.match("[a-z]+$", token):
        return 'downcase'
    elif re.match('\w+$', token):
        return 'mixedcase'
    else:
        return 'other'
