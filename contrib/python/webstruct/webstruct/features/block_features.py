# -*- coding: utf-8 -*-
from __future__ import absolute_import

__all__ = ['parent_tag', 'InsideTag', 'borders', 'block_length']

def _inside_tag(elem, tagname):
    """
    >>> from lxml.html import fragment_fromstring
    >>> root = fragment_fromstring('<div><i>foo</i><strong><p>head 1</p></strong></div>')
    >>> elem = list(root.iter('p'))[0]
    >>> _inside_tag(elem, 'strong')
    True
    >>> _inside_tag(elem, 'div')
    True
    >>> _inside_tag(elem, 'p')
    True
    >>> _inside_tag(elem, 'span')
    False
    >>> _inside_tag(elem, 'i')
    False
    """
    if elem.tag == tagname:
        return True
    return any(e is not None for e in elem.iterancestors(tagname))


def parent_tag(html_token):
    return {'parent_tag': html_token.parent.tag}


class InsideTag(object):
    def __init__(self, tagname):
        self.tagname = tagname
        self.key = 'inside_tag_' + tagname

    def __call__(self, html_token):
        return {self.key: _inside_tag(html_token.elem, self.tagname)}


def borders(html_token):
    return {
        'border_at_left': html_token.index == 0,
        'border_at_right': html_token.index == len(html_token.tokens)-1,
    }


def block_length(html_token):
    block_len = len(html_token.tokens)
    if block_len == 1:
        bl = '1'
    elif 1 < block_len <= 10:
        bl = 'short'
    elif 10 < block_len <= 20:
        bl = 'medium'
    else:
        bl = 'large'
    return {'block_length': bl}
