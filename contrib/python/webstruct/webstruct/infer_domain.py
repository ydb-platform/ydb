# -*- coding: utf-8 -*-
"""
Module for getting a most likely base URL (domain) for a page.
It is useful if you've downloaded HTML files, but haven't preserved URLs
explicitly, and still want to have cross-validation done right.
Grouping pages by domain name is a reasonable way to do that.

WebAnnotator data has either <base> tags with original URLs 
(or at least original domains), or a commented out base tags.

Unfortunately, GATE-annotated data doesn't have this tag. 
So the idea is to use a most popular domain mentioned in a page as 
a page's domain.
"""
import re
from collections import Counter

from webstruct.utils import get_domain


_find_base_href = re.compile(r'base\s+href="(.*)"').search
_DOMAIN_BLACKLIST = {
    'google.com', 'twitter.com', 'facebook.com', 'youtube.com',
    'fonts.com', 'googleapis.com', 'fonts.net', 'addthis.com',
    'flickr.com', 'paypal.com', 'pinterest.com', 'linkedin.com',
}


def get_tree_domain(tree, blacklist=_DOMAIN_BLACKLIST, get_domain=get_domain):
    """
    Return the most likely domain for the tree. Domain is extracted from base
    tag or guessed if there is no base tag. If domain can't be detected an
    empty string is returned.
    """
    href = get_base_href(tree)
    if href:
        return get_domain(href)
    return guess_domain(tree, blacklist, get_domain)


def guess_domain(tree, blacklist=_DOMAIN_BLACKLIST, get_domain=get_domain):
    """ Return most common domain not in a black list. """
    domains = [get_domain(href) for href in tree.xpath('//*/@href')]
    domains = [d for d in domains if d and d not in blacklist]
    if not domains:
        return ''  # unknown
    cnt = Counter(domains)
    max_count = cnt.most_common(1)[0][1]
    top_domains = [k for k, v in cnt.items() if v == max_count]
    return sorted(top_domains)[0]


def get_base_href(tree):
    """ Return href of a base tag; base tag could be commented out. """
    href = _get_base_href(tree)
    if href:
        return href
    return _get_commented_base_href(tree)


def _get_commented_base_href(tree):
    """ Return href value found in a commented out <base> tag """
    for comment in tree.xpath('//head/comment()'):
        m = _find_base_href(comment.text)
        if m:
            return m.group(1)


def _get_base_href(tree):
    """ Return href value of a base tag """
    base_hrefs = tree.xpath('//base/@href')
    if base_hrefs:
        return base_hrefs[0]
