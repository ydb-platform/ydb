# -*- coding: utf-8 -*-

# Copyright (c) 2007-2012 Christoph Haas <email@christoph-haas.de>
# See the file LICENSE for copying permission.

""""Test paginate module."""
import pytest
import paginate

def test_wrong_collection():
    """Test whether an empty list is handled correctly."""
    with pytest.raises(TypeError):
        page = paginate.Page({}, page=0)


def test_empty_list():
    """Test whether an empty list is handled correctly."""
    items = []
    page = paginate.Page(items, page=0)
    assert page.page == 1
    assert page.first_item is None
    assert page.last_item is None
    assert page.first_page is None
    assert page.last_page is None
    assert page.previous_page is None
    assert page.next_page is None
    assert page.items_per_page == 20
    assert page.item_count == 0
    assert page.page_count == 0
    assert page.pager(url="http://example.org/page=$page") == ''
    assert page.pager(url="http://example.org/page=$page",
                      show_if_single_page=True) == ''


def test_one_page():
    """Test that fits 10 items on a single 10-item page."""
    items = range(10)
    page = paginate.Page(items, page=0, items_per_page=10)
    url = "http://example.org/foo/page=$page"
    assert page.page == 1
    assert page.first_item == 1
    assert page.last_item == 10
    assert page.first_page == 1
    assert page.last_page == 1
    assert page.previous_page is None
    assert page.next_page is None
    assert page.items_per_page == 10
    assert page.item_count == 10
    assert page.page_count == 1
    assert page.pager(url=url) == ''
    assert page.pager(url=url, show_if_single_page=True) == '1'


def test_many_pages():
    """Test that fits 100 items on seven pages consisting of 15 items."""
    items = range(100)
    page = paginate.Page(items, page=0, items_per_page=15)
    url = "http://example.org/foo/page=$page"
    assert hasattr(page.collection_type, '__iter__') is True
    assert page.page == 1
    assert page.first_item == 1
    assert page.last_item == 15
    assert page.first_page == 1
    assert page.last_page == 7
    assert page.previous_page is None
    assert page.next_page == 2
    assert page.items_per_page == 15
    assert page.item_count == 100
    assert page.page_count == 7
    assert page.pager(
        url=url) == '1 <a href="http://example.org/foo/page=2">2</a> <a href="http://example.org/foo/page=3">3</a> .. <a href="http://example.org/foo/page=7">7</a>'
    assert page.pager(url=url,
                      separator='_') == '1_<a href="http://example.org/foo/page=2">2</a>_<a href="http://example.org/foo/page=3">3</a>_.._<a href="http://example.org/foo/page=7">7</a>'
    assert page.pager(url=url, link_attr={'style': 'linkstyle'},
                      curpage_attr={'style': 'curpagestyle'}, dotdot_attr={
            'style': 'dotdotstyle'}) == '<span style="curpagestyle">1</span> <a href="http://example.org/foo/page=2" style="linkstyle">2</a> <a href="http://example.org/foo/page=3" style="linkstyle">3</a> <span style="dotdotstyle">..</span> <a href="http://example.org/foo/page=7" style="linkstyle">7</a>'


def test_slice_page_0():
    items = list(range(1, 1000))
    page = paginate.Page(items, page=0, items_per_page=10)
    assert page.page == 1
    assert page.first_item == 1
    assert page.last_item == 10
    assert page.first_page == 1
    assert page.last_page == 100
    assert page.previous_page is None
    assert page.next_page == 2
    assert page.items_per_page == 10
    assert page.item_count == 999
    assert page.page_count == 100
    assert page.items == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_slice_page_5():
    items = list(range(1, 1000))
    page = paginate.Page(items, page=5, items_per_page=10)
    assert page.page == 5
    assert page.first_item == 41
    assert page.last_item == 50
    assert page.first_page == 1
    assert page.last_page == 100
    assert page.previous_page is 4
    assert page.next_page == 6
    assert page.items_per_page == 10
    assert page.item_count == 999
    assert page.page_count == 100
    assert page.items == [41, 42, 43, 44, 45, 46, 47, 48, 49, 50]


def test_link_map():
    """Test that fits 10 items on a single 10-item page."""
    items = range(109)
    page = paginate.Page(items, page=0, items_per_page=15)
    url = "http://example.org/foo/page=$page"
    format = '$link_first $link_previous ~4~ $link_next $link_last (Page $page our of $page_count - total items $item_count)'
    result = page.link_map(format, url=url)
    fpage_result = {'current_page': {'attrs': {},
                                     'type': 'current_page',
                                     'href': 'http://example.org/foo/page=1',
                                     'value': 1},
                    'first_page': {'attrs': {},
                                   'href': 'http://example.org/foo/page=1',
                                   'number': 1,
                                   'type': 'first_page',
                                   'value': '&lt;&lt;'},
                    'last_page': {'attrs': {},
                                  'href': 'http://example.org/foo/page=8',
                                  'number': 8,
                                  'type': 'last_page',
                                  'value': '&gt;&gt;'},
                    'next_page': {'attrs': {},
                                  'href': 'http://example.org/foo/page=2',
                                  'number': 2,
                                  'type': 'next_page',
                                  'value': '&gt;'},
                    'previous_page': {'attrs': {},
                                      'href': 'http://example.org/foo/page=1',
                                      'number': 1,
                                      'type': 'previous_page',
                                      'value': '&lt;'},
                    'range_pages': [{'attrs': {},
                                     'href': 'http://example.org/foo/page=1',
                                     'number': 1,
                                     'type': 'current_page',
                                     'value': '1'},
                                    {'attrs': {},
                                     'href': 'http://example.org/foo/page=2',
                                     'number': 2,
                                     'type': 'page',
                                     'value': '2'},
                                    {'attrs': {},
                                     'href': 'http://example.org/foo/page=3',
                                     'number': 3,
                                     'type': 'page',
                                     'value': '3'},
                                    {'attrs': {},
                                     'href': 'http://example.org/foo/page=4',
                                     'number': 4,
                                     'type': 'page',
                                     'value': '4'},
                                    {'attrs': {},
                                     'href': 'http://example.org/foo/page=5',
                                     'number': 5,
                                     'type': 'page',
                                     'value': '5'},
                                    {'attrs': {},
                                     'href': '',
                                     'number': None,
                                     'type': 'span',
                                     'value': '..'}],
                    'radius': 4}
    assert result == fpage_result
    page = paginate.Page(items, page=100, items_per_page=15)
    result = page.link_map(format, url=url)
    l_page_result = {'current_page': {'attrs': {},
                                      'type': 'current_page',
                                      'href': 'http://example.org/foo/page=8',
                                      'value': 8},
                     'first_page': {'attrs': {},
                                    'href': 'http://example.org/foo/page=1',
                                    'number': 1,
                                    'type': 'first_page',
                                    'value': '&lt;&lt;'},
                     'last_page': {'attrs': {},
                                   'href': 'http://example.org/foo/page=8',
                                   'number': 8,
                                   'type': 'last_page',
                                   'value': '&gt;&gt;'},
                     'next_page': {'attrs': {},
                                   'href': 'http://example.org/foo/page=8',
                                   'number': 8,
                                   'type': 'next_page',
                                   'value': '&gt;'},
                     'previous_page': {'attrs': {},
                                       'href': 'http://example.org/foo/page=7',
                                       'number': 7,
                                       'type': 'previous_page',
                                       'value': '&lt;'},
                     'range_pages': [{'attrs': {},
                                      'href': '',
                                      'number': None,
                                      'type': 'span',
                                      'value': '..'},
                                     {'attrs': {},
                                      'href': 'http://example.org/foo/page=4',
                                      'number': 4,
                                      'type': 'page',
                                      'value': '4'},
                                     {'attrs': {},
                                      'href': 'http://example.org/foo/page=5',
                                      'number': 5,
                                      'type': 'page',
                                      'value': '5'},
                                     {'attrs': {},
                                      'href': 'http://example.org/foo/page=6',
                                      'number': 6,
                                      'type': 'page',
                                      'value': '6'},
                                     {'attrs': {},
                                      'href': 'http://example.org/foo/page=7',
                                      'number': 7,
                                      'type': 'page',
                                      'value': '7'},
                                     {'attrs': {},
                                      'href': 'http://example.org/foo/page=8',
                                      'number': 8,
                                      'type': 'current_page',
                                      'value': '8'}],
                     'radius': 4}
    assert result == l_page_result

    page = paginate.Page(items, page=100, items_per_page=15)
    result = page.link_map(format, url=url, symbol_next=u'nëxt',
                           symbol_previous=u'prëvious')
    next_page = {'attrs': {},
                 'href': u'http://example.org/foo/page=8',
                 'number': 8,
                 'type': 'next_page',
                 'value': u'nëxt'}
    assert next_page == result['next_page']


def test_empty_link_map():
    """Test that fits 10 items on a single 10-item page."""
    items = []
    page = paginate.Page(items, page=0, items_per_page=15)
    url = "http://example.org/foo/page=$page"
    format = '$link_first $link_previous ~4~ $link_next $link_last (Page $page our of $page_count - total items $item_count)'
    result = page.link_map(format, url=url)
    assert result == {'current_page': None,
                      'first_page': None,
                      'last_page': None,
                      'next_page': None,
                      'previous_page': None,
                      'radius': 4,
                      'range_pages': []}


def test_make_html_tag():
    """Test the make_html_tag() function"""
    assert paginate.make_html_tag('div') == '<div>'
    assert paginate.make_html_tag('a',
                                  href="/another/page") == '<a href="/another/page">'
    assert paginate.make_html_tag('a', href="/another/page",
                                  text="foo") == '<a href="/another/page">foo</a>'
    assert paginate.make_html_tag('a', href=u"/другой/страница",
                                  text="foo") == u'<a href="/другой/страница">foo</a>'
    assert paginate.make_html_tag('a', href="/another/page", text="foo",
                                  onclick="$('#mydiv').focus();") == """<a href="/another/page" onclick="$('#mydiv').focus();">foo</a>"""
    assert paginate.make_html_tag('span',
                                  style='green') == '<span style="green">'
    assert paginate.make_html_tag('div', _class='red',
                                  id='maindiv') == '<div class="red" id="maindiv">'


def test_url_assertion():
    page = paginate.Page(range(100), page=0, items_per_page=10)
    url = "http://example.org/"
    with pytest.raises(Exception):
        page.pager(url=url)


def test_url_generation():
    def url_maker(page_number):
        return str('x%s' % page_number)

    page = paginate.Page(range(100), page=1, url_maker=url_maker)
    assert page.pager() == '1 <a href="x2">2</a> <a href="x3">3</a> .. <a href="x5">5</a>'


def test_pager_without_any_pattern():
    def url_maker(page_number):
        return str('x%s' % page_number)

    page = paginate.Page(range(100), page=1, url_maker=url_maker)
    assert page.pager('') == ''


def test_pager_without_radius_pattern():
    def url_maker(page_number):
        return str('x%s' % page_number)

    page = paginate.Page(range(100), page=2, url_maker=url_maker)
    assert page.pager(
        '$link_first FOO $link_last') == '<a href="x1">&lt;&lt;</a> FOO <a href="x5">&gt;&gt;</a>'


class UnsliceableSequence(object):
    def __init__(self, seq):
        self.l = seq

    def __iter__(self):
        for i in self.l:
            yield i

    def __len__(self):
        return len(self.l)


class UnsliceableSequence2(UnsliceableSequence):
    def __getitem__(self, key):
        raise TypeError("unhashable type")


class TestCollectionTypes(object):
    rng = list(range(10))  # A list in both Python 2 and 3.

    def test_list(self):
        paginate.Page(self.rng)

    def test_list_of_dicts(self):
        rng = [{'a':x} for x in range(200)]
        page = paginate.Page(rng)
        assert page.item_count == 200

    def test_tuple(self):
        paginate.Page(tuple(self.rng))

    def test_unsliceable_sequence(self):
        with pytest.raises(TypeError):
            paginate.Page(UnsliceableSequence(self.rng))

    def test_unsliceable_sequence2(self):
        with pytest.raises(TypeError):
            paginate.Page(UnsliceableSequence2(self.rng))

    def test_unsliceable_sequence3(self):
        with pytest.raises(TypeError):
            paginate.Page(dict(one=1))
