# -*- coding: utf-8 -*-
import unittest
import six
from w3lib.html import (replace_entities, replace_tags, remove_comments,
    remove_tags_with_content, replace_escape_chars, remove_tags, unquote_markup,
    get_base_url, get_meta_refresh)


class RemoveEntitiesTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return uncode
        assert isinstance(replace_entities(b'no entities'), six.text_type)
        assert isinstance(replace_entities(b'Price: &pound;100!'),  six.text_type)
        assert isinstance(replace_entities(u'no entities'), six.text_type)
        assert isinstance(replace_entities(u'Price: &pound;100!'),  six.text_type)

    def test_regular(self):
        # regular conversions
        self.assertEqual(replace_entities(u'As low as &#163;100!'),
                         u'As low as \xa3100!')
        self.assertEqual(replace_entities(b'As low as &pound;100!'),
                         u'As low as \xa3100!')
        self.assertEqual(replace_entities('redirectTo=search&searchtext=MR0221Y&aff=buyat&affsrc=d_data&cm_mmc=buyat-_-ELECTRICAL & SEASONAL-_-MR0221Y-_-9-carat gold &frac12;oz solid crucifix pendant'),
                         u'redirectTo=search&searchtext=MR0221Y&aff=buyat&affsrc=d_data&cm_mmc=buyat-_-ELECTRICAL & SEASONAL-_-MR0221Y-_-9-carat gold \xbdoz solid crucifix pendant')

    def test_keep_entities(self):
        # keep some entities
        self.assertEqual(replace_entities(b'<b>Low &lt; High &amp; Medium &pound; six</b>', keep=['lt', 'amp']),
                         u'<b>Low &lt; High &amp; Medium \xa3 six</b>')
        self.assertEqual(replace_entities(u'<b>Low &lt; High &amp; Medium &pound; six</b>', keep=[u'lt', u'amp']),
                         u'<b>Low &lt; High &amp; Medium \xa3 six</b>')

    def test_illegal_entities(self):
        self.assertEqual(replace_entities('a &lt; b &illegal; c &#12345678; six', remove_illegal=False),
                         u'a < b &illegal; c &#12345678; six')
        self.assertEqual(replace_entities('a &lt; b &illegal; c &#12345678; six', remove_illegal=True),
                         u'a < b  c  six')
        self.assertEqual(replace_entities('x&#x2264;y'), u'x\u2264y')
        self.assertEqual(replace_entities('x&#157;y'), u'xy')
        self.assertEqual(replace_entities('x&#157;y', remove_illegal=False), u'x&#157;y')

    def test_browser_hack(self):
        # check browser hack for numeric character references in the 80-9F range
        self.assertEqual(replace_entities('x&#153;y', encoding='cp1252'), u'x\u2122y')
        self.assertEqual(replace_entities('x&#x99;y', encoding='cp1252'), u'x\u2122y')

    def test_missing_semicolon(self):
        for entity, result in (
                ('&lt&lt!', '<<!',),
                ('&LT!', '<!',),
                ('&#X41 ', 'A ',),
                ('&#x41!', 'A!',),
                ('&#x41h', 'Ah',),
                ('&#65!', 'A!',),
                ('&#65x', 'Ax',),
                ('&sup3!', u'\u00B3!',),
                ('&Aacute!', u'\u00C1!',),
                ('&#9731!', u'\u2603!',),
                ('&#153', u'\u2122',),
                ('&#x99', u'\u2122',),
                ):
            self.assertEqual(replace_entities(entity, encoding='cp1252'), result)
            self.assertEqual(replace_entities('x%sy' % entity, encoding='cp1252'), u'x%sy' % result)


    def test_encoding(self):
        self.assertEqual(replace_entities(b'x\x99&#153;&#8482;y', encoding='cp1252'), \
                         u'x\u2122\u2122\u2122y')


class ReplaceTagsTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return uncode
        assert isinstance(replace_tags(b'no entities'), six.text_type)
        assert isinstance(replace_tags('no entities'), six.text_type)

    def test_replace_tags(self):
        self.assertEqual(replace_tags(u'This text contains <a>some tag</a>'),
                         u'This text contains some tag')
        self.assertEqual(replace_tags(b'This text is very im<b>port</b>ant', ' '),
                         u'This text is very im port ant')

    def test_replace_tags_multiline(self):
        self.assertEqual(replace_tags(b'Click <a class="one"\r\n href="url">here</a>'),
                         u'Click here')


class RemoveCommentsTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_comments(b'without comments'), six.text_type)
        assert isinstance(remove_comments(b'<!-- with comments -->'), six.text_type)
        assert isinstance(remove_comments(u'without comments'), six.text_type)
        assert isinstance(remove_comments(u'<!-- with comments -->'), six.text_type)

    def test_no_comments(self):
        # text without comments
        self.assertEqual(remove_comments(u'text without comments'), u'text without comments')

    def test_remove_comments(self):
        # text with comments
        self.assertEqual(remove_comments(u'<!--text with comments-->'), u'')
        self.assertEqual(remove_comments(u'Hello<!--World-->'), u'Hello')
        self.assertEqual(remove_comments(u'Hello<!--My\nWorld-->'), u'Hello')

        self.assertEqual(remove_comments(b"test <!--textcoment--> whatever"), u'test  whatever')
        self.assertEqual(remove_comments(b"test <!--\ntextcoment\n--> whatever"), u'test  whatever')

        self.assertEqual(remove_comments(b"test <!--"), u'test ')


class RemoveTagsTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_tags(b'no tags'), six.text_type)
        assert isinstance(remove_tags(b'no tags', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags(b'<p>one tag</p>'), six.text_type)
        assert isinstance(remove_tags(b'<p>one tag</p>', which_ones=('p')), six.text_type)
        assert isinstance(remove_tags(b'<a>link</a>', which_ones=('b',)), six.text_type)
        assert isinstance(remove_tags(u'no tags'), six.text_type)
        assert isinstance(remove_tags(u'no tags', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags(u'<p>one tag</p>'), six.text_type)
        assert isinstance(remove_tags(u'<p>one tag</p>', which_ones=('p')), six.text_type)
        assert isinstance(remove_tags(u'<a>link</a>', which_ones=('b',)), six.text_type)

    def test_remove_tags_without_tags(self):
        # text without tags
        self.assertEqual(remove_tags(u'no tags'), u'no tags')
        self.assertEqual(remove_tags(u'no tags', which_ones=('p', 'b',)), u'no tags')

    def test_remove_tags(self):
        # text with tags
        self.assertEqual(remove_tags(u'<p>one p tag</p>'), u'one p tag')
        self.assertEqual(remove_tags(u'<p>one p tag</p>', which_ones=('b',)), u'<p>one p tag</p>')

        self.assertEqual(remove_tags(u'<b>not will removed</b><i>i will removed</i>', which_ones=('i',)),
                         u'<b>not will removed</b>i will removed')

    def test_remove_tags_with_attributes(self):
        # text with tags and attributes
        self.assertEqual(remove_tags(u'<p align="center" class="one">texty</p>'), u'texty')
        self.assertEqual(remove_tags(u'<p align="center" class="one">texty</p>', which_ones=('b',)),
                         u'<p align="center" class="one">texty</p>')

    def test_remove_empty_tags(self):
        # text with empty tags
        self.assertEqual(remove_tags(u'a<br />b<br/>c'), u'abc')
        self.assertEqual(remove_tags(u'a<br />b<br/>c', which_ones=('br',)), u'abc')

    def test_keep_argument(self):
        self.assertEqual(remove_tags(u'<p>a<br />b<br/>c</p>', keep=('br',)), u'a<br />b<br/>c')
        self.assertEqual(remove_tags(u'<p>a<br />b<br/>c</p>', keep=('p',)), u'<p>abc</p>')
        self.assertEqual(remove_tags(u'<p>a<br />b<br/>c</p>', keep=('p', 'br', 'div')), u'<p>a<br />b<br/>c</p>')

    def test_uppercase_tags(self):
        self.assertEqual(remove_tags(u'<foo></foo><bar></bar><baz/>', which_ones=('Foo', 'BAR', 'baZ')), u'')
        self.assertEqual(remove_tags(u'<FOO></foO><BaR></bAr><BAZ/>', which_ones=('foo', 'bar', 'baz')), u'')


class RemoveTagsWithContentTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_tags_with_content(b'no tags'), six.text_type)
        assert isinstance(remove_tags_with_content(b'no tags', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags_with_content(b'<p>one tag</p>', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags_with_content(b'<a>link</a>', which_ones=('b',)), six.text_type)
        assert isinstance(remove_tags_with_content(u'no tags'), six.text_type)
        assert isinstance(remove_tags_with_content(u'no tags', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags_with_content(u'<p>one tag</p>', which_ones=('p',)), six.text_type)
        assert isinstance(remove_tags_with_content(u'<a>link</a>', which_ones=('b',)), six.text_type)

    def test_without_tags(self):
        # text without tags
        self.assertEqual(remove_tags_with_content(u'no tags'), u'no tags')
        self.assertEqual(remove_tags_with_content(u'no tags', which_ones=('p', 'b',)), u'no tags')

    def test_with_tags(self):
        # text with tags
        self.assertEqual(remove_tags_with_content(u'<p>one p tag</p>'), u'<p>one p tag</p>')
        self.assertEqual(remove_tags_with_content(u'<p>one p tag</p>', which_ones=('p',)), u'')

        self.assertEqual(remove_tags_with_content(u'<b>not will removed</b><i>i will removed</i>', which_ones=('i',)),
                         u'<b>not will removed</b>')

    def test_empty_tags(self):
        # text with empty tags
        self.assertEqual(remove_tags_with_content(u'<br/>a<br />', which_ones=('br',)), u'a')

    def test_tags_with_shared_prefix(self):
        # https://github.com/scrapy/w3lib/issues/114
        self.assertEqual(remove_tags_with_content(u'<span></span><s></s>', which_ones=('s',)), u'<span></span>')


class ReplaceEscapeCharsTest(unittest.TestCase):
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(replace_escape_chars(b'no ec'), six.text_type)
        assert isinstance(replace_escape_chars(b'no ec', replace_by='str'), six.text_type)
        assert isinstance(replace_escape_chars(b'no ec', replace_by=u'str'), six.text_type)
        assert isinstance(replace_escape_chars(b'no ec', which_ones=('\n', '\t',)), six.text_type)
        assert isinstance(replace_escape_chars(u'no ec'), six.text_type)
        assert isinstance(replace_escape_chars(u'no ec', replace_by=u'str'), six.text_type)
        assert isinstance(replace_escape_chars(u'no ec', which_ones=('\n', '\t',)), six.text_type)

    def test_without_escape_chars(self):
        # text without escape chars
        self.assertEqual(replace_escape_chars(u'no ec'), u'no ec')
        self.assertEqual(replace_escape_chars(u'no ec', which_ones=('\n',)), u'no ec')

    def test_with_escape_chars(self):
        # text with escape chars
        self.assertEqual(replace_escape_chars(u'escape\n\n'), u'escape')
        self.assertEqual(replace_escape_chars(u'escape\n', which_ones=('\t',)), u'escape\n')
        self.assertEqual(replace_escape_chars(u'escape\tchars\n', which_ones=('\t',)), 'escapechars\n')
        self.assertEqual(replace_escape_chars(u'escape\tchars\n', replace_by=' '), 'escape chars ')
        self.assertEqual(replace_escape_chars(u'escape\tchars\n', replace_by=u'\xa3'), u'escape\xa3chars\xa3')
        self.assertEqual(replace_escape_chars(u'escape\tchars\n', replace_by=b'\xc2\xa3'), u'escape\xa3chars\xa3')


class UnquoteMarkupTest(unittest.TestCase):

    sample_txt1 = u"""<node1>hi, this is sample text with entities: &amp; &copy;
<![CDATA[although this is inside a cdata! &amp; &quot;]]></node1>"""
    sample_txt2 = u'<node2>blah&amp;blah<![CDATA[blahblahblah!&pound;]]>moreblah&lt;&gt;</node2>'
    sample_txt3 = u'something&pound;&amp;more<node3><![CDATA[things, stuff, and such]]>what&quot;ever</node3><node4'

    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(unquote_markup(self.sample_txt1.encode('latin-1')), six.text_type)
        assert isinstance(unquote_markup(self.sample_txt2), six.text_type)

    def test_unquote_markup(self):
        self.assertEqual(unquote_markup(self.sample_txt1), u"""<node1>hi, this is sample text with entities: & \xa9
although this is inside a cdata! &amp; &quot;</node1>""")

        self.assertEqual(unquote_markup(self.sample_txt2), u'<node2>blah&blahblahblahblah!&pound;moreblah<></node2>')

        self.assertEqual(unquote_markup(self.sample_txt1 + self.sample_txt2), u"""<node1>hi, this is sample text with entities: & \xa9
although this is inside a cdata! &amp; &quot;</node1><node2>blah&blahblahblahblah!&pound;moreblah<></node2>""")

        self.assertEqual(unquote_markup(self.sample_txt3), u'something\xa3&more<node3>things, stuff, and suchwhat"ever</node3><node4')


class GetBaseUrlTest(unittest.TestCase):

    def test_get_base_url(self):
        baseurl = u'https://example.org'

        text = u"""\
            <html>\
            <head><title>Dummy</title><base href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        self.assertEqual(get_base_url(text, baseurl), 'http://example.org/something')
        self.assertEqual(get_base_url(text, baseurl.encode('ascii')), 'http://example.org/something')


    def test_relative_url_with_absolute_path(self):
        baseurl = 'https://example.org'
        text = u"""\
            <html>\
            <head><title>Dummy</title><base href='/absolutepath' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        self.assertEqual(get_base_url(text, baseurl), 'https://example.org/absolutepath')

    def test_no_scheme_url(self):
        baseurl = 'https://example.org'
        text = b"""\
            <html>\
            <head><title>Dummy</title><base href='//noscheme.com/path' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        self.assertEqual(get_base_url(text, baseurl), 'https://noscheme.com/path')

    def test_attributes_before_href(self):
        baseurl = u'https://example.org'

        text = u"""\
            <html>\
            <head><title>Dummy</title><base id='my_base_tag' href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        self.assertEqual(get_base_url(text, baseurl), 'http://example.org/something')

    def test_tag_name(self):
        baseurl = u'https://example.org'

        text = u"""\
            <html>\
            <head><title>Dummy</title><basefoo href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        self.assertEqual(get_base_url(text, baseurl), 'https://example.org')

    def test_get_base_url_utf8(self):
        baseurl = u'https://example.org'

        text = u"""
            <html>
            <head><title>Dummy</title><base href='http://example.org/snowman\u2368' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        self.assertEqual(get_base_url(text, baseurl),
                         'http://example.org/snowman%E2%8D%A8')

    def test_get_base_url_latin1(self):
        # page encoding does not affect URL path encoding before percent-escaping
        # we should still use UTF-8 by default
        baseurl = u'https://example.org'

        text = u"""
            <html>
            <head><title>Dummy</title><base href='http://example.org/sterling\u00a3' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        self.assertEqual(get_base_url(text, baseurl, encoding='latin-1'),
                         'http://example.org/sterling%C2%A3')

    def test_get_base_url_latin1_percent(self):
        # non-UTF-8 percent-encoded characters sequence are left untouched
        baseurl = u'https://example.org'

        text = u"""
            <html>
            <head><title>Dummy</title><base href='http://example.org/sterling%a3' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        self.assertEqual(get_base_url(text, baseurl),
                         'http://example.org/sterling%a3')


class GetMetaRefreshTest(unittest.TestCase):
    def test_get_meta_refresh(self):
        baseurl = 'http://example.org'
        body = """
            <html>
            <head><title>Dummy</title><meta http-equiv="refresh" content="5;url=http://example.org/newpage" /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (5, 'http://example.org/newpage'))

    def test_without_url(self):
        # refresh without url should return (None, None)
        baseurl = 'http://example.org'
        body = """<meta http-equiv="refresh" content="5" />"""
        self.assertEqual(get_meta_refresh(body, baseurl), (None, None))

        body = """<meta http-equiv="refresh" content="5;
            url=http://example.org/newpage" /></head>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (5, 'http://example.org/newpage'))

    def test_multiline(self):
        # meta refresh in multiple lines
        baseurl = 'http://example.org'
        body = """<html><head>
               <META
               HTTP-EQUIV="Refresh"
               CONTENT="1; URL=http://example.org/newpage">"""
        self.assertEqual(get_meta_refresh(body, baseurl), (1, 'http://example.org/newpage'))

    def test_entities_in_redirect_url(self):
        # entities in the redirect url
        baseurl = 'http://example.org'
        body = """<meta http-equiv="refresh" content="3; url=&#39;http://www.example.com/other&#39;">"""
        self.assertEqual(get_meta_refresh(body, baseurl), (3, 'http://www.example.com/other'))

    def test_relative_redirects(self):
        # relative redirects
        baseurl = 'http://example.com/page/this.html'
        body = """<meta http-equiv="refresh" content="3; url=other.html">"""
        self.assertEqual(get_meta_refresh(body, baseurl), (3, 'http://example.com/page/other.html'))

    def test_nonascii_url_utf8(self):
        # non-ascii chars in the url (utf8 - default)
        baseurl = 'http://example.com'
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xc2\xa3">"""
        self.assertEqual(get_meta_refresh(body, baseurl), (3, 'http://example.com/to%C2%A3'))

    def test_nonascii_url_latin1(self):
        # non-ascii chars in the url path (latin1)
        # should end up UTF-8 encoded anyway
        baseurl = 'http://example.com'
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xa3">"""
        self.assertEqual(get_meta_refresh(body, baseurl, 'latin1'), (3, 'http://example.com/to%C2%A3'))

    def test_nonascii_url_latin1_query(self):
        # non-ascii chars in the url path and query (latin1)
        # only query part should be kept latin1 encoded before percent escaping
        baseurl = 'http://example.com'
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xa3?unit=\xb5">"""
        self.assertEqual(get_meta_refresh(body, baseurl, 'latin1'), (3, 'http://example.com/to%C2%A3?unit=%B5'))

    def test_commented_meta_refresh(self):
        # html commented meta refresh header must not directed
        baseurl = 'http://example.com'
        body = """<!--<meta http-equiv="refresh" content="3; url=http://example.com/">-->"""
        self.assertEqual(get_meta_refresh(body, baseurl), (None, None))

    def test_html_comments_with_uncommented_meta_refresh(self):
        # html comments must not interfere with uncommented meta refresh header
        baseurl = 'http://example.com'
        body = """<!-- commented --><meta http-equiv="refresh" content="3; url=http://example.com/">-->"""
        self.assertEqual(get_meta_refresh(body, baseurl), (3, 'http://example.com/'))

    def test_float_refresh_intervals(self):
        # float refresh intervals
        baseurl = 'http://example.com'
        body = """<meta http-equiv="refresh" content=".1;URL=index.html" />"""
        self.assertEqual(get_meta_refresh(body, baseurl), (0.1, 'http://example.com/index.html'))

        body = """<meta http-equiv="refresh" content="3.1;URL=index.html" />"""
        self.assertEqual(get_meta_refresh(body, baseurl), (3.1, 'http://example.com/index.html'))

    def test_tag_name(self):
        baseurl = 'http://example.org'
        body = """
            <html>
            <head><title>Dummy</title><metafoo http-equiv="refresh" content="5;url=http://example.org/newpage" /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (None, None))

    def test_leading_newline_in_url(self):
        baseurl = 'http://example.org'
        body = """
        <html>
        <head><title>Dummy</title><meta http-equiv="refresh" content="0; URL=
http://www.example.org/index.php" />
        </head>
        </html>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (0.0, 'http://www.example.org/index.php'))

    def test_inside_noscript(self):
        baseurl = 'http://example.org'
        body = """
            <html>
            <head><noscript><meta http-equiv="refresh" content="0;url=http://example.org/javascript_required" /></noscript></head>
            </html>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (None, None))
        self.assertEqual(get_meta_refresh(body, baseurl, ignore_tags=()), (0.0, "http://example.org/javascript_required"))

    def test_inside_script(self):
        baseurl = 'http://example.org'
        body = """
            <html>
            <head><script>if(!foobar()){ $('<meta http-equiv="refresh" content="0;url=http://example.org/foobar_required" />').appendTo('body'); }</script></head>
            </html>"""
        self.assertEqual(get_meta_refresh(body, baseurl), (None, None))
        self.assertEqual(get_meta_refresh(body, baseurl, ignore_tags=()), (0.0, "http://example.org/foobar_required"))
