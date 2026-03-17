from w3lib.html import (
    get_base_url,
    get_meta_refresh,
    remove_comments,
    remove_tags,
    remove_tags_with_content,
    replace_entities,
    replace_escape_chars,
    replace_tags,
    unquote_markup,
)


class TestRemoveEntities:
    def test_returns_unicode(self):
        # make sure it always return uncode
        assert isinstance(replace_entities(b"no entities"), str)
        assert isinstance(replace_entities(b"Price: &pound;100!"), str)
        assert isinstance(replace_entities("no entities"), str)
        assert isinstance(replace_entities("Price: &pound;100!"), str)

    def test_regular(self):
        # regular conversions
        assert replace_entities("As low as &#163;100!") == "As low as \xa3100!"
        assert replace_entities(b"As low as &pound;100!") == "As low as \xa3100!"
        assert (
            replace_entities(
                "redirectTo=search&searchtext=MR0221Y&aff=buyat&affsrc=d_data&cm_mmc=buyat-_-ELECTRICAL & SEASONAL-_-MR0221Y-_-9-carat gold &frac12;oz solid crucifix pendant"
            )
            == "redirectTo=search&searchtext=MR0221Y&aff=buyat&affsrc=d_data&cm_mmc=buyat-_-ELECTRICAL & SEASONAL-_-MR0221Y-_-9-carat gold \xbdoz solid crucifix pendant"
        )

    def test_keep_entities(self):
        # keep some entities
        assert (
            replace_entities(
                b"<b>Low &lt; High &amp; Medium &pound; six</b>", keep=["lt", "amp"]
            )
            == "<b>Low &lt; High &amp; Medium \xa3 six</b>"
        )
        assert (
            replace_entities(
                "<b>Low &lt; High &amp; Medium &pound; six</b>", keep=["lt", "amp"]
            )
            == "<b>Low &lt; High &amp; Medium \xa3 six</b>"
        )

    def test_illegal_entities(self):
        assert (
            replace_entities(
                "a &lt; b &illegal; c &#12345678; six", remove_illegal=False
            )
            == "a < b &illegal; c &#12345678; six"
        )
        assert (
            replace_entities(
                "a &lt; b &illegal; c &#12345678; six", remove_illegal=True
            )
            == "a < b  c  six"
        )
        assert replace_entities("x&#x2264;y") == "x\u2264y"
        assert replace_entities("x&#157;y") == "xy"
        assert replace_entities("x&#157;y", remove_illegal=False) == "x&#157;y"
        assert replace_entities("&#82179209091;") == ""
        assert (
            replace_entities("&#82179209091;", remove_illegal=False) == "&#82179209091;"
        )

    def test_browser_hack(self):
        # check browser hack for numeric character references in the 80-9F range
        assert replace_entities("x&#153;y", encoding="cp1252") == "x\u2122y"
        assert replace_entities("x&#x99;y", encoding="cp1252") == "x\u2122y"

    def test_missing_semicolon(self):
        for entity, result in (
            ("&lt&lt!", "<<!"),
            ("&LT!", "<!"),
            ("&#X41 ", "A "),
            ("&#x41!", "A!"),
            ("&#x41h", "Ah"),
            ("&#65!", "A!"),
            ("&#65x", "Ax"),
            ("&sup3!", "\u00b3!"),
            ("&Aacute!", "\u00c1!"),
            ("&#9731!", "\u2603!"),
            ("&#153", "\u2122"),
            ("&#x99", "\u2122"),
        ):
            assert replace_entities(entity, encoding="cp1252") == result
            assert replace_entities(f"x{entity}y", encoding="cp1252") == f"x{result}y"

    def test_encoding(self):
        assert (
            replace_entities(b"x\x99&#153;&#8482;y", encoding="cp1252")
            == "x\u2122\u2122\u2122y"
        )


class TestReplaceTags:
    def test_returns_unicode(self):
        # make sure it always return uncode
        assert isinstance(replace_tags(b"no entities"), str)
        assert isinstance(replace_tags("no entities"), str)

    def test_replace_tags(self):
        assert (
            replace_tags("This text contains <a>some tag</a>")
            == "This text contains some tag"
        )
        assert (
            replace_tags(b"This text is very im<b>port</b>ant", " ")
            == "This text is very im port ant"
        )

    def test_replace_tags_multiline(self):
        assert (
            replace_tags(b'Click <a class="one"\r\n href="url">here</a>')
            == "Click here"
        )


class TestRemoveComments:
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_comments(b"without comments"), str)
        assert isinstance(remove_comments(b"<!-- with comments -->"), str)
        assert isinstance(remove_comments("without comments"), str)
        assert isinstance(remove_comments("<!-- with comments -->"), str)

    def test_no_comments(self):
        # text without comments
        assert remove_comments("text without comments") == "text without comments"

    def test_remove_comments(self):
        # text with comments
        assert remove_comments("<!--text with comments-->") == ""
        assert remove_comments("Hello<!--World-->") == "Hello"
        assert remove_comments("Hello<!--My\nWorld-->") == "Hello"

        assert remove_comments(b"test <!--textcoment--> whatever") == "test  whatever"
        assert (
            remove_comments(b"test <!--\ntextcoment\n--> whatever") == "test  whatever"
        )

        assert remove_comments(b"test <!--") == "test "


class TestRemoveTags:
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_tags(b"no tags"), str)
        assert isinstance(remove_tags(b"no tags", which_ones=("p",)), str)
        assert isinstance(remove_tags(b"<p>one tag</p>"), str)
        assert isinstance(remove_tags(b"<p>one tag</p>", which_ones=("p",)), str)
        assert isinstance(remove_tags(b"<a>link</a>", which_ones=("b",)), str)
        assert isinstance(remove_tags("no tags"), str)
        assert isinstance(remove_tags("no tags", which_ones=("p",)), str)
        assert isinstance(remove_tags("<p>one tag</p>"), str)
        assert isinstance(remove_tags("<p>one tag</p>", which_ones=("p",)), str)
        assert isinstance(remove_tags("<a>link</a>", which_ones=("b",)), str)

    def test_remove_tags_without_tags(self):
        # text without tags
        assert remove_tags("no tags") == "no tags"
        assert remove_tags("no tags", which_ones=("p", "b")) == "no tags"

    def test_remove_tags(self):
        # text with tags
        assert remove_tags("<p>one p tag</p>") == "one p tag"
        assert remove_tags("<p>one p tag</p>", which_ones=("b",)) == "<p>one p tag</p>"

        assert (
            remove_tags(
                "<b>not will removed</b><i>i will removed</i>", which_ones=("i",)
            )
            == "<b>not will removed</b>i will removed"
        )

    def test_remove_tags_with_attributes(self):
        # text with tags and attributes
        assert remove_tags('<p align="center" class="one">texty</p>') == "texty"
        assert (
            remove_tags('<p align="center" class="one">texty</p>', which_ones=("b",))
            == '<p align="center" class="one">texty</p>'
        )

    def test_remove_empty_tags(self):
        # text with empty tags
        assert remove_tags("a<br />b<br/>c") == "abc"
        assert remove_tags("a<br />b<br/>c", which_ones=("br",)) == "abc"

    def test_keep_argument(self):
        assert remove_tags("<p>a<br />b<br/>c</p>", keep=("br",)) == "a<br />b<br/>c"
        assert remove_tags("<p>a<br />b<br/>c</p>", keep=("p",)) == "<p>abc</p>"
        assert (
            remove_tags("<p>a<br />b<br/>c</p>", keep=("p", "br", "div"))
            == "<p>a<br />b<br/>c</p>"
        )

    def test_uppercase_tags(self):
        assert (
            remove_tags(
                "<foo></foo><bar></bar><baz/>", which_ones=("Foo", "BAR", "baZ")
            )
            == ""
        )
        assert (
            remove_tags(
                "<FOO></foO><BaR></bAr><BAZ/>", which_ones=("foo", "bar", "baz")
            )
            == ""
        )


class TestRemoveTagsWithContent:
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(remove_tags_with_content(b"no tags"), str)
        assert isinstance(remove_tags_with_content(b"no tags", which_ones=("p",)), str)
        assert isinstance(
            remove_tags_with_content(b"<p>one tag</p>", which_ones=("p",)), str
        )
        assert isinstance(
            remove_tags_with_content(b"<a>link</a>", which_ones=("b",)), str
        )
        assert isinstance(remove_tags_with_content("no tags"), str)
        assert isinstance(remove_tags_with_content("no tags", which_ones=("p",)), str)
        assert isinstance(
            remove_tags_with_content("<p>one tag</p>", which_ones=("p",)), str
        )
        assert isinstance(
            remove_tags_with_content("<a>link</a>", which_ones=("b",)), str
        )

    def test_without_tags(self):
        # text without tags
        assert remove_tags_with_content("no tags") == "no tags"
        assert remove_tags_with_content("no tags", which_ones=("p", "b")) == "no tags"

    def test_with_tags(self):
        # text with tags
        assert remove_tags_with_content("<p>one p tag</p>") == "<p>one p tag</p>"
        assert remove_tags_with_content("<p>one p tag</p>", which_ones=("p",)) == ""

        assert (
            remove_tags_with_content(
                "<b>not will removed</b><i>i will removed</i>", which_ones=("i",)
            )
            == "<b>not will removed</b>"
        )

    def test_empty_tags(self):
        # text with empty tags
        assert remove_tags_with_content("<br/>a<br />", which_ones=("br",)) == "a"

    def test_tags_with_shared_prefix(self):
        # https://github.com/scrapy/w3lib/issues/114
        assert (
            remove_tags_with_content("<span></span><s></s>", which_ones=("s",))
            == "<span></span>"
        )


class TestReplaceEscapeChars:
    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(replace_escape_chars(b"no ec"), str)
        assert isinstance(replace_escape_chars(b"no ec", replace_by="str"), str)
        assert isinstance(replace_escape_chars(b"no ec", replace_by="str"), str)
        assert isinstance(replace_escape_chars(b"no ec", which_ones=("\n", "\t")), str)
        assert isinstance(replace_escape_chars("no ec"), str)
        assert isinstance(replace_escape_chars("no ec", replace_by="str"), str)
        assert isinstance(replace_escape_chars("no ec", which_ones=("\n", "\t")), str)

    def test_without_escape_chars(self):
        # text without escape chars
        assert replace_escape_chars("no ec") == "no ec"
        assert replace_escape_chars("no ec", which_ones=("\n",)) == "no ec"

    def test_with_escape_chars(self):
        # text with escape chars
        assert replace_escape_chars("escape\n\n") == "escape"
        assert replace_escape_chars("escape\n", which_ones=("\t",)) == "escape\n"
        assert (
            replace_escape_chars("escape\tchars\n", which_ones=("\t",))
            == "escapechars\n"
        )
        assert (
            replace_escape_chars("escape\tchars\n", replace_by=" ") == "escape chars "
        )
        assert (
            replace_escape_chars("escape\tchars\n", replace_by="\xa3")
            == "escape\xa3chars\xa3"
        )
        assert (
            replace_escape_chars("escape\tchars\n", replace_by=b"\xc2\xa3")
            == "escape\xa3chars\xa3"
        )


class TestUnquoteMarkup:
    sample_txt1 = """<node1>hi, this is sample text with entities: &amp; &copy;
<![CDATA[although this is inside a cdata! &amp; &quot;]]></node1>"""
    sample_txt2 = (
        "<node2>blah&amp;blah<![CDATA[blahblahblah!&pound;]]>moreblah&lt;&gt;</node2>"
    )
    sample_txt3 = "something&pound;&amp;more<node3><![CDATA[things, stuff, and such]]>what&quot;ever</node3><node4"

    def test_returns_unicode(self):
        # make sure it always return unicode
        assert isinstance(unquote_markup(self.sample_txt1.encode("latin-1")), str)
        assert isinstance(unquote_markup(self.sample_txt2), str)

    def test_unquote_markup(self):
        assert (
            unquote_markup(self.sample_txt1)
            == """<node1>hi, this is sample text with entities: & \xa9
although this is inside a cdata! &amp; &quot;</node1>"""
        )

        assert (
            unquote_markup(self.sample_txt2)
            == "<node2>blah&blahblahblahblah!&pound;moreblah<></node2>"
        )

        assert (
            unquote_markup(self.sample_txt1 + self.sample_txt2)
            == """<node1>hi, this is sample text with entities: & \xa9
although this is inside a cdata! &amp; &quot;</node1><node2>blah&blahblahblahblah!&pound;moreblah<></node2>"""
        )

        assert (
            unquote_markup(self.sample_txt3)
            == 'something\xa3&more<node3>things, stuff, and suchwhat"ever</node3><node4'
        )


class TestGetBaseUrl:
    def test_get_base_url(self):
        baseurl = "https://example.org"

        text = """\
            <html>\
            <head><title>Dummy</title><base href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        assert get_base_url(text, baseurl) == "http://example.org/something"
        assert (
            get_base_url(text, baseurl.encode("ascii"))
            == "http://example.org/something"
        )

    def test_base_url_in_comment(self):
        assert get_base_url("""<!-- <base href="http://example.com/"/> -->""") == ""
        assert get_base_url("""<!-- <base href="http://example.com/"/>""") == ""
        assert get_base_url("""<!-- <base href="http://example.com/"/> --""") == ""
        assert (
            get_base_url(
                """<!-- <!--  <base href="http://example.com/"/> -- -->  <base href="http://example_2.com/"/> """
            )
            == "http://example_2.com/"
        )

        assert (
            get_base_url(
                """<!-- <base href="http://example.com/"/> --> <!-- <base href="http://example_2.com/"/> --> <base href="http://example_3.com/"/>"""
            )
            == "http://example_3.com/"
        )

    def test_relative_url_with_absolute_path(self):
        baseurl = "https://example.org"
        text = """\
            <html>\
            <head><title>Dummy</title><base href='/absolutepath' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        assert get_base_url(text, baseurl) == "https://example.org/absolutepath"

    def test_no_scheme_url(self):
        baseurl = "https://example.org"
        text = b"""\
            <html>\
            <head><title>Dummy</title><base href='//noscheme.com/path' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        assert get_base_url(text, baseurl) == "https://noscheme.com/path"

    def test_attributes_before_href(self):
        baseurl = "https://example.org"

        text = """\
            <html>\
            <head><title>Dummy</title><base id='my_base_tag' href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        assert get_base_url(text, baseurl) == "http://example.org/something"

    def test_tag_name(self):
        baseurl = "https://example.org"

        text = """\
            <html>\
            <head><title>Dummy</title><basefoo href='http://example.org/something' /></head>\
            <body>blahablsdfsal&amp;</body>\
            </html>"""
        assert get_base_url(text, baseurl) == "https://example.org"

    def test_get_base_url_utf8(self):
        baseurl = "https://example.org"

        text = """
            <html>
            <head><title>Dummy</title><base href='http://example.org/snowman\u2368' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        assert get_base_url(text, baseurl) == "http://example.org/snowman%E2%8D%A8"

    def test_get_base_url_latin1(self):
        # page encoding does not affect URL path encoding before percent-escaping
        # we should still use UTF-8 by default
        baseurl = "https://example.org"

        text = """
            <html>
            <head><title>Dummy</title><base href='http://example.org/sterling\u00a3' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        assert (
            get_base_url(text, baseurl, encoding="latin-1")
            == "http://example.org/sterling%C2%A3"
        )

    def test_get_base_url_latin1_percent(self):
        # non-UTF-8 percent-encoded characters sequence are left untouched
        baseurl = "https://example.org"

        text = """
            <html>
            <head><title>Dummy</title><base href='http://example.org/sterling%a3' /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        assert get_base_url(text, baseurl) == "http://example.org/sterling%a3"


class TestGetMetaRefresh:
    def test_get_meta_refresh(self):
        baseurl = "http://example.org"
        body = """
            <html>
            <head><title>Dummy</title><meta http-equiv="refresh" content="5;url=http://example.org/newpage" /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        assert get_meta_refresh(body, baseurl) == (5, "http://example.org/newpage")

    def test_without_url(self):
        # refresh without url should return (None, None)
        baseurl = "http://example.org"
        body = """<meta http-equiv="refresh" content="5" />"""
        assert get_meta_refresh(body, baseurl) == (None, None)

        body = """<meta http-equiv="refresh" content="5;
            url=http://example.org/newpage" /></head>"""
        assert get_meta_refresh(body, baseurl) == (5, "http://example.org/newpage")

    def test_multiline(self):
        # meta refresh in multiple lines
        baseurl = "http://example.org"
        body = """<html><head>
               <META
               HTTP-EQUIV="Refresh"
               CONTENT="1; URL=http://example.org/newpage">"""
        assert get_meta_refresh(body, baseurl) == (1, "http://example.org/newpage")

    def test_entities_in_redirect_url(self):
        # entities in the redirect url
        baseurl = "http://example.org"
        body = """<meta http-equiv="refresh" content="3; url=&#39;http://www.example.com/other&#39;">"""
        assert get_meta_refresh(body, baseurl) == (3, "http://www.example.com/other")

    def test_relative_redirects(self):
        # relative redirects
        baseurl = "http://example.com/page/this.html"
        body = """<meta http-equiv="refresh" content="3; url=other.html">"""
        assert get_meta_refresh(body, baseurl) == (
            3,
            "http://example.com/page/other.html",
        )

    def test_nonascii_url_utf8(self):
        # non-ascii chars in the url (utf8 - default)
        baseurl = "http://example.com"
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xc2\xa3">"""
        assert get_meta_refresh(body, baseurl) == (3, "http://example.com/to%C2%A3")

    def test_nonascii_url_latin1(self):
        # non-ascii chars in the url path (latin1)
        # should end up UTF-8 encoded anyway
        baseurl = "http://example.com"
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xa3">"""
        assert get_meta_refresh(body, baseurl, "latin1") == (
            3,
            "http://example.com/to%C2%A3",
        )

    def test_nonascii_url_latin1_query(self):
        # non-ascii chars in the url path and query (latin1)
        # only query part should be kept latin1 encoded before percent escaping
        baseurl = "http://example.com"
        body = b"""<meta http-equiv="refresh" content="3; url=http://example.com/to\xa3?unit=\xb5">"""
        assert get_meta_refresh(body, baseurl, "latin1") == (
            3,
            "http://example.com/to%C2%A3?unit=%B5",
        )

    def test_commented_meta_refresh(self):
        # html commented meta refresh header must not directed
        baseurl = "http://example.com"
        body = """<!--<meta http-equiv="refresh" content="3; url=http://example.com/">-->"""
        assert get_meta_refresh(body, baseurl) == (None, None)

    def test_html_comments_with_uncommented_meta_refresh(self):
        # html comments must not interfere with uncommented meta refresh header
        baseurl = "http://example.com"
        body = """<!-- commented --><meta http-equiv="refresh" content="3; url=http://example.com/">-->"""
        assert get_meta_refresh(body, baseurl) == (3, "http://example.com/")

    def test_float_refresh_intervals(self):
        # float refresh intervals
        baseurl = "http://example.com"
        body = """<meta http-equiv="refresh" content=".1;URL=index.html" />"""
        assert get_meta_refresh(body, baseurl) == (0.1, "http://example.com/index.html")

        body = """<meta http-equiv="refresh" content="3.1;URL=index.html" />"""
        assert get_meta_refresh(body, baseurl) == (3.1, "http://example.com/index.html")

    def test_tag_name(self):
        baseurl = "http://example.org"
        body = """
            <html>
            <head><title>Dummy</title><metafoo http-equiv="refresh" content="5;url=http://example.org/newpage" /></head>
            <body>blahablsdfsal&amp;</body>
            </html>"""
        assert get_meta_refresh(body, baseurl) == (None, None)

    def test_leading_newline_in_url(self):
        baseurl = "http://example.org"
        body = """
        <html>
        <head><title>Dummy</title><meta http-equiv="refresh" content="0; URL=
http://www.example.org/index.php" />
        </head>
        </html>"""
        assert get_meta_refresh(body, baseurl) == (
            0.0,
            "http://www.example.org/index.php",
        )

    def test_inside_noscript(self):
        baseurl = "http://example.org"
        body = """
            <html>
            <head><noscript><meta http-equiv="refresh" content="0;url=http://example.org/javascript_required" /></noscript></head>
            </html>"""
        assert get_meta_refresh(body, baseurl) == (None, None)
        assert get_meta_refresh(body, baseurl, ignore_tags=()) == (
            0.0,
            "http://example.org/javascript_required",
        )

    def test_inside_script(self):
        baseurl = "http://example.org"
        body = """
            <html>
            <head><script>if(!foobar()){ $('<meta http-equiv="refresh" content="0;url=http://example.org/foobar_required" />').appendTo('body'); }</script></head>
            </html>"""
        assert get_meta_refresh(body, baseurl) == (None, None)
        assert get_meta_refresh(body, baseurl, ignore_tags=()) == (
            0.0,
            "http://example.org/foobar_required",
        )

    def test_redirections_in_different_ordering__in_meta_tag(self):
        baseurl = "http://localhost:8000"
        url1 = '<html><head><meta http-equiv="refresh" content="0;url=dummy.html"></head></html>'
        url2 = '<html><head><meta content="0;url=dummy.html" http-equiv="refresh"></head></html>'
        assert get_meta_refresh(url1, baseurl) == (
            0.0,
            "http://localhost:8000/dummy.html",
        )
        assert get_meta_refresh(url2, baseurl) == (
            0.0,
            "http://localhost:8000/dummy.html",
        )
