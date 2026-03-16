"""Test Soup Sieve API."""
from __future__ import unicode_literals
import soupsieve as sv
from soupsieve import util as sv_util
from . import util
import copy
import random
import pytest
import pickle
import warnings


class TestSoupSieve(util.TestCase):
    """Test Soup Sieve."""

    def test_comments(self):
        """Test comments."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        comments = [sv_util.ustr(c).strip() for c in sv.comments(soup)]
        self.assertEqual(sorted(comments), sorted(['before header', 'comment', "don't ignore"]))

    def test_icomments(self):
        """Test comments iterator."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        comments = [sv_util.ustr(c).strip() for c in sv.icomments(soup, limit=2)]
        self.assertEqual(sorted(comments), sorted(['before header', 'comment']))

    def test_compiled_comments(self):
        """Test comments from compiled pattern."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')

        # Check that comments on compiled object work just like `sv.comments`
        pattern = sv.compile('div', None, 0)
        comments = [sv_util.ustr(c).strip() for c in pattern.comments(soup)]
        self.assertEqual(sorted(comments), sorted(['before header', 'comment', "don't ignore"]))

    def test_compiled_icomments(self):
        """Test comments iterator from compiled pattern."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        pattern = sv.compile('div', None, 0)
        comments = [sv_util.ustr(c).strip() for c in pattern.icomments(soup, limit=2)]
        self.assertEqual(sorted(comments), sorted(['before header', 'comment']))

    def test_select(self):
        """Test select."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        ids = []
        for el in sv.select('span[id]', soup):
            ids.append(el.attrs['id'])

        self.assertEqual(sorted(['5', 'some-id']), sorted(ids))

    def test_select_order(self):
        """Test select order."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        ids = []
        for el in sv.select('[id]', soup.body):
            ids.append(el.attrs['id'])

        self.assertEqual(['1', '2', '3', '4', '5', 'some-id', '6'], ids)

    def test_select_limit(self):
        """Test select limit."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')

        ids = []
        for el in sv.select('span[id]', soup, limit=1):
            ids.append(el.attrs['id'])

        self.assertEqual(sorted(['5']), sorted(ids))

    def test_select_one(self):
        """Test select one."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        self.assertEqual(
            sv.select('span[id]', soup, limit=1)[0].attrs['id'],
            sv.select_one('span[id]', soup).attrs['id']
        )

    def test_select_one_none(self):
        """Test select one returns none for no match."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        self.assertEqual(None, sv.select_one('h1', soup))

    def test_iselect(self):
        """Test select iterator."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')

        ids = []
        for el in sv.iselect('span[id]', soup):
            ids.append(el.attrs['id'])

        self.assertEqual(sorted(['5', 'some-id']), sorted(ids))

    def test_iselect_order(self):
        """Test select iterator order."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        ids = []
        for el in sv.iselect('[id]', soup):
            ids.append(el.attrs['id'])

        self.assertEqual(['1', '2', '3', '4', '5', 'some-id', '6'], ids)

    def test_match(self):
        """Test matching."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        nodes = sv.select('span[id]', soup)
        self.assertTrue(sv.match('span#\\35', nodes[0]))
        self.assertFalse(sv.match('span#\\35', nodes[1]))

    def test_filter_tag(self):
        """Test filter tag."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        nodes = sv.filter('pre#\\36', soup.html.body)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].attrs['id'], '6')

    def test_filter_tag_order(self):
        """Test filter tag order."""

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        ids = [tag['id'] for tag in sv.filter('[id]', soup.html.body.p)]
        self.assertEqual(['2', '3'], ids)

    def test_filter_list(self):
        """
        Test filter list.

        Even if a list is created from the content of a tag, as long as the
        content is document nodes, filter will still handle it.  It doesn't have
        to be just tags.
        """

        markup = """
        <!-- before header -->
        <html>
        <head>
        </head>
        <body>
        <!-- comment -->
        <p id="1"><code id="2"></code><img id="3" src="./image.png"/></p>
        <pre id="4"></pre>
        <p><span id="5" class="some-class"></span><span id="some-id"></span></p>
        <pre id="6" class='ignore'>
            <!-- don't ignore -->
        </pre>
        </body>
        </html>
        """

        soup = self.soup(markup, 'html.parser')
        nodes = sv.filter('pre#\\36', [el for el in soup.html.body.children])
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].attrs['id'], '6')

    def test_closest_match_parent(self):
        """Test match parent closest."""

        markup = """
        <article id="article">
          <div id="div-01">Here is div-01
            <div id="div-02">Here is div-02
              <div id="div-04">Here is div-04</div>
              <div id="div-03">Here is div-03</div>
            </div>
            <div id="div-05">Here is div-05</div>
          </div>
        </article>
        """

        soup = self.soup(markup, 'html.parser')
        el = sv.select_one('#div-03', soup)
        self.assertTrue(sv.closest('#div-02', el).attrs['id'] == 'div-02')

    def test_closest_match_complex_parent(self):
        """Test closest match complex parent."""

        markup = """
        <article id="article">
          <div id="div-01">Here is div-01
            <div id="div-02">Here is div-02
              <div id="div-04">Here is div-04</div>
              <div id="div-03">Here is div-03</div>
            </div>
            <div id="div-05">Here is div-05</div>
          </div>
        </article>
        """

        soup = self.soup(markup, 'html.parser')
        el = sv.select_one('#div-03', soup)
        self.assertTrue(sv.closest('article > div', el).attrs['id'] == 'div-01')
        self.assertTrue(sv.closest(':not(div)', el).attrs['id'] == 'article')

    def test_closest_match_self(self):
        """Test closest match self."""

        markup = """
        <article id="article">
          <div id="div-01">Here is div-01
            <div id="div-02">Here is div-02
              <div id="div-04">Here is div-04</div>
              <div id="div-03">Here is div-03</div>
            </div>
            <div id="div-05">Here is div-05</div>
          </div>
        </article>
        """

        soup = self.soup(markup, 'html.parser')
        el = sv.select_one('#div-03', soup)
        self.assertTrue(sv.closest('div div', el).attrs['id'] == 'div-03')

    def test_closest_must_be_parent(self):
        """Test that closest only matches parents or self."""

        markup = """
        <article id="article">
          <div id="div-01">Here is div-01
            <div id="div-02">Here is div-02
              <div id="div-04">Here is div-04</div>
              <div id="div-03">Here is div-03</div>
            </div>
            <div id="div-05">Here is div-05</div>
          </div>
        </article>
        """

        soup = self.soup(markup, 'html.parser')
        el = sv.select_one('#div-03', soup)
        self.assertTrue(sv.closest('div #div-05', el) is None)
        self.assertTrue(sv.closest('a', el) is None)

    def test_escape_hyphen(self):
        """Test escape hyphen cases."""

        self.assertEqual(r'\-', sv.escape('-'))
        self.assertEqual(r'--', sv.escape('--'))

    def test_escape_numbers(self):
        """Test escape hyphen cases."""

        self.assertEqual(r'\33 ', sv.escape('3'))
        self.assertEqual(r'-\33 ', sv.escape('-3'))
        self.assertEqual(r'--3', sv.escape('--3'))

    def test_escape_null(self):
        """Test escape null character."""

        self.assertEqual('\ufffdtest', sv.escape('\x00test'))

    def test_escape_ctrl(self):
        """Test escape control character."""

        self.assertEqual(r'\1 test', sv.escape('\x01test'))

    def test_escape_special(self):
        """Test escape special character."""

        self.assertEqual(r'\{\}\[\]\ \(\)', sv.escape('{}[] ()'))

    def test_escape_wide_unicode(self):
        """Test handling of wide Unicode."""

        self.assertEqual('Emoji\\ \U0001F60D', sv.escape('Emoji \U0001F60D'))

    def test_copy_pickle(self):
        """Test copy and pickle."""

        # Test that we can pickle and unpickle
        # We force a pattern that contains all custom types:
        # `Selector`, `NullSelector`, `SelectorTag`, `SelectorAttribute`,
        # `SelectorNth`, `SelectorLang`, `SelectorList`, `Namespaces`,
        # `SelectorContains`, and `CustomSelectors`.
        p1 = sv.compile(
            'p.class#id[id]:nth-child(2):lang(en):focus:contains("text", "other text")',
            {'html': 'http://www.w3.org/TR/html4/'},
            custom={':--header': 'h1, h2, h3, h4, h5, h6'}
        )
        sp1 = pickle.dumps(p1)
        pp1 = pickle.loads(sp1)
        self.assertTrue(pp1 == p1)

        # Test that we pull the same one from cache
        p2 = sv.compile(
            'p.class#id[id]:nth-child(2):lang(en):focus:contains("text", "other text")',
            {'html': 'http://www.w3.org/TR/html4/'},
            custom={':--header': 'h1, h2, h3, h4, h5, h6'}
        )
        self.assertTrue(p1 is p2)

        # Test that we compile a new one when providing a different flags
        p3 = sv.compile(
            'p.class#id[id]:nth-child(2):lang(en):focus:contains("text", "other text")',
            {'html': 'http://www.w3.org/TR/html4/'},
            custom={':--header': 'h1, h2, h3, h4, h5, h6'},
            flags=0x10
        )
        self.assertTrue(p1 is not p3)
        self.assertTrue(p1 != p3)

        # Test that the copy is equivalent, but not same.
        p4 = copy.copy(p1)
        self.assertTrue(p4 is not p1)
        self.assertTrue(p4 == p1)

        p5 = copy.copy(p3)
        self.assertTrue(p5 is not p3)
        self.assertTrue(p5 == p3)
        self.assertTrue(p5 is not p4)

    def test_cache(self):
        """Test cache."""

        sv.purge()
        self.assertEqual(sv.cp._cached_css_compile.cache_info().currsize, 0)
        for x in range(1000):
            value = '[value="{}"]'.format(sv_util.ustr(random.randint(1, 10000)))
            p = sv.compile(value)
            self.assertTrue(p.pattern == value)
            self.assertTrue(sv.cp._cached_css_compile.cache_info().currsize > 0)
        self.assertTrue(sv.cp._cached_css_compile.cache_info().currsize == 500)
        sv.purge()
        self.assertEqual(sv.cp._cached_css_compile.cache_info().currsize, 0)

    def test_recompile(self):
        """If you feed through the same object, it should pass through unless you change parameters."""

        p1 = sv.compile('p[id]')
        p2 = sv.compile(p1)
        self.assertTrue(p1 is p2)

        with pytest.raises(ValueError):
            sv.compile(p1, flags=sv.DEBUG)

        with pytest.raises(ValueError):
            sv.compile(p1, namespaces={"": ""})

        with pytest.raises(ValueError):
            sv.compile(p1, custom={":--header": 'h1, h2, h3, h4, h5, h6'})

    def test_immutable_dict_size(self):
        """Test immutable dictionary."""

        idict = sv.ct.ImmutableDict({'a': 'b', 'c': 'd'})
        self.assertEqual(2, len(idict))


class TestInvalid(util.TestCase):
    """Test invalid."""

    def test_immutable_object(self):
        """Test immutable object."""

        obj = sv.ct.Immutable()

        with self.assertRaises(AttributeError):
            obj.member = 3

    def test_immutable_dict_read_only(self):
        """Test immutable dictionary is read only."""

        idict = sv.ct.ImmutableDict({'a': 'b', 'c': 'd'})
        with self.assertRaises(TypeError):
            idict['a'] = 'f'

    def test_immutable_dict_hashable_value(self):
        """Test immutable dictionary has a hashable value."""

        with self.assertRaises(TypeError):
            sv.ct.ImmutableDict([[3, {}]])

    def test_immutable_dict_hashable_key(self):
        """Test immutable dictionary has a hashable key."""

        with self.assertRaises(TypeError):
            sv.ct.ImmutableDict([[{}, 3]])

    def test_invalid_namespace_type(self):
        """Test invalid namespace type."""

        with self.assertRaises(TypeError):
            sv.ct.Namespaces(((3, 3),))

    def test_invalid_namespace_hashable_value(self):
        """Test namespace has hashable value."""

        with self.assertRaises(TypeError):
            sv.ct.Namespaces({'a': {}})

    def test_invalid_namespace_hashable_key(self):
        """Test namespace key is hashable."""

        with self.assertRaises(TypeError):
            sv.ct.Namespaces({{}: 'string'})

    def test_invalid_custom_type(self):
        """Test invalid custom selector type."""

        with self.assertRaises(TypeError):
            sv.ct.CustomSelectors(((3, 3),))

    def test_invalid_custom_hashable_value(self):
        """Test custom selector has hashable value."""

        with self.assertRaises(TypeError):
            sv.ct.CustomSelectors({'a': {}})

    def test_invalid_custom_hashable_key(self):
        """Test custom selector key is hashable."""

        with self.assertRaises(TypeError):
            sv.ct.CustomSelectors({{}: 'string'})

    def test_invalid_type_input_match(self):
        """Test bad input into the match API."""

        flags = sv.DEBUG

        with self.assertRaises(TypeError):
            sv.match('div', "not a tag", flags=flags)

    def test_invalid_type_input_select(self):
        """Test bad input into the select API."""

        flags = sv.DEBUG

        with self.assertRaises(TypeError):
            sv.select('div', "not a tag", flags=flags)

    def test_invalid_type_input_filter(self):
        """Test bad input into the filter API."""

        flags = sv.DEBUG

        with self.assertRaises(TypeError):
            sv.filter('div', "not a tag", flags=flags)

    def test_invalid_type_input_comments(self):
        """Test bad input into the comments API."""

        flags = sv.DEBUG

        with self.assertRaises(TypeError):
            sv.comments('div', "not a tag", flags=flags)


class TestDeprecated(util.TestCase):
    """Test deprecated."""

    def test_comment(self):
        """Test comment."""

        html = '<div><!-- comments -->text</div>'
        soup = self.soup(html, 'html.parser')

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            sv.comments(soup)
            self.assertTrue(len(w) == 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

    def test_comment_compilied(self):
        """Test compiled comment."""

        html = '<div><!-- comments -->text</div>'
        soup = self.soup(html, 'html.parser')

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            pattern = sv.compile('div')
            pattern.comments(soup)
            self.assertTrue(len(w) == 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

    def test_icomment(self):
        """Test `icomment`."""

        html = '<div><!-- comments -->text</div>'
        soup = self.soup(html, 'html.parser')

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            sv.icomments(soup)
            self.assertTrue(len(w) == 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

    def test_icomment_compilied(self):
        """Test compiled `icomment`."""

        html = '<div><!-- comments -->text</div>'
        soup = self.soup(html, 'html.parser')

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            pattern = sv.compile('div')
            pattern.icomments(soup)
            self.assertTrue(len(w) == 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))


class TestSyntaxErrorReporting(util.TestCase):
    """Test reporting of syntax errors."""

    def test_syntax_error_has_text_and_position(self):
        """Test that selector syntax errors contain the position."""

        with self.assertRaises(sv.SelectorSyntaxError) as cm:
            sv.compile('input.field[type=42]')
        e = cm.exception
        self.assertEqual(e.context, 'input.field[type=42]\n           ^')
        self.assertEqual(e.line, 1)
        self.assertEqual(e.col, 12)

    def test_syntax_error_with_multiple_lines(self):
        """Test that multiline selector errors have the right position."""

        with self.assertRaises(sv.SelectorSyntaxError) as cm:
            sv.compile(
                'input\n'
                '.field[type=42]')
        e = cm.exception
        self.assertEqual(e.context, '    input\n--> .field[type=42]\n          ^')
        self.assertEqual(e.line, 2)
        self.assertEqual(e.col, 7)

    def test_syntax_error_on_third_line(self):
        """Test that multiline selector errors have the right position."""

        with self.assertRaises(sv.SelectorSyntaxError) as cm:
            sv.compile(
                'input:is(\n'
                '  [name=foo]\n'
                '  [type=42]\n'
                ')\n'
            )
        e = cm.exception
        self.assertEqual(e.line, 3)
        self.assertEqual(e.col, 3)

    def test_simple_syntax_error(self):
        """Test a simple syntax error (no context)."""

        with self.assertRaises(sv.SelectorSyntaxError) as cm:
            raise sv.SelectorSyntaxError('Syntax Message')

        e = cm.exception
        self.assertEqual(e.context, None)
        self.assertEqual(e.line, None)
        self.assertEqual(e.col, None)
        self.assertEqual(str(e), 'Syntax Message')
