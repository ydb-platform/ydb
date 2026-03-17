"""Test scope selectors."""
from __future__ import unicode_literals
from .. import util
import soupsieve as sv


class TestScope(util.TestCase):
    """Test scope selectors."""

    MARKUP = """
    <html id="root">
    <head>
    </head>
    <body>
    <div id="div">
    <p id="0" class="somewordshere">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3" class="herewords">Direct child</span>
    <pre id="pre" class="wordshere">
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    </body>
    </html>
    """

    def test_scope_is_root(self):
        """Test scope is the root when the a specific element is not the target of the select call."""

        # Scope is root when applied to a document node
        self.assert_selector(
            self.MARKUP,
            ":scope",
            ["root"],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            ":scope > body > div",
            ["div"],
            flags=util.HTML
        )

    def test_scope_cannot_select_target(self):
        """Test that scope, the element which scope is called on, cannot be selected."""

        for parser in util.available_parsers(
                'html.parser', 'lxml', 'html5lib', 'xml'):
            soup = self.soup(self.MARKUP, parser)
            el = soup.html

            # Scope is the element we are applying the select to, and that element is never returned
            self.assertTrue(len(sv.select(':scope', el, flags=sv.DEBUG)) == 0)

    def test_scope_is_select_target(self):
        """Test that scope is the element which scope is called on."""

        for parser in util.available_parsers(
                'html.parser', 'lxml', 'html5lib', 'xml'):
            soup = self.soup(self.MARKUP, parser)
            el = soup.html

            # Scope here means the current element under select
            ids = []
            for el in sv.select(':scope div', el, flags=sv.DEBUG):
                ids.append(el.attrs['id'])
            self.assertEqual(sorted(ids), sorted(['div']))

            el = soup.body
            ids = []
            for el in sv.select(':scope div', el, flags=sv.DEBUG):
                ids.append(el.attrs['id'])
            self.assertEqual(sorted(ids), sorted(['div']))

            # `div` is the current element under select, and it has no `div` elements.
            el = soup.div
            ids = []
            for el in sv.select(':scope div', el, flags=sv.DEBUG):
                ids.append(el.attrs['id'])
            self.assertEqual(sorted(ids), sorted([]))

            # `div` does have an element with the class `.wordshere`
            ids = []
            for el in sv.select(':scope .wordshere', el, flags=sv.DEBUG):
                ids.append(el.attrs['id'])
            self.assertEqual(sorted(ids), sorted(['pre']))
