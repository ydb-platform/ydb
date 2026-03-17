"""
Python Markdown

A Python implementation of John Gruber's Markdown.

Documentation: https://python-markdown.github.io/
GitHub: https://github.com/Python-Markdown/markdown/
PyPI: https://pypi.org/project/Markdown/

Started by Manfred Stienstra (http://www.dwerg.net/).
Maintained for a few years by Yuri Takhteyev (http://www.freewisdom.org).
Currently maintained by Waylan Limberg (https://github.com/waylan),
Dmitry Shachnev (https://github.com/mitya57) and Isaac Muse (https://github.com/facelessuser).

Copyright 2007-2019 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).
"""

from markdown.test_tools import TestCase
import markdown
import markdown.extensions.codehilite
import os

try:
    import pygments  # noqa
    import pygments.formatters  # noqa
    has_pygments = True
except ImportError:
    has_pygments = False

# The version required by the tests is the version specified and installed in the `pygments` tox environment.
# In any environment where the `PYGMENTS_VERSION` environment variable is either not defined or doesn't
# match the version of Pygments installed, all tests which rely in Pygments will be skipped.
required_pygments_version = os.environ.get('PYGMENTS_VERSION', '')


class TestFencedCode(TestCase):

    def testBasicFence(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                A paragraph before a fenced code block:

                ```
                Fenced code block
                ```
                '''
            ),
            self.dedent(
                '''
                <p>A paragraph before a fenced code block:</p>
                <pre><code>Fenced code block
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testNestedFence(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ````

                ```
                ````
                '''
            ),
            self.dedent(
                '''
                <pre><code>
                ```
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedTildes(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ~~~
                # Arbitrary code
                ``` # these backticks will not close the block
                ~~~
                '''
            ),
            self.dedent(
                '''
                <pre><code># Arbitrary code
                ``` # these backticks will not close the block
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageNoDot(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` python
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageWithDot(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` .python
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def test_fenced_code_in_raw_html(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                <details>
                ```
                Begone placeholders!
                ```
                </details>
                """
            ),
            self.dedent(
                """
                <details>

                <pre><code>Begone placeholders!
                </code></pre>

                </details>
                """
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` {.python}
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedMultipleClassesInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` {.python .foo .bar}
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre class="foo bar"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedIdInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { #foo }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedIdAndLangInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedIdAndLangAndClassInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo .bar }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo" class="bar"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageIdAndPygmentsDisabledInAttrNoCodehilite(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo use_pygments=False }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageIdAndPygmentsEnabledInAttrNoCodehilite(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo use_pygments=True }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code']
        )

    def testFencedLanguageNoCodehiliteWithAttrList(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python foo=bar }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python" foo="bar"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )

    def testFencedLanguagePygmentsDisabledInAttrNoCodehiliteWithAttrList(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python foo=bar use_pygments=False }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python" foo="bar"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )

    def testFencedLanguagePygmentsEnabledInAttrNoCodehiliteWithAttrList(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python foo=bar use_pygments=True }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )

    def testFencedLanguageNoPrefix(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` python
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="python"># Some python code
                </code></pre>
                '''
            ),
            extensions=[markdown.extensions.fenced_code.FencedCodeExtension(lang_prefix='')]
        )

    def testFencedLanguageAltPrefix(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` python
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="lang-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=[markdown.extensions.fenced_code.FencedCodeExtension(lang_prefix='lang-')]
        )

    def testFencedCodeEscapedAttrs(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { ."weird #"foo bar=">baz }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="&quot;foo"><code class="language-&quot;weird" bar="&quot;&gt;baz"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )

    def testFencedCodeCurlyInAttrs(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { data-test="{}" }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code data-test="{}"># Some python code
                </code></pre>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )

    def testFencedCodeMismatchedCurlyInAttrs(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { data-test="{}" } }
                # Some python code
                ```
                ```
                test
                ```
                '''
            ),
            self.dedent(
                '''
                <p>``` { data-test="{}" } }</p>
                <h1>Some python code</h1>
                <pre><code></code></pre>
                <p>test
                ```</p>
                '''
            ),
            extensions=['fenced_code', 'attr_list']
        )


class TestFencedCodeWithCodehilite(TestCase):

    def setUp(self):
        if has_pygments and pygments.__version__ != required_pygments_version:
            self.skipTest(f'Pygments=={required_pygments_version} is required')

    def test_shebang(self):

        if has_pygments:
            expected = '''
            <div class="codehilite"><pre><span></span><code>#!test
            </code></pre></div>
            '''
        else:
            expected = '''
            <pre class="codehilite"><code>#!test
            </code></pre>
            '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```
                #!test
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(linenums=None, guess_lang=False),
                'fenced_code'
            ]
        )

    def testFencedCodeWithHighlightLines(self):
        if has_pygments:
            expected = self.dedent(
                '''
                <div class="codehilite"><pre><span></span><code><span class="hll">line 1
                </span>line 2
                <span class="hll">line 3
                </span></code></pre></div>
                '''
            )
        else:
            expected = self.dedent(
                    '''
                    <pre class="codehilite"><code>line 1
                    line 2
                    line 3
                    </code></pre>
                    '''
                )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```hl_lines="1 3"
                line 1
                line 2
                line 3
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(linenums=None, guess_lang=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageAndHighlightLines(self):
        if has_pygments:
            expected = (
                '<div class="codehilite"><pre><span></span><code>'
                '<span class="hll"><span class="n">line</span> <span class="mi">1</span>\n'
                '</span><span class="n">line</span> <span class="mi">2</span>\n'
                '<span class="hll"><span class="n">line</span> <span class="mi">3</span>\n'
                '</span></code></pre></div>'
            )
        else:
            expected = self.dedent(
                    '''
                    <pre class="codehilite"><code class="language-python">line 1
                    line 2
                    line 3
                    </code></pre>
                    '''
                )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` .python hl_lines="1 3"
                line 1
                line 2
                line 3
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(linenums=None, guess_lang=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageAndPygmentsDisabled(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` .python
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(use_pygments=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageDoubleEscape(self):
        if has_pygments:
            expected = (
                '<div class="codehilite"><pre><span></span><code>'
                '<span class="p">&lt;</span><span class="nt">span</span>'
                '<span class="p">&gt;</span>This<span class="ni">&amp;amp;</span>'
                'That<span class="p">&lt;/</span><span class="nt">span</span>'
                '<span class="p">&gt;</span>\n'
                '</code></pre></div>'
            )
        else:
            expected = (
                '<pre class="codehilite"><code class="language-html">'
                '&lt;span&gt;This&amp;amp;That&lt;/span&gt;\n'
                '</code></pre>'
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```html
                <span>This&amp;That</span>
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(),
                'fenced_code'
            ]
        )

    def testFencedAmps(self):
        if has_pygments:
            expected = self.dedent(
                '''
                <div class="codehilite"><pre><span></span><code>&amp;
                &amp;amp;
                &amp;amp;amp;
                </code></pre></div>
                '''
            )
        else:
            expected = self.dedent(
                '''
                <pre class="codehilite"><code class="language-text">&amp;
                &amp;amp;
                &amp;amp;amp;
                </code></pre>
                '''
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```text
                &
                &amp;
                &amp;amp;
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(),
                'fenced_code'
            ]
        )

    def testFencedCodeWithHighlightLinesInAttr(self):
        if has_pygments:
            expected = self.dedent(
                '''
                <div class="codehilite"><pre><span></span><code><span class="hll">line 1
                </span>line 2
                <span class="hll">line 3
                </span></code></pre></div>
                '''
            )
        else:
            expected = self.dedent(
                    '''
                    <pre class="codehilite"><code>line 1
                    line 2
                    line 3
                    </code></pre>
                    '''
                )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```{ hl_lines="1 3" }
                line 1
                line 2
                line 3
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(linenums=None, guess_lang=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageAndHighlightLinesInAttr(self):
        if has_pygments:
            expected = (
                '<div class="codehilite"><pre><span></span><code>'
                '<span class="hll"><span class="n">line</span> <span class="mi">1</span>\n'
                '</span><span class="n">line</span> <span class="mi">2</span>\n'
                '<span class="hll"><span class="n">line</span> <span class="mi">3</span>\n'
                '</span></code></pre></div>'
            )
        else:
            expected = self.dedent(
                    '''
                    <pre class="codehilite"><code class="language-python">line 1
                    line 2
                    line 3
                    </code></pre>
                    '''
                )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python hl_lines="1 3" }
                line 1
                line 2
                line 3
                ```
                '''
            ),
            expected,
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(linenums=None, guess_lang=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageIdInAttrAndPygmentsDisabled(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(use_pygments=False),
                'fenced_code'
            ]
        )

    def testFencedLanguageIdAndPygmentsDisabledInAttr(self):
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python #foo use_pygments=False }
                # Some python code
                ```
                '''
            ),
            self.dedent(
                '''
                <pre id="foo"><code class="language-python"># Some python code
                </code></pre>
                '''
            ),
            extensions=['codehilite', 'fenced_code']
        )

    def testFencedLanguageAttrCssclass(self):
        if has_pygments:
            expected = self.dedent(
                '''
                <div class="pygments"><pre><span></span><code><span class="c1"># Some python code</span>
                </code></pre></div>
                '''
            )
        else:
            expected = (
                '<pre class="pygments"><code class="language-python"># Some python code\n'
                '</code></pre>'
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python css_class='pygments' }
                # Some python code
                ```
                '''
            ),
            expected,
            extensions=['codehilite', 'fenced_code']
        )

    def testFencedLanguageAttrLinenums(self):
        if has_pygments:
            expected = (
                '<table class="codehilitetable"><tr>'
                '<td class="linenos"><div class="linenodiv"><pre>1</pre></div></td>'
                '<td class="code"><div class="codehilite"><pre><span></span>'
                '<code><span class="c1"># Some python code</span>\n'
                '</code></pre></div>\n'
                '</td></tr></table>'
            )
        else:
            expected = (
                '<pre class="codehilite"><code class="language-python linenums"># Some python code\n'
                '</code></pre>'
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python linenums=True }
                # Some python code
                ```
                '''
            ),
            expected,
            extensions=['codehilite', 'fenced_code']
        )

    def testFencedLanguageAttrGuesslang(self):
        if has_pygments:
            expected = self.dedent(
                '''
                <div class="codehilite"><pre><span></span><code># Some python code
                </code></pre></div>
                '''
            )
        else:
            expected = (
                '<pre class="codehilite"><code># Some python code\n'
                '</code></pre>'
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { guess_lang=False }
                # Some python code
                ```
                '''
            ),
            expected,
            extensions=['codehilite', 'fenced_code']
        )

    def testFencedLanguageAttrNoclasses(self):
        if has_pygments:
            expected = (
                '<div class="codehilite" style="background: #f8f8f8">'
                '<pre style="line-height: 125%; margin: 0;"><span></span><code>'
                '<span style="color: #408080; font-style: italic"># Some python code</span>\n'
                '</code></pre></div>'
            )
        else:
            expected = (
                '<pre class="codehilite"><code class="language-python"># Some python code\n'
                '</code></pre>'
            )
        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python noclasses=True }
                # Some python code
                ```
                '''
            ),
            expected,
            extensions=['codehilite', 'fenced_code']
        )

    def testFencedMultipleBlocksSameStyle(self):
        if has_pygments:
            # See also: https://github.com/Python-Markdown/markdown/issues/1240
            expected = (
                '<div class="codehilite" style="background: #202020"><pre style="line-height: 125%; margin: 0;">'
                '<span></span><code><span style="color: #999999; font-style: italic"># First Code Block</span>\n'
                '</code></pre></div>\n\n'
                '<p>Normal paragraph</p>\n'
                '<div class="codehilite" style="background: #202020"><pre style="line-height: 125%; margin: 0;">'
                '<span></span><code><span style="color: #999999; font-style: italic"># Second Code Block</span>\n'
                '</code></pre></div>'
            )
        else:
            expected = '''
            <pre class="codehilite"><code class="language-python"># First Code Block
            </code></pre>

            <p>Normal paragraph</p>
            <pre class="codehilite"><code class="language-python"># Second Code Block
            </code></pre>
            '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ``` { .python }
                # First Code Block
                ```

                Normal paragraph

                ``` { .python }
                # Second Code Block
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(pygments_style="native", noclasses=True),
                'fenced_code'
            ]
        )

    def testCustomPygmentsFormatter(self):
        if has_pygments:
            class CustomFormatter(pygments.formatters.HtmlFormatter):
                def wrap(self, source, outfile):
                    return self._wrap_div(self._wrap_code(source))

                def _wrap_code(self, source):
                    yield 0, '<code>'
                    for i, t in source:
                        if i == 1:
                            t += '<br>'
                        yield i, t
                    yield 0, '</code>'

            expected = '''
            <div class="codehilite"><code>hello world
            <br>hello another world
            <br></code></div>
            '''

        else:
            CustomFormatter = None
            expected = '''
            <pre class="codehilite"><code>hello world
            hello another world
            </code></pre>
            '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```
                hello world
                hello another world
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(
                    pygments_formatter=CustomFormatter,
                    guess_lang=False,
                ),
                'fenced_code'
            ]
        )

    def testPygmentsAddLangClassFormatter(self):
        if has_pygments:
            class CustomAddLangHtmlFormatter(pygments.formatters.HtmlFormatter):
                def __init__(self, lang_str='', **options):
                    super().__init__(**options)
                    self.lang_str = lang_str

                def _wrap_code(self, source):
                    yield 0, f'<code class="{self.lang_str}">'
                    yield from source
                    yield 0, '</code>'

            expected = '''
                <div class="codehilite"><pre><span></span><code class="language-text">hello world
                hello another world
                </code></pre></div>
                '''
        else:
            CustomAddLangHtmlFormatter = None
            expected = '''
                <pre class="codehilite"><code class="language-text">hello world
                hello another world
                </code></pre>
                '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```text
                hello world
                hello another world
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(
                    guess_lang=False,
                    pygments_formatter=CustomAddLangHtmlFormatter,
                ),
                'fenced_code'
            ]
        )

    def testSvgCustomPygmentsFormatter(self):
        if has_pygments:
            expected = '''
            <?xml version="1.0"?>
            <!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.0//EN" "http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd">
            <svg xmlns="http://www.w3.org/2000/svg">
            <g font-family="monospace" font-size="14px">
            <text x="0" y="14" xml:space="preserve">hello&#160;world</text>
            <text x="0" y="33" xml:space="preserve">hello&#160;another&#160;world</text>
            <text x="0" y="52" xml:space="preserve"></text></g></svg>
            '''

        else:
            expected = '''
            <pre class="codehilite"><code>hello world
            hello another world
            </code></pre>
            '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```
                hello world
                hello another world
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(
                    pygments_formatter='svg',
                    linenos=False,
                    guess_lang=False,
                ),
                'fenced_code'
            ]
        )

    def testInvalidCustomPygmentsFormatter(self):
        if has_pygments:
            expected = '''
            <div class="codehilite"><pre><span></span><code>hello world
            hello another world
            </code></pre></div>
            '''

        else:
            expected = '''
            <pre class="codehilite"><code>hello world
            hello another world
            </code></pre>
            '''

        self.assertMarkdownRenders(
            self.dedent(
                '''
                ```
                hello world
                hello another world
                ```
                '''
            ),
            self.dedent(
                expected
            ),
            extensions=[
                markdown.extensions.codehilite.CodeHiliteExtension(
                    pygments_formatter='invalid',
                    guess_lang=False,
                ),
                'fenced_code'
            ]
        )
