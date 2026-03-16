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

Copyright 2007-2023 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).
"""

from markdown.test_tools import TestCase
from markdown.extensions.tables import TableExtension


class TestTableBlocks(TestCase):

    def test_empty_cells(self):
        """Empty cells (`nbsp`)."""

        text = """
   | Second Header
------------- | -------------
   | Content Cell
Content Cell  |  
"""

        self.assertMarkdownRenders(
            text,
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th> </th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td> </td>
                <td>Content Cell</td>
                </tr>
                <tr>
                <td>Content Cell</td>
                <td> </td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_no_sides(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                First Header  | Second Header
                ------------- | -------------
                Content Cell  | Content Cell
                Content Cell  | Content Cell
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th>First Header</th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_both_sides(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                | First Header  | Second Header |
                | ------------- | ------------- |
                | Content Cell  | Content Cell  |
                | Content Cell  | Content Cell  |
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th>First Header</th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_align_columns(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                | Item      | Value |
                | :-------- | -----:|
                | Computer  | $1600 |
                | Phone     |   $12 |
                | Pipe      |    $1 |
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th style="text-align: left;">Item</th>
                <th style="text-align: right;">Value</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td style="text-align: left;">Computer</td>
                <td style="text-align: right;">$1600</td>
                </tr>
                <tr>
                <td style="text-align: left;">Phone</td>
                <td style="text-align: right;">$12</td>
                </tr>
                <tr>
                <td style="text-align: left;">Pipe</td>
                <td style="text-align: right;">$1</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_styles_in_tables(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                | Function name | Description                    |
                | ------------- | ------------------------------ |
                | `help()`      | Display the help window.       |
                | `destroy()`   | **Destroy your computer!**     |
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th>Function name</th>
                <th>Description</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td><code>help()</code></td>
                <td>Display the help window.</td>
                </tr>
                <tr>
                <td><code>destroy()</code></td>
                <td><strong>Destroy your computer!</strong></td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_align_three(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                |foo|bar|baz|
                |:--|:-:|--:|
                |   | Q |   |
                |W  |   |  W|
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th style="text-align: left;">foo</th>
                <th style="text-align: center;">bar</th>
                <th style="text-align: right;">baz</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td style="text-align: left;"></td>
                <td style="text-align: center;">Q</td>
                <td style="text-align: right;"></td>
                </tr>
                <tr>
                <td style="text-align: left;">W</td>
                <td style="text-align: center;"></td>
                <td style="text-align: right;">W</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_three_columns(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                foo|bar|baz
                ---|---|---
                   | Q |
                 W |   | W
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th>foo</th>
                <th>bar</th>
                <th>baz</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td></td>
                <td>Q</td>
                <td></td>
                </tr>
                <tr>
                <td>W</td>
                <td></td>
                <td>W</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_three_spaces_prefix(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Three spaces in front of a table:

                   First Header | Second Header
                   ------------ | -------------
                   Content Cell | Content Cell
                   Content Cell | Content Cell

                   | First Header | Second Header |
                   | ------------ | ------------- |
                   | Content Cell | Content Cell  |
                   | Content Cell | Content Cell  |
                """),
            self.dedent(
                """
                <p>Three spaces in front of a table:</p>
                <table>
                <thead>
                <tr>
                <th>First Header</th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>First Header</th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                <tr>
                <td>Content Cell</td>
                <td>Content Cell</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_code_block_table(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Four spaces is a code block:

                    First Header | Second Header
                    ------------ | -------------
                    Content Cell | Content Cell
                    Content Cell | Content Cell

                | First Header | Second Header |
                | ------------ | ------------- |
                """),
            self.dedent(
                """
                <p>Four spaces is a code block:</p>
                <pre><code>First Header | Second Header
                ------------ | -------------
                Content Cell | Content Cell
                Content Cell | Content Cell
                </code></pre>
                <table>
                <thead>
                <tr>
                <th>First Header</th>
                <th>Second Header</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td></td>
                <td></td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_inline_code_blocks(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                More inline code block tests

                Column 1 | Column 2 | Column 3
                ---------|----------|---------
                word 1   | word 2   | word 3
                word 1   | `word 2` | word 3
                word 1   | \\`word 2 | word 3
                word 1   | `word 2 | word 3
                word 1   | `word |2` | word 3
                words    |`` some | code `` | more words
                words    |``` some | code ``` | more words
                words    |```` some | code ```` | more words
                words    |`` some ` | ` code `` | more words
                words    |``` some ` | ` code ``` | more words
                words    |```` some ` | ` code ```` | more words
                """),
            self.dedent(
                """
                <p>More inline code block tests</p>
                <table>
                <thead>
                <tr>
                <th>Column 1</th>
                <th>Column 2</th>
                <th>Column 3</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>word 1</td>
                <td>word 2</td>
                <td>word 3</td>
                </tr>
                <tr>
                <td>word 1</td>
                <td><code>word 2</code></td>
                <td>word 3</td>
                </tr>
                <tr>
                <td>word 1</td>
                <td>`word 2</td>
                <td>word 3</td>
                </tr>
                <tr>
                <td>word 1</td>
                <td>`word 2</td>
                <td>word 3</td>
                </tr>
                <tr>
                <td>word 1</td>
                <td><code>word |2</code></td>
                <td>word 3</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some | code</code></td>
                <td>more words</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some | code</code></td>
                <td>more words</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some | code</code></td>
                <td>more words</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some ` | ` code</code></td>
                <td>more words</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some ` | ` code</code></td>
                <td>more words</td>
                </tr>
                <tr>
                <td>words</td>
                <td><code>some ` | ` code</code></td>
                <td>more words</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_issue_440(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                A test for issue #440:

                foo | bar
                --- | ---
                foo | (`bar`) and `baz`.
                """),
            self.dedent(
                """
                <p>A test for issue #440:</p>
                <table>
                <thead>
                <tr>
                <th>foo</th>
                <th>bar</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>foo</td>
                <td>(<code>bar</code>) and <code>baz</code>.</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_lists_not_tables(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Lists are not tables

                 - this | should | not
                 - be | a | table
                """),
            self.dedent(
                """
                <p>Lists are not tables</p>
                <ul>
                <li>this | should | not</li>
                <li>be | a | table</li>
                </ul>
                """
            ),
            extensions=['tables']
        )

    def test_issue_449(self):
        self.assertMarkdownRenders(
            self.dedent(
                r"""
                Add tests for issue #449

                Odd backticks | Even backticks
                ------------ | -------------
                ``[!\"\#$%&'()*+,\-./:;<=>?@\[\\\]^_`{|}~]`` | ``[!\"\#$%&'()*+,\-./:;<=>?@\[\\\]^`_`{|}~]``

                Escapes | More Escapes
                ------- | ------
                `` `\`` | `\`

                Only the first backtick can be escaped

                Escaped | Bacticks
                ------- | ------
                \`` \`  | \`\`

                Test escaped pipes

                Column 1 | Column 2
                -------- | --------
                `|` \|   | Pipes are okay in code and escaped. \|

                | Column 1 | Column 2 |
                | -------- | -------- |
                | row1     | row1    \|
                | row2     | row2     |

                Test header escapes

                | `` `\`` \| | `\` \|
                | ---------- | ---- |
                | row1       | row1 |
                | row2       | row2 |

                Escaped pipes in format row should not be a table

                | Column1   | Column2 |
                | ------- \|| ------- |
                | row1      | row1    |
                | row2      | row2    |

                Test escaped code in Table

                Should not be code | Should be code
                ------------------ | --------------
                \`Not code\`       | \\`code`
                \\\`Not code\\\`   | \\\\`code`
                """),
            self.dedent(
                """
                <p>Add tests for issue #449</p>
                <table>
                <thead>
                <tr>
                <th>Odd backticks</th>
                <th>Even backticks</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td><code>[!\\"\\#$%&amp;'()*+,\\-./:;&lt;=&gt;?@\\[\\\\\\]^_`{|}~]</code></td>
                <td><code>[!\\"\\#$%&amp;'()*+,\\-./:;&lt;=&gt;?@\\[\\\\\\]^`_`{|}~]</code></td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Escapes</th>
                <th>More Escapes</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td><code>`\\</code></td>
                <td><code>\\</code></td>
                </tr>
                </tbody>
                </table>
                <p>Only the first backtick can be escaped</p>
                <table>
                <thead>
                <tr>
                <th>Escaped</th>
                <th>Bacticks</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>`<code>\\</code></td>
                <td>``</td>
                </tr>
                </tbody>
                </table>
                <p>Test escaped pipes</p>
                <table>
                <thead>
                <tr>
                <th>Column 1</th>
                <th>Column 2</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td><code>|</code> |</td>
                <td>Pipes are okay in code and escaped. |</td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Column 1</th>
                <th>Column 2</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>row1</td>
                <td>row1    |</td>
                </tr>
                <tr>
                <td>row2</td>
                <td>row2</td>
                </tr>
                </tbody>
                </table>
                <p>Test header escapes</p>
                <table>
                <thead>
                <tr>
                <th><code>`\\</code> |</th>
                <th><code>\\</code> |</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>row1</td>
                <td>row1</td>
                </tr>
                <tr>
                <td>row2</td>
                <td>row2</td>
                </tr>
                </tbody>
                </table>
                <p>Escaped pipes in format row should not be a table</p>
                <p>| Column1   | Column2 |
                | ------- || ------- |
                | row1      | row1    |
                | row2      | row2    |</p>
                <p>Test escaped code in Table</p>
                <table>
                <thead>
                <tr>
                <th>Should not be code</th>
                <th>Should be code</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>`Not code`</td>
                <td>\\<code>code</code></td>
                </tr>
                <tr>
                <td>\\`Not code\\`</td>
                <td>\\\\<code>code</code></td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=['tables']
        )

    def test_single_column_tables(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                Single column tables

                | Is a Table |
                | ---------- |

                | Is a Table
                | ----------

                Is a Table |
                ---------- |

                | Is a Table |
                | ---------- |
                | row        |

                | Is a Table
                | ----------
                | row

                Is a Table |
                ---------- |
                row        |

                | Is not a Table
                --------------
                | row

                Is not a Table |
                --------------
                row            |

                | Is not a Table
                | --------------
                row

                Is not a Table |
                -------------- |
                row
                """),
            self.dedent(
                """
                <p>Single column tables</p>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td></td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td></td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td></td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>row</td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>row</td>
                </tr>
                </tbody>
                </table>
                <table>
                <thead>
                <tr>
                <th>Is a Table</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td>row</td>
                </tr>
                </tbody>
                </table>
                <h2>| Is not a Table</h2>
                <p>| row</p>
                <h2>Is not a Table |</h2>
                <p>row            |</p>
                <p>| Is not a Table
                | --------------
                row</p>
                <p>Is not a Table |
                -------------- |
                row</p>
                """
            ),
            extensions=['tables']
        )

    def test_align_columns_legacy(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                | Item      | Value |
                | :-------- | -----:|
                | Computer  | $1600 |
                | Phone     |   $12 |
                | Pipe      |    $1 |
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th align="left">Item</th>
                <th align="right">Value</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td align="left">Computer</td>
                <td align="right">$1600</td>
                </tr>
                <tr>
                <td align="left">Phone</td>
                <td align="right">$12</td>
                </tr>
                <tr>
                <td align="left">Pipe</td>
                <td align="right">$1</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=[TableExtension(use_align_attribute=True)]
        )

    def test_align_three_legacy(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                |foo|bar|baz|
                |:--|:-:|--:|
                |   | Q |   |
                |W  |   |  W|
                """
            ),
            self.dedent(
                """
                <table>
                <thead>
                <tr>
                <th align="left">foo</th>
                <th align="center">bar</th>
                <th align="right">baz</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                <td align="left"></td>
                <td align="center">Q</td>
                <td align="right"></td>
                </tr>
                <tr>
                <td align="left">W</td>
                <td align="center"></td>
                <td align="right">W</td>
                </tr>
                </tbody>
                </table>
                """
            ),
            extensions=[TableExtension(use_align_attribute=True)]
        )
