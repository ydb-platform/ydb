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

from markdown.test_tools import TestCase, recursionlimit


class TestBlockquoteBlocks(TestCase):

    # TODO: Move legacy tests here

    def test_nesting_limit(self):
        # Test that the nesting limit is within 100 levels of recursion limit. Future code changes could cause the
        # recursion limit to need adjusted here. We need to account for all of Markdown's internal calls. Finally, we
        # need to account for the 100 level cushion which we are testing.
        with recursionlimit(120):
            self.assertMarkdownRenders(
                '>>>>>>>>>>',
                self.dedent(
                    """
                    <blockquote>
                    <blockquote>
                    <blockquote>
                    <blockquote>
                    <blockquote>
                    <p>&gt;&gt;&gt;&gt;&gt;</p>
                    </blockquote>
                    </blockquote>
                    </blockquote>
                    </blockquote>
                    </blockquote>
                    """
                )
            )
