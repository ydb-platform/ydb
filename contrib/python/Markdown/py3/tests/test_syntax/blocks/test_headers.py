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

import unittest
from markdown.test_tools import TestCase


class TestSetextHeaders(TestCase):

    def test_setext_h1(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H1
                =============
                """
            ),

            '<h1>This is an H1</h1>'
        )

    def test_setext_h2(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H2
                -------------
                """
            ),

            '<h2>This is an H2</h2>'
        )

    def test_setext_h1_mismatched_length(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H1
                ===
                """
            ),

            '<h1>This is an H1</h1>'
        )

    def test_setext_h2_mismatched_length(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H2
                ---
                """
            ),

            '<h2>This is an H2</h2>'
        )

    def test_setext_h1_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H1
                =============
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h1>This is an H1</h1>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_setext_h2_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is an H2
                -------------
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h2>This is an H2</h2>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    # TODO: fix this
    # see https://johnmacfarlane.net/babelmark2/?normalize=1&text=Paragraph%0AAn+H1%0A%3D%3D%3D%3D%3D
    @unittest.skip('This is broken in Python-Markdown')
    def test_p_followed_by_setext_h1(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is a Paragraph.
                Followed by an H1 with no blank line.
                =====================================
                """
            ),
            self.dedent(
                """
                <p>This is a Paragraph.</p>
                <h1>Followed by an H1 with no blank line.</h1>
                """
            )
        )

    # TODO: fix this
    # see https://johnmacfarlane.net/babelmark2/?normalize=1&text=Paragraph%0AAn+H2%0A-----
    @unittest.skip('This is broken in Python-Markdown')
    def test_p_followed_by_setext_h2(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is a Paragraph.
                Followed by an H2 with no blank line.
                -------------------------------------
                """
            ),
            self.dedent(
                """
                <p>This is a Paragraph.</p>
                <h2>Followed by an H2 with no blank line.</h2>
                """
            )
        )


class TestHashHeaders(TestCase):

    def test_hash_h1_open(self):
        self.assertMarkdownRenders(
            '# This is an H1',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_open(self):
        self.assertMarkdownRenders(
            '## This is an H2',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_open(self):
        self.assertMarkdownRenders(
            '### This is an H3',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_open(self):
        self.assertMarkdownRenders(
            '#### This is an H4',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_open(self):
        self.assertMarkdownRenders(
            '##### This is an H5',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_open(self):
        self.assertMarkdownRenders(
            '###### This is an H6',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_open(self):
        self.assertMarkdownRenders(
            '####### This is an H6',

            '<h6># This is an H6</h6>'
        )

    def test_hash_h1_open_missing_space(self):
        self.assertMarkdownRenders(
            '#This is an H1',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_open_missing_space(self):
        self.assertMarkdownRenders(
            '##This is an H2',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_open_missing_space(self):
        self.assertMarkdownRenders(
            '###This is an H3',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_open_missing_space(self):
        self.assertMarkdownRenders(
            '####This is an H4',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_open_missing_space(self):
        self.assertMarkdownRenders(
            '#####This is an H5',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_open_missing_space(self):
        self.assertMarkdownRenders(
            '######This is an H6',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_open_missing_space(self):
        self.assertMarkdownRenders(
            '#######This is an H6',

            '<h6>#This is an H6</h6>'
        )

    def test_hash_h1_closed(self):
        self.assertMarkdownRenders(
            '# This is an H1 #',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_closed(self):
        self.assertMarkdownRenders(
            '## This is an H2 ##',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_closed(self):
        self.assertMarkdownRenders(
            '### This is an H3 ###',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_closed(self):
        self.assertMarkdownRenders(
            '#### This is an H4 ####',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_closed(self):
        self.assertMarkdownRenders(
            '##### This is an H5 #####',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_closed(self):
        self.assertMarkdownRenders(
            '###### This is an H6 ######',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_closed(self):
        self.assertMarkdownRenders(
            '####### This is an H6 #######',

            '<h6># This is an H6</h6>'
        )

    def test_hash_h1_closed_missing_space(self):
        self.assertMarkdownRenders(
            '#This is an H1#',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_closed_missing_space(self):
        self.assertMarkdownRenders(
            '##This is an H2##',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_closed_missing_space(self):
        self.assertMarkdownRenders(
            '###This is an H3###',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_closed_missing_space(self):
        self.assertMarkdownRenders(
            '####This is an H4####',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_closed_missing_space(self):
        self.assertMarkdownRenders(
            '#####This is an H5#####',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_closed_missing_space(self):
        self.assertMarkdownRenders(
            '######This is an H6######',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_closed_missing_space(self):
        self.assertMarkdownRenders(
            '#######This is an H6#######',

            '<h6>#This is an H6</h6>'
        )

    def test_hash_h1_closed_mismatch(self):
        self.assertMarkdownRenders(
            '# This is an H1 ##',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_closed_mismatch(self):
        self.assertMarkdownRenders(
            '## This is an H2 #',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_closed_mismatch(self):
        self.assertMarkdownRenders(
            '### This is an H3 #',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_closed_mismatch(self):
        self.assertMarkdownRenders(
            '#### This is an H4 #',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_closed_mismatch(self):
        self.assertMarkdownRenders(
            '##### This is an H5 #',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_closed_mismatch(self):
        self.assertMarkdownRenders(
            '###### This is an H6 #',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_closed_mismatch(self):
        self.assertMarkdownRenders(
            '####### This is an H6 ##################',

            '<h6># This is an H6</h6>'
        )

    def test_hash_h1_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                # This is an H1
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h1>This is an H1</h1>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h2_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ## This is an H2
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h2>This is an H2</h2>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h3_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ### This is an H3
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h3>This is an H3</h3>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h4_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                #### This is an H4
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h4>This is an H4</h4>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h5_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ##### This is an H5
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h5>This is an H5</h5>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h6_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ###### This is an H6
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h6>This is an H6</h6>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_hash_h1_leading_space(self):
        self.assertMarkdownRenders(
            ' # This is an H1',

            '<p># This is an H1</p>'
        )

    def test_hash_h2_leading_space(self):
        self.assertMarkdownRenders(
            ' ## This is an H2',

            '<p>## This is an H2</p>'
        )

    def test_hash_h3_leading_space(self):
        self.assertMarkdownRenders(
            ' ### This is an H3',

            '<p>### This is an H3</p>'
        )

    def test_hash_h4_leading_space(self):
        self.assertMarkdownRenders(
            ' #### This is an H4',

            '<p>#### This is an H4</p>'
        )

    def test_hash_h5_leading_space(self):
        self.assertMarkdownRenders(
            ' ##### This is an H5',

            '<p>##### This is an H5</p>'
        )

    def test_hash_h6_leading_space(self):
        self.assertMarkdownRenders(
            ' ###### This is an H6',

            '<p>###### This is an H6</p>'
        )

    def test_hash_h1_open_trailing_space(self):
        self.assertMarkdownRenders(
            '# This is an H1 ',

            '<h1>This is an H1</h1>'
        )

    def test_hash_h2_open_trailing_space(self):
        self.assertMarkdownRenders(
            '## This is an H2 ',

            '<h2>This is an H2</h2>'
        )

    def test_hash_h3_open_trailing_space(self):
        self.assertMarkdownRenders(
            '### This is an H3 ',

            '<h3>This is an H3</h3>'
        )

    def test_hash_h4_open_trailing_space(self):
        self.assertMarkdownRenders(
            '#### This is an H4 ',

            '<h4>This is an H4</h4>'
        )

    def test_hash_h5_open_trailing_space(self):
        self.assertMarkdownRenders(
            '##### This is an H5 ',

            '<h5>This is an H5</h5>'
        )

    def test_hash_h6_open_trailing_space(self):
        self.assertMarkdownRenders(
            '###### This is an H6 ',

            '<h6>This is an H6</h6>'
        )

    def test_hash_gt6_open_trailing_space(self):
        self.assertMarkdownRenders(
            '####### This is an H6 ',

            '<h6># This is an H6</h6>'
        )

    # TODO: Possibly change the following behavior. While this follows the behavior
    # of markdown.pl, it is rather uncommon and not necessarily intuitive.
    # See: https://johnmacfarlane.net/babelmark2/?normalize=1&text=%23+This+is+an+H1+%23+
    def test_hash_h1_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '# This is an H1 # ',

            '<h1>This is an H1 #</h1>'
        )

    def test_hash_h2_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '## This is an H2 ## ',

            '<h2>This is an H2 ##</h2>'
        )

    def test_hash_h3_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '### This is an H3 ### ',

            '<h3>This is an H3 ###</h3>'
        )

    def test_hash_h4_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '#### This is an H4 #### ',

            '<h4>This is an H4 ####</h4>'
        )

    def test_hash_h5_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '##### This is an H5 ##### ',

            '<h5>This is an H5 #####</h5>'
        )

    def test_hash_h6_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '###### This is an H6 ###### ',

            '<h6>This is an H6 ######</h6>'
        )

    def test_hash_gt6_closed_trailing_space(self):
        self.assertMarkdownRenders(
            '####### This is an H6 ####### ',

            '<h6># This is an H6 #######</h6>'
        )

    def test_no_blank_lines_between_hashs(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                # This is an H1
                ## This is an H2
                """
            ),
            self.dedent(
                """
                <h1>This is an H1</h1>
                <h2>This is an H2</h2>
                """
            )
        )

    def test_random_hash_levels(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                ### H3
                ###### H6
                # H1
                ##### H5
                #### H4
                ## H2
                ### H3
                """
            ),
            self.dedent(
                """
                <h3>H3</h3>
                <h6>H6</h6>
                <h1>H1</h1>
                <h5>H5</h5>
                <h4>H4</h4>
                <h2>H2</h2>
                <h3>H3</h3>
                """
            )
        )

    def test_hash_followed_by_p(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                # This is an H1
                Followed by a Paragraph with no blank line.
                """
            ),
            self.dedent(
                """
                <h1>This is an H1</h1>
                <p>Followed by a Paragraph with no blank line.</p>
                """
            )
        )

    def test_p_followed_by_hash(self):
        self.assertMarkdownRenders(
            self.dedent(
                """
                This is a Paragraph.
                # Followed by an H1 with no blank line.
                """
            ),
            self.dedent(
                """
                <p>This is a Paragraph.</p>
                <h1>Followed by an H1 with no blank line.</h1>
                """
            )
        )

    def test_escaped_hash(self):
        self.assertMarkdownRenders(
            "### H3 \\###",
            self.dedent(
                """
                <h3>H3 #</h3>
                """
            )
        )

    def test_unescaped_hash(self):
        self.assertMarkdownRenders(
            "### H3 \\\\###",
            self.dedent(
                """
                <h3>H3 \\</h3>
                """
            )
        )
