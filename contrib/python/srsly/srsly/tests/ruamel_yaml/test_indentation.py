# coding: utf-8

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals


import pytest  # NOQA

from .roundtrip import round_trip, round_trip_load, round_trip_dump, dedent, YAML


def rt(s):
    import srsly.ruamel_yaml

    res = srsly.ruamel_yaml.dump(
        srsly.ruamel_yaml.load(s, Loader=srsly.ruamel_yaml.RoundTripLoader),
        Dumper=srsly.ruamel_yaml.RoundTripDumper,
    )
    return res.strip() + "\n"


class TestIndent:
    def test_roundtrip_inline_list(self):
        s = "a: [a, b, c]\n"
        output = rt(s)
        assert s == output

    def test_roundtrip_mapping_of_inline_lists(self):
        s = dedent(
            """\
        a: [a, b, c]
        j: [k, l, m]
        """
        )
        output = rt(s)
        assert s == output

    def test_roundtrip_mapping_of_inline_lists_comments(self):
        s = dedent(
            """\
        # comment A
        a: [a, b, c]
        # comment B
        j: [k, l, m]
        """
        )
        output = rt(s)
        assert s == output

    def test_roundtrip_mapping_of_inline_sequence_eol_comments(self):
        s = dedent(
            """\
        # comment A
        a: [a, b, c]  # comment B
        j: [k, l, m]  # comment C
        """
        )
        output = rt(s)
        assert s == output

    # first test by explicitly setting flow style
    def test_added_inline_list(self):
        import srsly.ruamel_yaml

        s1 = dedent(
            """
        a:
        - b
        - c
        - d
        """
        )
        s = "a: [b, c, d]\n"
        data = srsly.ruamel_yaml.load(s1, Loader=srsly.ruamel_yaml.RoundTripLoader)
        val = data["a"]
        val.fa.set_flow_style()
        # print(type(val), '_yaml_format' in dir(val))
        output = srsly.ruamel_yaml.dump(data, Dumper=srsly.ruamel_yaml.RoundTripDumper)
        assert s == output

    # ############ flow mappings

    def test_roundtrip_flow_mapping(self):
        import srsly.ruamel_yaml

        s = dedent(
            """\
        - {a: 1, b: hallo}
        - {j: fka, k: 42}
        """
        )
        data = srsly.ruamel_yaml.load(s, Loader=srsly.ruamel_yaml.RoundTripLoader)
        output = srsly.ruamel_yaml.dump(data, Dumper=srsly.ruamel_yaml.RoundTripDumper)
        assert s == output

    def test_roundtrip_sequence_of_inline_mappings_eol_comments(self):
        s = dedent(
            """\
        # comment A
        - {a: 1, b: hallo}  # comment B
        - {j: fka, k: 42}  # comment C
        """
        )
        output = rt(s)
        assert s == output

    def test_indent_top_level(self):
        inp = """
        -   a:
            -   b
        """
        round_trip(inp, indent=4)

    def test_set_indent_5_block_list_indent_1(self):
        inp = """
        a:
         -   b: c
         -   1
         -   d:
              -   2
        """
        round_trip(inp, indent=5, block_seq_indent=1)

    def test_set_indent_4_block_list_indent_2(self):
        inp = """
        a:
          - b: c
          - 1
          - d:
              - 2
        """
        round_trip(inp, indent=4, block_seq_indent=2)

    def test_set_indent_3_block_list_indent_0(self):
        inp = """
        a:
        -  b: c
        -  1
        -  d:
           -  2
        """
        round_trip(inp, indent=3, block_seq_indent=0)

    def Xtest_set_indent_3_block_list_indent_2(self):
        inp = """
        a:
          -
           b: c
          -
           1
          -
           d:
             -
              2
        """
        round_trip(inp, indent=3, block_seq_indent=2)

    def test_set_indent_3_block_list_indent_2(self):
        inp = """
        a:
          - b: c
          - 1
          - d:
             - 2
        """
        round_trip(inp, indent=3, block_seq_indent=2)

    def Xtest_set_indent_2_block_list_indent_2(self):
        inp = """
        a:
          -
           b: c
          -
           1
          -
           d:
             -
              2
        """
        round_trip(inp, indent=2, block_seq_indent=2)

    # this is how it should be: block_seq_indent stretches the indent
    def test_set_indent_2_block_list_indent_2(self):
        inp = """
        a:
          - b: c
          - 1
          - d:
            - 2
        """
        round_trip(inp, indent=2, block_seq_indent=2)

    # have to set indent!
    def test_roundtrip_four_space_indents(self):
        # fmt: off
        s = (
            'a:\n'
            '-   foo\n'
            '-   bar\n'
        )
        # fmt: on
        round_trip(s, indent=4)

    def test_roundtrip_four_space_indents_no_fail(self):
        inp = """
        a:
        -   foo
        -   bar
        """
        exp = """
        a:
        - foo
        - bar
        """
        assert round_trip_dump(round_trip_load(inp)) == dedent(exp)


class TestYpkgIndent:
    def test_00(self):
        inp = """
        name       : nano
        version    : 2.3.2
        release    : 1
        homepage   : http://www.nano-editor.org
        source     :
          - http://www.nano-editor.org/dist/v2.3/nano-2.3.2.tar.gz : ff30924807ea289f5b60106be8
        license    : GPL-2.0
        summary    : GNU nano is an easy-to-use text editor
        builddeps  :
          - ncurses-devel
        description: |
            GNU nano is an easy-to-use text editor originally designed
            as a replacement for Pico, the ncurses-based editor from the non-free mailer
            package Pine (itself now available under the Apache License as Alpine).
        """
        round_trip(
            inp,
            indent=4,
            block_seq_indent=2,
            top_level_colon_align=True,
            prefix_colon=" ",
        )


def guess(s):
    from srsly.ruamel_yaml.util import load_yaml_guess_indent

    x, y, z = load_yaml_guess_indent(dedent(s))
    return y, z


class TestGuessIndent:
    def test_guess_20(self):
        inp = """\
        a:
        - 1
        """
        assert guess(inp) == (2, 0)

    def test_guess_42(self):
        inp = """\
        a:
          - 1
        """
        assert guess(inp) == (4, 2)

    def test_guess_42a(self):
        # block seq indent prevails over nested key indent level
        inp = """\
        b:
              a:
                - 1
        """
        assert guess(inp) == (4, 2)

    def test_guess_3None(self):
        inp = """\
        b:
           a: 1
        """
        assert guess(inp) == (3, None)


class TestSeparateMapSeqIndents:
    # using uncommon 6 indent with 3 push in as 2 push in automatically
    # gets you 4 indent even if not set
    def test_00(self):
        # old style
        yaml = YAML()
        yaml.indent = 6
        yaml.block_seq_indent = 3
        inp = """
        a:
           -  1
           -  [1, 2]
        """
        yaml.round_trip(inp)

    def test_01(self):
        yaml = YAML()
        yaml.indent(sequence=6)
        yaml.indent(offset=3)
        inp = """
        a:
           -  1
           -  {b: 3}
        """
        yaml.round_trip(inp)

    def test_02(self):
        yaml = YAML()
        yaml.indent(mapping=5, sequence=6, offset=3)
        inp = """
        a:
             b:
                -  1
                -  [1, 2]
        """
        yaml.round_trip(inp)

    def test_03(self):
        inp = """
        a:
            b:
                c:
                -   1
                -   [1, 2]
        """
        round_trip(inp, indent=4)

    def test_04(self):
        yaml = YAML()
        yaml.indent(mapping=5, sequence=6)
        inp = """
        a:
             b:
             -     1
             -     [1, 2]
             -     {d: 3.14}
        """
        yaml.round_trip(inp)

    def test_issue_51(self):
        yaml = YAML()
        # yaml.map_indent = 2 # the default
        yaml.indent(sequence=4, offset=2)
        yaml.preserve_quotes = True
        yaml.round_trip(
            """
        role::startup::author::rsyslog_inputs:
          imfile:
            - ruleset: 'AEM-slinglog'
              File: '/opt/aem/author/crx-quickstart/logs/error.log'
              startmsg.regex: '^[-+T.:[:digit:]]*'
              tag: 'error'
            - ruleset: 'AEM-slinglog'
              File: '/opt/aem/author/crx-quickstart/logs/stdout.log'
              startmsg.regex: '^[-+T.:[:digit:]]*'
              tag: 'stdout'
        """
        )


# ############ indentation
