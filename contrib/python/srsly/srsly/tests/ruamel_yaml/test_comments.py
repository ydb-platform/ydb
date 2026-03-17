# coding: utf-8

"""
comment testing is all about roundtrips
these can be done in the "old" way by creating a file.data and file.roundtrip
but there is little flexibility in doing that

but some things are not easily tested, eog. how a
roundtrip changes

"""

import pytest
import sys

from .roundtrip import round_trip, dedent, round_trip_load, round_trip_dump


class TestComments:
    def test_no_end_of_file_eol(self):
        """not excluding comments caused some problems if at the end of
        the file without a newline. First error, then included \0 """
        x = """\
        - europe: 10 # abc"""
        round_trip(x, extra="\n")
        with pytest.raises(AssertionError):
            round_trip(x, extra="a\n")

    def test_no_comments(self):
        round_trip(
            """
        - europe: 10
        - usa:
          - ohio: 2
          - california: 9
        """
        )

    def test_round_trip_ordering(self):
        round_trip(
            """
        a: 1
        b: 2
        c: 3
        b1: 2
        b2: 2
        d: 4
        e: 5
        f: 6
        """
        )

    def test_complex(self):
        round_trip(
            """
        - europe: 10 # top
        - usa:
          - ohio: 2
          - california: 9 # o
        """
        )

    def test_dropped(self):
        s = """\
        # comment
        scalar
        ...
        """
        round_trip(s, "scalar\n...\n")

    def test_main_mapping_begin_end(self):
        round_trip(
            """
        # C start a
        # C start b
        abc: 1
        ghi: 2
        klm: 3
        # C end a
        # C end b
        """
        )

    def test_reindent(self):
        x = """\
        a:
          b:     # comment 1
            c: 1 # comment 2
        """
        d = round_trip_load(x)
        y = round_trip_dump(d, indent=4)
        assert y == dedent(
            """\
        a:
            b:   # comment 1
                c: 1 # comment 2
        """
        )

    def test_main_mapping_begin_end_items_post(self):
        round_trip(
            """
        # C start a
        # C start b
        abc: 1      # abc comment
        ghi: 2
        klm: 3      # klm comment
        # C end a
        # C end b
        """
        )

    def test_main_sequence_begin_end(self):
        round_trip(
            """
        # C start a
        # C start b
        - abc
        - ghi
        - klm
        # C end a
        # C end b
        """
        )

    def test_main_sequence_begin_end_items_post(self):
        round_trip(
            """
        # C start a
        # C start b
        - abc      # abc comment
        - ghi
        - klm      # klm comment
        # C end a
        # C end b
        """
        )

    def test_main_mapping_begin_end_complex(self):
        round_trip(
            """
        # C start a
        # C start b
        abc: 1
        ghi: 2
        klm:
          3a: alpha
          3b: beta   # it is all greek to me
        # C end a
        # C end b
        """
        )

    def test_09(self):  # 2.9 from the examples in the spec
        s = """\
        hr: # 1998 hr ranking
          - Mark McGwire
          - Sammy Sosa
        rbi:
          # 1998 rbi ranking
          - Sammy Sosa
          - Ken Griffey
        """
        round_trip(s, indent=4, block_seq_indent=2)

    def test_09a(self):
        round_trip(
            """
        hr: # 1998 hr ranking
        - Mark McGwire
        - Sammy Sosa
        rbi:
          # 1998 rbi ranking
        - Sammy Sosa
        - Ken Griffey
        """
        )

    def test_simple_map_middle_comment(self):
        round_trip(
            """
        abc: 1
        # C 3a
        # C 3b
        ghi: 2
        """
        )

    def test_map_in_map_0(self):
        round_trip(
            """
        map1: # comment 1
          # comment 2
          map2:
            key1: val1
        """
        )

    def test_map_in_map_1(self):
        # comment is moved from value to key
        round_trip(
            """
        map1:
          # comment 1
          map2:
            key1: val1
        """
        )

    def test_application_arguments(self):
        # application configur
        round_trip(
            """
        args:
          username: anthon
          passwd: secret
          fullname: Anthon van der Neut
          tmux:
            session-name: test
          loop:
            wait: 10
        """
        )

    def test_substitute(self):
        x = """
        args:
          username: anthon          # name
          passwd: secret            # password
          fullname: Anthon van der Neut
          tmux:
            session-name: test
          loop:
            wait: 10
        """
        data = round_trip_load(x)
        data["args"]["passwd"] = "deleted password"
        # note the requirement to add spaces for alignment of comment
        x = x.replace(": secret          ", ": deleted password")
        assert round_trip_dump(data) == dedent(x)

    def test_set_comment(self):
        round_trip(
            """
        !!set
        # the beginning
        ? a
        # next one is B (lowercase)
        ? b  #  You see? Promised you.
        ? c
        # this is the end
        """
        )

    def test_omap_comment_roundtrip(self):
        round_trip(
            """
        !!omap
        - a: 1
        - b: 2  # two
        - c: 3  # three
        - d: 4
        """
        )

    def test_omap_comment_roundtrip_pre_comment(self):
        round_trip(
            """
        !!omap
        - a: 1
        - b: 2  # two
        - c: 3  # three
        # last one
        - d: 4
        """
        )

    def test_non_ascii(self):
        round_trip(
            """
        verbosity: 1                  # 0 is minimal output, -1 none
        base_url: http://gopher.net
        special_indices: [1, 5, 8]
        also_special:
        - a
        - 19
        - 32
        asia and europe: &asia_europe
          Turkey: Ankara
          Russia: Moscow
        countries:
          Asia:
            <<: *asia_europe
            Japan: Tokyo # 東京
          Europe:
            <<: *asia_europe
            Spain: Madrid
            Italy: Rome
        """
        )

    def test_dump_utf8(self):
        import srsly.ruamel_yaml  # NOQA

        x = dedent(
            """\
        ab:
        - x  # comment
        - y  # more comment
        """
        )
        data = round_trip_load(x)
        dumper = srsly.ruamel_yaml.RoundTripDumper
        for utf in [True, False]:
            y = srsly.ruamel_yaml.dump(
                data, default_flow_style=False, Dumper=dumper, allow_unicode=utf
            )
            assert y == x

    def test_dump_unicode_utf8(self):
        import srsly.ruamel_yaml  # NOQA

        x = dedent(
            u"""\
        ab:
        - x  # comment
        - y  # more comment
        """
        )
        data = round_trip_load(x)
        dumper = srsly.ruamel_yaml.RoundTripDumper
        for utf in [True, False]:
            y = srsly.ruamel_yaml.dump(
                data, default_flow_style=False, Dumper=dumper, allow_unicode=utf
            )
            assert y == x

    def test_mlget_00(self):
        x = """\
        a:
        - b:
          c: 42
        - d:
            f: 196
          e:
            g: 3.14
        """
        d = round_trip_load(x)
        assert d.mlget(["a", 1, "d", "f"], list_ok=True) == 196
        with pytest.raises(AssertionError):
            d.mlget(["a", 1, "d", "f"]) == 196


class TestInsertPopList:
    """list insertion is more complex than dict insertion, as you
    need to move the values to subsequent keys on insert"""

    @property
    def ins(self):
        return """\
        ab:
        - a      # a
        - b      # b
        - c
        - d      # d

        de:
        - 1
        - 2
        """

    def test_insert_0(self):
        d = round_trip_load(self.ins)
        d["ab"].insert(0, "xyz")
        y = round_trip_dump(d, indent=2)
        assert y == dedent(
            """\
        ab:
        - xyz
        - a      # a
        - b      # b
        - c
        - d      # d

        de:
        - 1
        - 2
        """
        )

    def test_insert_1(self):
        d = round_trip_load(self.ins)
        d["ab"].insert(4, "xyz")
        y = round_trip_dump(d, indent=2)
        assert y == dedent(
            """\
        ab:
        - a      # a
        - b      # b
        - c
        - d      # d

        - xyz
        de:
        - 1
        - 2
        """
        )

    def test_insert_2(self):
        d = round_trip_load(self.ins)
        d["ab"].insert(1, "xyz")
        y = round_trip_dump(d, indent=2)
        assert y == dedent(
            """\
        ab:
        - a      # a
        - xyz
        - b      # b
        - c
        - d      # d

        de:
        - 1
        - 2
        """
        )

    def test_pop_0(self):
        d = round_trip_load(self.ins)
        d["ab"].pop(0)
        y = round_trip_dump(d, indent=2)
        print(y)
        assert y == dedent(
            """\
        ab:
        - b      # b
        - c
        - d      # d

        de:
        - 1
        - 2
        """
        )

    def test_pop_1(self):
        d = round_trip_load(self.ins)
        d["ab"].pop(1)
        y = round_trip_dump(d, indent=2)
        print(y)
        assert y == dedent(
            """\
        ab:
        - a      # a
        - c
        - d      # d

        de:
        - 1
        - 2
        """
        )

    def test_pop_2(self):
        d = round_trip_load(self.ins)
        d["ab"].pop(2)
        y = round_trip_dump(d, indent=2)
        print(y)
        assert y == dedent(
            """\
        ab:
        - a      # a
        - b      # b
        - d      # d

        de:
        - 1
        - 2
        """
        )

    def test_pop_3(self):
        d = round_trip_load(self.ins)
        d["ab"].pop(3)
        y = round_trip_dump(d, indent=2)
        print(y)
        assert y == dedent(
            """\
        ab:
        - a      # a
        - b      # b
        - c
        de:
        - 1
        - 2
        """
        )


# inspired by demux' question on stackoverflow
# http://stackoverflow.com/a/36970608/1307905
class TestInsertInMapping:
    @property
    def ins(self):
        return """\
        first_name: Art
        occupation: Architect  # This is an occupation comment
        about: Art Vandelay is a fictional character that George invents...
        """

    def test_insert_at_pos_1(self):
        d = round_trip_load(self.ins)
        d.insert(1, "last name", "Vandelay", comment="new key")
        y = round_trip_dump(d)
        print(y)
        assert y == dedent(
            """\
        first_name: Art
        last name: Vandelay    # new key
        occupation: Architect  # This is an occupation comment
        about: Art Vandelay is a fictional character that George invents...
        """
        )

    def test_insert_at_pos_0(self):
        d = round_trip_load(self.ins)
        d.insert(0, "last name", "Vandelay", comment="new key")
        y = round_trip_dump(d)
        print(y)
        assert y == dedent(
            """\
        last name: Vandelay  # new key
        first_name: Art
        occupation: Architect  # This is an occupation comment
        about: Art Vandelay is a fictional character that George invents...
        """
        )

    def test_insert_at_pos_3(self):
        # much more simple if done with appending.
        d = round_trip_load(self.ins)
        d.insert(3, "last name", "Vandelay", comment="new key")
        y = round_trip_dump(d)
        print(y)
        assert y == dedent(
            """\
        first_name: Art
        occupation: Architect  # This is an occupation comment
        about: Art Vandelay is a fictional character that George invents...
        last name: Vandelay  # new key
        """
        )


class TestCommentedMapMerge:
    def test_in_operator(self):
        data = round_trip_load(
            """
        x: &base
          a: 1
          b: 2
          c: 3
        y:
          <<: *base
          k: 4
          l: 5
        """
        )
        assert data["x"]["a"] == 1
        assert "a" in data["x"]
        assert data["y"]["a"] == 1
        assert "a" in data["y"]

    def test_issue_60(self):
        data = round_trip_load(
            """
        x: &base
          a: 1
        y:
          <<: *base
        """
        )
        assert data["x"]["a"] == 1
        assert data["y"]["a"] == 1
        if sys.version_info >= (3, 12):
            assert str(data["y"]) == """ordereddict({'a': 1})"""
        else:
            assert str(data["y"]) == """ordereddict([('a', 1)])"""

    def test_issue_60_1(self):
        data = round_trip_load(
            """
        x: &base
          a: 1
        y:
          <<: *base
          b: 2
        """
        )
        assert data["x"]["a"] == 1
        assert data["y"]["a"] == 1
        if sys.version_info >= (3, 12):
            assert str(data["y"]) == """ordereddict({'b': 2, 'a': 1})"""
        else:
            assert str(data["y"]) == """ordereddict([('b', 2), ('a', 1)])"""


class TestEmptyLines:
    # prompted by issue 46 from Alex Harvey
    def test_issue_46(self):
        yaml_str = dedent(
            """\
        ---
        # Please add key/value pairs in alphabetical order

        aws_s3_bucket: 'mys3bucket'

        jenkins_ad_credentials:
          bind_name: 'CN=svc-AAA-BBB-T,OU=Example,DC=COM,DC=EXAMPLE,DC=Local'
          bind_pass: 'xxxxyyyy{'
        """
        )
        d = round_trip_load(yaml_str, preserve_quotes=True)
        y = round_trip_dump(d, explicit_start=True)
        assert yaml_str == y

    def test_multispace_map(self):
        round_trip(
            """
        a: 1x

        b: 2x


        c: 3x



        d: 4x

        """
        )

    @pytest.mark.xfail(strict=True)
    def test_multispace_map_initial(self):
        round_trip(
            """

        a: 1x

        b: 2x


        c: 3x



        d: 4x

        """
        )

    def test_embedded_map(self):
        round_trip(
            """
        - a: 1y
          b: 2y

          c: 3y
        """
        )

    def test_toplevel_seq(self):
        round_trip(
            """\
        - 1

        - 2

        - 3
        """
        )

    def test_embedded_seq(self):
        round_trip(
            """
        a:
          b:
          - 1

          - 2


          - 3
        """
        )

    def test_line_with_only_spaces(self):
        # issue 54
        yaml_str = "---\n\na: 'x'\n \nb: y\n"
        d = round_trip_load(yaml_str, preserve_quotes=True)
        y = round_trip_dump(d, explicit_start=True)
        stripped = ""
        for line in yaml_str.splitlines():
            stripped += line.rstrip() + "\n"
            print(line + "$")
        assert stripped == y

    def test_some_eol_spaces(self):
        # spaces after tokens and on empty lines
        yaml_str = '---  \n  \na: "x"  \n   \nb: y  \n'
        d = round_trip_load(yaml_str, preserve_quotes=True)
        y = round_trip_dump(d, explicit_start=True)
        stripped = ""
        for line in yaml_str.splitlines():
            stripped += line.rstrip() + "\n"
            print(line + "$")
        assert stripped == y

    def test_issue_54_not_ok(self):
        yaml_str = dedent(
            """\
        toplevel:

            # some comment
            sublevel: 300
        """
        )
        d = round_trip_load(yaml_str)
        print(d.ca)
        y = round_trip_dump(d, indent=4)
        print(y.replace("\n", "$\n"))
        assert yaml_str == y

    def test_issue_54_ok(self):
        yaml_str = dedent(
            """\
        toplevel:
            # some comment
            sublevel: 300
        """
        )
        d = round_trip_load(yaml_str)
        y = round_trip_dump(d, indent=4)
        assert yaml_str == y

    def test_issue_93(self):
        round_trip(
            """\
        a:
          b:
          - c1: cat  # a1
          # my comment on catfish
          - c2: catfish  # a2
        """
        )

    def test_issue_93_00(self):
        round_trip(
            """\
        a:
        - - c1: cat   # a1
          # my comment on catfish
          - c2: catfish  # a2
        """
        )

    def test_issue_93_01(self):
        round_trip(
            """\
        - - c1: cat   # a1
          # my comment on catfish
          - c2: catfish  # a2
        """
        )

    def test_issue_93_02(self):
        # never failed as there is no indent
        round_trip(
            """\
        - c1: cat
        # my comment on catfish
        - c2: catfish
        """
        )

    def test_issue_96(self):
        # inserted extra line on trailing spaces
        round_trip(
            """\
        a:
          b:
            c: c_val
            d:

          e:
            g: g_val
        """
        )


class TestUnicodeComments:
    @pytest.mark.skipif(sys.version_info < (2, 7), reason="wide unicode")
    def test_issue_55(self):  # reported by Haraguroicha Hsu
        round_trip(
            """\
        name: TEST
        description: test using
        author: Harguroicha
        sql:
          command: |-
            select name from testtbl where no = :no

          ci-test:
          - :no: 04043709 # 小花
          - :no: 05161690 # 茶
          - :no: 05293147 # 〇𤋥川
          - :no: 05338777 # 〇〇啓
          - :no: 05273867 # 〇
          - :no: 05205786 # 〇𤦌
        """
        )


class TestEmptyValueBeforeComments:
    def test_issue_25a(self):
        round_trip(
            """\
        - a: b
          c: d
          d:  # foo
          - e: f
        """
        )

    def test_issue_25a1(self):
        round_trip(
            """\
        - a: b
          c: d
          d:  # foo
            e: f
        """
        )

    def test_issue_25b(self):
        round_trip(
            """\
        var1: #empty
        var2: something #notempty
        """
        )

    def test_issue_25c(self):
        round_trip(
            """\
        params:
          a: 1 # comment a
          b:   # comment b
          c: 3 # comment c
        """
        )

    def test_issue_25c1(self):
        round_trip(
            """\
        params:
          a: 1 # comment a
          b:   # comment b
          # extra
          c: 3 # comment c
        """
        )

    def test_issue_25_00(self):
        round_trip(
            """\
        params:
          a: 1 # comment a
          b:   # comment b
        """
        )

    def test_issue_25_01(self):
        round_trip(
            """\
        a:        # comment 1
                  #  comment 2
        - b:      #   comment 3
            c: 1  #    comment 4
        """
        )

    def test_issue_25_02(self):
        round_trip(
            """\
        a:        # comment 1
                  #  comment 2
        - b: 2    #   comment 3
        """
        )

    def test_issue_25_03(self):
        s = """\
        a:        # comment 1
                  #  comment 2
          - b: 2  #   comment 3
        """
        round_trip(s, indent=4, block_seq_indent=2)

    def test_issue_25_04(self):
        round_trip(
            """\
        a:        # comment 1
                  #  comment 2
          b: 1    #   comment 3
        """
        )

    def test_flow_seq_within_seq(self):
        round_trip(
            """\
        # comment 1
        - a
        - b
        # comment 2
        - c
        - d
        # comment 3
        - [e]
        - f
        # comment 4
        - []
        """
        )


test_block_scalar_commented_line_template = """\
y: p
# Some comment

a: |
  x
{}b: y
"""


class TestBlockScalarWithComments:
    # issue 99 reported by Colm O'Connor
    def test_scalar_with_comments(self):
        import srsly.ruamel_yaml  # NOQA

        for x in [
            "",
            "\n",
            "\n# Another comment\n",
            "\n\n",
            "\n\n# abc\n#xyz\n",
            "\n\n# abc\n#xyz\n",
            "# abc\n\n#xyz\n",
            "\n\n  # abc\n  #xyz\n",
        ]:

            commented_line = test_block_scalar_commented_line_template.format(x)
            data = srsly.ruamel_yaml.round_trip_load(commented_line)

            assert srsly.ruamel_yaml.round_trip_dump(data) == commented_line
