# coding: utf-8

import pytest  # NOQA

from .roundtrip import round_trip, round_trip_load, YAML


def register_xxx(**kw):
    import srsly.ruamel_yaml as yaml

    class XXX(yaml.comments.CommentedMap):
        @staticmethod
        def yaml_dump(dumper, data):
            return dumper.represent_mapping(u"!xxx", data)

        @classmethod
        def yaml_load(cls, constructor, node):
            data = cls()
            yield data
            constructor.construct_mapping(node, data)

    yaml.add_constructor(u"!xxx", XXX.yaml_load, constructor=yaml.RoundTripConstructor)
    yaml.add_representer(XXX, XXX.yaml_dump, representer=yaml.RoundTripRepresenter)


class TestIndentFailures:
    def test_tag(self):
        round_trip(
            """\
        !!python/object:__main__.Developer
        name: Anthon
        location: Germany
        language: python
        """
        )

    def test_full_tag(self):
        round_trip(
            """\
        !!tag:yaml.org,2002:python/object:__main__.Developer
        name: Anthon
        location: Germany
        language: python
        """
        )

    def test_standard_tag(self):
        round_trip(
            """\
        !!tag:yaml.org,2002:python/object:map
        name: Anthon
        location: Germany
        language: python
        """
        )

    def test_Y1(self):
        round_trip(
            """\
        !yyy
        name: Anthon
        location: Germany
        language: python
        """
        )

    def test_Y2(self):
        round_trip(
            """\
        !!yyy
        name: Anthon
        location: Germany
        language: python
        """
        )


class TestRoundTripCustom:
    def test_X1(self):
        register_xxx()
        round_trip(
            """\
        !xxx
        name: Anthon
        location: Germany
        language: python
        """
        )

    @pytest.mark.xfail(strict=True)
    def test_X_pre_tag_comment(self):
        register_xxx()
        round_trip(
            """\
        -
          # hello
          !xxx
          name: Anthon
          location: Germany
          language: python
        """
        )

    @pytest.mark.xfail(strict=True)
    def test_X_post_tag_comment(self):
        register_xxx()
        round_trip(
            """\
        - !xxx
          # hello
          name: Anthon
          location: Germany
          language: python
        """
        )

    def test_scalar_00(self):
        # https://stackoverflow.com/a/45967047/1307905
        round_trip(
            """\
        Outputs:
          Vpc:
            Value: !Ref: vpc    # first tag
            Export:
              Name: !Sub "${AWS::StackName}-Vpc"  # second tag
        """
        )


class TestIssue201:
    def test_encoded_unicode_tag(self):
        round_trip_load(
            """
        s: !!python/%75nicode 'abc'
        """
        )


class TestImplicitTaggedNodes:
    def test_scalar(self):
        round_trip(
            """\
        - !Scalar abcdefg
        """
        )

    def test_mapping(self):
        round_trip(
            """\
        - !Mapping {a: 1, b: 2}
        """
        )

    def test_sequence(self):
        yaml = YAML()
        yaml.brace_single_entry_mapping_in_flow_sequence = True
        yaml.mapping_value_align = True
        yaml.round_trip(
            """
        - !Sequence [a, {b: 1}, {c: {d: 3}}]
        """
        )

    def test_sequence2(self):
        yaml = YAML()
        yaml.mapping_value_align = True
        yaml.round_trip(
            """
        - !Sequence [a, b: 1, c: {d: 3}]
        """
        )
