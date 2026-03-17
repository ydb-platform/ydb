"""Tests for josepy.interfaces."""

import sys
import unittest
from typing import Any, Dict, List

import pytest


class JSONDeSerializableTest(unittest.TestCase):
    def setUp(self) -> None:
        from josepy.interfaces import JSONDeSerializable

        class Basic(JSONDeSerializable):
            def __init__(self, v: Any) -> None:
                self.v = v

            def to_partial_json(self) -> Any:
                return self.v

            @classmethod
            def from_json(cls, jobj: Any) -> "Basic":
                return cls(jobj)

        class Sequence(JSONDeSerializable):
            def __init__(self, x: Basic, y: Basic) -> None:
                self.x = x
                self.y = y

            def to_partial_json(self) -> List[Basic]:
                return [self.x, self.y]

            @classmethod
            def from_json(cls, jobj: List[Any]) -> "Sequence":
                return cls(Basic.from_json(jobj[0]), Basic.from_json(jobj[1]))

        class Mapping(JSONDeSerializable):
            def __init__(self, x: Any, y: Any) -> None:
                self.x = x
                self.y = y

            def to_partial_json(self) -> Dict[Basic, Basic]:
                return {self.x: self.y}

            @classmethod
            def from_json(cls, jobj: Any) -> "Mapping":
                return cls("dummy", "values")  # pragma: no cover

        self.basic1 = Basic("foo1")
        self.basic2 = Basic("foo2")
        self.seq = Sequence(self.basic1, self.basic2)
        self.mapping = Mapping(self.basic1, self.basic2)
        self.nested = Basic([[self.basic1]])
        self.tuple = Basic(("foo",))

        self.Basic = Basic
        self.Sequence = Sequence
        self.Mapping = Mapping

    def test_to_json_sequence(self) -> None:
        assert self.seq.to_json() == ["foo1", "foo2"]

    def test_to_json_mapping(self) -> None:
        assert self.mapping.to_json() == {"foo1": "foo2"}

    def test_to_json_other(self) -> None:
        mock_value = object()
        assert self.Basic(mock_value).to_json() is mock_value

    def test_to_json_nested(self) -> None:
        assert self.nested.to_json() == [["foo1"]]

    def test_to_json(self) -> None:
        assert self.tuple.to_json() == (("foo",))

    def test_from_json_not_implemented(self) -> None:
        from josepy.interfaces import JSONDeSerializable

        with pytest.raises(TypeError):
            JSONDeSerializable.from_json("xxx")

    def test_json_loads(self) -> None:
        seq = self.Sequence.json_loads('["foo1", "foo2"]')
        assert isinstance(seq, self.Sequence)
        assert isinstance(seq.x, self.Basic)
        assert isinstance(seq.y, self.Basic)
        assert seq.x.v == "foo1"
        assert seq.y.v == "foo2"

    def test_json_dumps(self) -> None:
        assert '["foo1", "foo2"]' == self.seq.json_dumps()

    def test_json_dumps_pretty(self) -> None:
        assert self.seq.json_dumps_pretty() == '[\n    "foo1",\n    "foo2"\n]'

    def test_json_dump_default(self) -> None:
        from josepy.interfaces import JSONDeSerializable

        assert "foo1" == JSONDeSerializable.json_dump_default(self.basic1)

        jobj = JSONDeSerializable.json_dump_default(self.seq)
        assert len(jobj) == 2
        assert jobj[0] is self.basic1
        assert jobj[1] is self.basic2

    def test_json_dump_default_type_error(self) -> None:
        from josepy.interfaces import JSONDeSerializable

        with pytest.raises(TypeError):
            # We're purposefully testing with the incorrect type here.
            JSONDeSerializable.json_dump_default(object())  # type: ignore


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
