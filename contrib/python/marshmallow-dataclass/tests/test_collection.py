import itertools
import sys
import unittest
from typing import Optional, Sequence, Set, FrozenSet, Mapping

import marshmallow

from marshmallow_dataclass import dataclass


class TestSequenceField(unittest.TestCase):
    def test_simple(self):
        @dataclass
        class IntSequence:
            value: Sequence[int]

        schema = IntSequence.Schema()

        # can load a sequence of ints
        data_in = {"value": list(range(5))}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntSequence(value=tuple(range(5))))
        self.assertEqual(schema.dump(loaded), data_in)

        # can load a empty sequence
        data_in = {"value": []}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntSequence(value=()))
        self.assertEqual(schema.dump(loaded), data_in)

        # any sequence can be dumped (any object that can be iterated in order)
        for sequence in ((1, 2, 3), {1: "a", 2: "b", 3: "c"}, [1, 2, 3]):
            self.assertEqual(
                schema.dump(IntSequence(value=sequence)), {"value": [1, 2, 3]}
            )
        # does not allow None
        with self.assertRaises(marshmallow.exceptions.ValidationError):
            schema.load({"value": None})

        # should only accept int
        for data_not_int in ({"value": [None]}, {"value": ["a"]}):
            with self.assertRaises(marshmallow.exceptions.ValidationError):
                schema.load(data_not_int)

    def test_sequence_no_arg(self):
        @dataclass
        class AnySequence:
            value: Sequence

        schema = AnySequence.Schema()

        # can load a sequence of mixed kind
        data_in = {"value": [1, "2", 3.0, True, None, {"this", "is a", "set"}]}
        loaded = schema.load(data_in)
        self.assertEqual(
            loaded,
            AnySequence(value=(1, "2", 3.0, True, None, {"this", "is a", "set"})),
        )
        self.assertEqual(schema.dump(loaded), data_in)

    def test_optional_sequence(self):
        @dataclass
        class IntOptionalSequence:
            value: Optional[Sequence[int]]

        schema = IntOptionalSequence.Schema()
        for data_in in ({"value": list(range(5))}, {"value": []}, {"value": None}):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_sequence_of_optional(self):
        @dataclass
        class OptionalIntSequence:
            value: Sequence[Optional[int]]

        schema = OptionalIntSequence.Schema()
        for data_in in ({"value": list(range(5))}, {"value": [1, None, 2, None]}):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_nested_sequence(self):
        @dataclass
        class SequenceIntSequence:
            value: Sequence[Sequence[int]]

        schema = SequenceIntSequence.Schema()
        for data_in in (
            {"value": []},
            {"value": [[], []]},
            {"value": [[1, 2, 3], [4, 5, 6]]},
        ):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_sequence_of_dataclass(self):
        @dataclass
        class Elm:
            value: int

        @dataclass
        class SequenceIntSequence:
            value: Sequence[Elm]

        schema = SequenceIntSequence.Schema()
        for data_in in (
            {"value": []},
            {"value": [{"value": 1}]},
        ):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_sequence_of_optional_dataclass(self):
        @dataclass
        class Elm:
            value: int

        @dataclass
        class SequenceIntSequence:
            value: Sequence[Optional[Elm]]

        schema = SequenceIntSequence.Schema()
        for data_in in (
            {"value": []},
            {"value": [None]},
            {"value": [{"value": 1}]},
        ):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)


class TestSetField(unittest.TestCase):
    def assertEqualAsSet(self, a, b):
        self.assertEqual(set(a), set(b))

    def test_simple(self):
        @dataclass
        class IntSet:
            value: Set[int]

        schema = IntSet.Schema()
        data_in = {"value": list(range(5))}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntSet(value=set(range(5))))
        self.assertEqualAsSet(schema.dump(loaded)["value"], data_in["value"])

    def test_set_no_arg(self):
        @dataclass
        class AnySet:
            value: Set

        schema = AnySet.Schema()

        # can load a set of mixed kind
        data_in = {"value": [1, "2", 3.0, (1, 2, 3)]}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, AnySet(value={1, "2", 3.0, (1, 2, 3)}))
        self.assertEqualAsSet(schema.dump(loaded)["value"], data_in["value"])

    def test_optional_set(self):
        @dataclass
        class IntOptionalSet:
            value: Optional[Set[int]]

        schema = IntOptionalSet.Schema()

        # None can be loaded and dumped
        data_in = {"value": None}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

        # and regular values too
        data_in = {"value": list(range(5))}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntOptionalSet(value=set(range(5))))
        self.assertEqualAsSet(schema.dump(loaded)["value"], data_in["value"])

    def test_set_of_optional(self):
        @dataclass
        class OptionalIntSet:
            value: Set[Optional[int]]

        schema = OptionalIntSet.Schema()
        for data_in in ({"value": set(range(5))}, {"value": {1, None, 2}}):
            self.assertEqual(
                set(schema.dump(schema.load(data_in))["value"]), data_in["value"]
            )

    def test_set_only_work_in_hashable_types(self):
        @dataclass
        class SetIntSet:
            value: Set[Set[int]]

        schema = SetIntSet.Schema()

        with self.assertRaises(TypeError) as err_info:
            schema.load({"value": {set()}})
        self.assertEqual(str(err_info.exception), "unhashable type: 'set'")

        @dataclass()
        class Elm:
            value: int

        @dataclass
        class SetOfDataclass:
            value: Set[Elm]

        schema = SetOfDataclass.Schema()

        with self.assertRaises(TypeError) as err_info:
            schema.load({"value": {{"value": {set()}}}})
        self.assertEqual(str(err_info.exception), "unhashable type: 'set'")

    def test_set_of_frozen_dataclass(self):
        @dataclass(frozen=True)
        class Elm:
            value: str

        @dataclass
        class SetIntSet:
            value: Set[Elm]

        schema = SetIntSet.Schema()
        for data_in in (
            {"value": []},
            {"value": [{"value": "john"}, {"value": "doe"}, {"value": "alex"}]},
        ):
            # All the items have to be the same, but the order is not guaranteed
            self.assertIn(
                schema.dump(schema.load(data_in))["value"],
                map(list, itertools.permutations(data_in["value"])),
            )

    def test_set_of_optional_dataclass(self):
        @dataclass(frozen=True)
        class Elm:
            value: int

        @dataclass
        class SetIntSet:
            value: Set[Optional[Elm]]

        schema = SetIntSet.Schema()
        for data_in in (
            {"value": []},
            {"value": [None]},
            {"value": [{"value": 1}]},
        ):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_frozen_set(self):
        @dataclass
        class IntFrozenSet:
            value: FrozenSet[int]

        schema = IntFrozenSet.Schema()
        data_in = {"value": list(range(5))}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntFrozenSet(value=frozenset(range(5))))
        self.assertEqual(schema.dump(loaded), data_in)

    def test_nested_frozenset(self):
        """frozen sets are hashable so we can nest them"""

        @dataclass
        class SetIntSet:
            value: FrozenSet[FrozenSet[int]]

        schema = SetIntSet.Schema()
        data_in = {
            "value": [
                [1, 2, 3],
                [3, 2, 1],
                [1, 3, 2],
                [2, 3, 1],
                [3, 1, 2],
                [2, 1, 3],
                [123],
            ]
        }
        loaded = schema.load(data_in)
        self.assertEqual(
            loaded,
            SetIntSet(value=frozenset([frozenset([1, 2, 3]), frozenset([123])])),
        )
        self.assertEqualAsSet(schema.dump(loaded), data_in)


class TestMappingField(unittest.TestCase):
    def test_simple(self):
        @dataclass
        class StrIntMapping:
            value: Mapping[str, int]

        schema = StrIntMapping.Schema()
        value = {key: value for key, value in zip("abcdefghi", range(5))}
        data_in = {"value": value}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, StrIntMapping(value=value))
        self.assertEqual(schema.dump(loaded), data_in)

    def test_mapping_no_arg(self):
        @dataclass
        class AnyMapping:
            value: Mapping

        schema = AnyMapping.Schema()

        # can load a sequence of mixed kind
        data_in = {
            "value": {
                1: "a number key a str value",
                "a str key a number value": 2,
                None: "this is still valid",
                "even this": None,
            }
        }
        loaded = schema.load(data_in)
        self.assertEqual(
            loaded,
            AnyMapping(
                value={
                    1: "a number key a str value",
                    "a str key a number value": 2,
                    None: "this is still valid",
                    "even this": None,
                }
            ),
        )
        self.assertEqual(schema.dump(loaded), data_in)

    def test_mapping_of_frozen_dataclass(self):
        @dataclass(frozen=True)
        class Elm:
            name: str

        @dataclass
        class DataclassMapping:
            value: Mapping[int, Elm]

        schema = DataclassMapping.Schema()
        for data_in in (
            {"value": {1: {"name": "John"}}},
            {"value": {1: {"name": "Doe"}}},
        ):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)


class TestBuiltinGenerics(unittest.TestCase):
    @unittest.skipIf(
        sys.version_info < (3, 9),
        "builtin generics are only available in python 3.9 upwards",
    )
    def test_builtin(self):
        annotations = [list[int], dict[str, int], set[str]]
        samples = [[1, 2, 3], {"foo": 1, "bar": 2}, {"f", "o", "b", "a", "r"}]

        for annotation, sample in zip(annotations, samples):
            with self.subTest(f"Testing {annotation}"):

                @dataclass
                class Dummy:
                    value: annotation

                schema = Dummy.Schema()

                # check if a round trip keeps the data untouched
                loaded = schema.load(schema.dump(Dummy(value=sample)))
                self.assertEqual(loaded, Dummy(value=sample))
