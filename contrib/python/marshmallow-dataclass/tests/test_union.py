from dataclasses import field
import sys
import unittest
from typing import List, Optional, Union, Dict

import marshmallow

from marshmallow_dataclass import dataclass, NewType


class TestClassSchema(unittest.TestCase):
    def test_simple_union(self):
        @dataclass
        class IntOrStr:
            value: Union[int, str]

        schema = IntOrStr.Schema()
        data_in = {"value": "hello"}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, IntOrStr(value="hello"))
        self.assertEqual(schema.dump(loaded), data_in)

        data_in = {"value": 42}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_list_union_builtin(self):
        @dataclass
        class Dclass2:
            value: List[Union[int, str]]

        schema = Dclass2.Schema()
        data_in = {"value": ["hello", 42]}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_list_union_object(self):
        @dataclass
        class Elm1:
            elm1: str

        @dataclass
        class Elm2:
            elm2: str

        @dataclass
        class Dclass:
            value: List[Union[Elm1, Elm2]]

        schema = Dclass.Schema()
        data_in = {"value": [{"elm1": "foo"}, {"elm2": "bar"}]}
        load = schema.load(data_in)
        self.assertEqual(load, Dclass(value=[Elm1(elm1="foo"), Elm2(elm2="bar")]))
        self.assertEqual(schema.dump(load), data_in)

    def test_union_list(self):
        @dataclass
        class Elm1:
            elm1: int

        @dataclass
        class Elm2:
            elm2: int

        @dataclass
        class TestDataClass:
            value: Union[List[Elm1], List[Elm2]]

        schema = TestDataClass.Schema()

        data_in = {"value": [{"elm1": 10}, {"elm1": 11}]}
        load = schema.load(data_in)
        self.assertEqual(load, TestDataClass(value=[Elm1(elm1=10), Elm1(elm1=11)]))
        self.assertEqual(schema.dump(load), data_in)

        data_in = {"value": [{"elm2": 10}, {"elm2": 11}]}
        load = schema.load(data_in)
        self.assertEqual(load, TestDataClass(value=[Elm2(elm2=10), Elm2(elm2=11)]))
        self.assertEqual(schema.dump(load), data_in)

        dictwrong_in = {"value": [{"elm1": 10}, {"elm2": 11}]}
        with self.assertRaises(marshmallow.exceptions.ValidationError):
            schema.load(dictwrong_in)

    def test_many_nested_union(self):
        @dataclass
        class Elm1:
            elm1: str

        @dataclass
        class Dclass:
            value: List[Union[List[Union[int, str, Elm1]], int]]

        schema = Dclass.Schema()
        data_in = {"value": [42, ["hello", 13, {"elm1": "foo"}]]}

        self.assertEqual(schema.dump(schema.load(data_in)), data_in)
        with self.assertRaises(marshmallow.exceptions.ValidationError):
            schema.load({"value": [42, ["hello", 13, {"elm2": "foo"}]]})

    def test_union_dict(self):
        @dataclass
        class Dclass:
            value: List[Union[Dict[int, Union[int, str]], Union[int, str]]]

        schema = Dclass.Schema()
        data_in = {"value": [42, {12: 13, 13: "hello"}, "foo"]}

        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

        with self.assertRaises(marshmallow.exceptions.ValidationError):
            schema.load({"value": [(42,), {12: 13, 13: "hello"}, "foo"]})

    def test_union_list_dict(self):
        @dataclass
        class Elm:
            elm: int

        @dataclass
        class Dclass:
            value: Union[List[int], Dict[str, Elm]]

        schema = Dclass.Schema()

        data_in = {"value": {"a": {"elm": 10}, "b": {"elm": 10}}}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

        data_in = {"value": [1, 2, 3, 4]}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_union_noschema(self):
        @dataclass
        class Dclass:
            value: Union[int, str]

        schema = Dclass.Schema()
        data_in = {"value": [1.4, 4.2]}
        with self.assertRaises(marshmallow.exceptions.ValidationError):
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_union_optional_object(self):
        @dataclass
        class Elm1:
            elm1: str

        @dataclass
        class Elm2:
            elm2: str

        @dataclass
        class Dclass:
            value: Optional[Union[Elm1, Elm2]]

        schema = Dclass.Schema()

        for data_in in [{"value": {"elm1": "hello"}}, {"value": {"elm2": "hello"}}]:
            self.assertEqual(schema.dump(schema.load(data_in)), data_in)

        for data_in in [{"value": None}, {}]:
            self.assertEqual(schema.dump(schema.load(data_in)), {"value": None})

    def test_required_optional_simple_union(self):
        @dataclass
        class Dclass:
            value: Optional[Union[int, str]] = field(metadata={"required": True})

        schema = Dclass.Schema()

        for value in None, 42, "strvar":
            self.assertEqual(schema.dump(Dclass(value=value)), {"value": value})
            self.assertEqual(schema.load({"value": value}), Dclass(value=value))

    def test_union_with_default(self):
        @dataclass
        class IntOrStrWithDefault:
            value: Union[int, str] = 42

        schema = IntOrStrWithDefault.Schema()
        self.assertEqual(schema.load({}), IntOrStrWithDefault(value=42))
        for value in 13, "strval":
            self.assertEqual(
                schema.load({"value": value}), IntOrStrWithDefault(value=value)
            )
        with self.assertRaises(marshmallow.exceptions.ValidationError):
            schema.load({"value": None})

    @unittest.skipIf(sys.version_info < (3, 10), "No PEP604 support in py<310")
    def test_pep604_union(self):
        @dataclass
        class PEP604IntOrStr:
            value: int | str

        schema = PEP604IntOrStr.Schema()
        data_in = {"value": "hello"}
        loaded = schema.load(data_in)
        self.assertEqual(loaded, PEP604IntOrStr(value="hello"))
        self.assertEqual(schema.dump(loaded), data_in)

        data_in = {"value": 42}
        self.assertEqual(schema.dump(schema.load(data_in)), data_in)

    def test_union_with_generics(self):
        IntList = NewType("IntList", List[int])

        @dataclass
        class Dclass:
            value: Union[IntList, List[str]]

        schema = Dclass.Schema()

        self.assertEqual(
            schema.load({"value": [1, 2, 3]}), Dclass(value=IntList([1, 2, 3]))
        )
        self.assertEqual(
            schema.dump(Dclass(value=IntList([1, 2, 3]))), {"value": [1, 2, 3]}
        )
