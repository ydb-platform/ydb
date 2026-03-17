"""
Tests `tap.tapify`. Currently requires Pydantic v2.
"""

import contextlib
from dataclasses import dataclass
import io
import sys
from typing import List, Optional, Tuple, Any
import unittest
from unittest import TestCase

from tap import tapify


try:
    import pydantic
except ModuleNotFoundError:
    _IS_PYDANTIC_V1 = None
else:
    _IS_PYDANTIC_V1 = pydantic.VERSION.startswith("1.")


# Suppress prints from SystemExit
class DevNull:
    def write(self, msg):
        pass


sys.stderr = DevNull()


class Person:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return f"Person({self.name})"


class Problems:
    def __init__(self, problem_1: str, problem_2):
        self.problem_1 = problem_1
        self.problem_2 = problem_2

    def __repr__(self) -> str:
        return f"Problems({self.problem_1}, {self.problem_2})"


class TapifyTests(TestCase):
    def test_tapify_empty(self):
        def pie() -> float:
            return 3.14

        class Pie:
            def __eq__(self, other: float) -> bool:
                return other == pie()

        @dataclass
        class PieDataclass:
            def __eq__(self, other: float) -> bool:
                return other == pie()

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class PieDataclassPydantic:
                def __eq__(self, other: float) -> bool:
                    return other == pie()

            class PieModel(pydantic.BaseModel):
                def __eq__(self, other: float) -> bool:
                    return other == pie()

            pydantic_data_models = [PieDataclassPydantic, PieModel]
        else:
            pydantic_data_models = []

        for class_or_function in [pie, Pie, PieDataclass] + pydantic_data_models:
            self.assertEqual(tapify(class_or_function, command_line_args=[]), 3.14)

    def test_tapify_simple_types(self):
        def concat(a: int, simple: str, test: float, of: float, types: bool) -> str:
            return f"{a} {simple} {test} {of} {types}"

        def concat_with_positionals(a: int, simple: str, test: float, of: float, types: bool, /) -> str:
            return f"{a} {simple} {test} {of} {types}"

        class Concat:
            def __init__(self, a: int, simple: str, test: float, of: float, types: bool):
                self.kwargs = {"a": a, "simple": simple, "test": test, "of": of, "types": types}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, a: int, simple: str, test: float, of: float, types: bool, /):
                self.kwargs = {"a": a, "simple": simple, "test": test, "of": of, "types": types}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            a: int
            simple: str
            test: float
            of: float
            types: bool

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.simple, self.test, self.of, self.types)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                a: int
                simple: str
                test: float
                of: float
                types: bool

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.simple, self.test, self.of, self.types)

            class ConcatModel(pydantic.BaseModel):
                a: int
                simple: str
                test: float
                of: float
                types: bool

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.simple, self.test, self.of, self.types)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=["--a", "1", "--simple", "simple", "--test", "3.14", "--of", "2.718", "--types"],
            )

            self.assertEqual(output, "1 simple 3.14 2.718 True")

    def test_tapify_simple_types_defaults(self):
        def concat(a: int, simple: str, test: float, of: float = -0.3, types: bool = False, wow: str = "abc") -> str:
            return f"{a} {simple} {test} {of} {types} {wow}"

        def concat_with_positionals(
            a: int, simple: str, test: float, /, of: float = -0.3, types: bool = False, wow: str = "abc"
        ) -> str:
            return f"{a} {simple} {test} {of} {types} {wow}"

        class Concat:
            def __init__(
                self, a: int, simple: str, test: float, of: float = -0.3, types: bool = False, wow: str = "abc"
            ):
                self.kwargs = {"a": a, "simple": simple, "test": test, "of": of, "types": types, "wow": wow}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(
                self, a: int, simple: str, test: float, /, of: float = -0.3, types: bool = False, wow: str = "abc"
            ):
                self.kwargs = {"a": a, "simple": simple, "test": test, "of": of, "types": types, "wow": wow}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            a: int
            simple: str
            test: float
            of: float = -0.3
            types: bool = False
            wow: str = "abc"

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.simple, self.test, self.of, self.types, self.wow)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                a: int
                simple: str
                test: float
                of: float = -0.3
                types: bool = False
                wow: str = "abc"

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.simple, self.test, self.of, self.types, self.wow)

            class ConcatModel(pydantic.BaseModel):
                a: int
                simple: str
                test: float
                of: float = -0.3
                types: bool = False
                wow: str = "abc"

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.simple, self.test, self.of, self.types, self.wow)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=["--a", "1", "--simple", "simple", "--test", "3.14", "--types", "--wow", "wee"],
            )

            self.assertEqual(output, "1 simple 3.14 -0.3 True wee")

    def test_tapify_complex_types(self):
        def concat(complexity: list[str], requires: tuple[int, int], intelligence: Person) -> str:
            return f'{" ".join(complexity)} {requires[0]} {requires[1]} {intelligence}'

        def concat_with_positionals(complexity: list[str], /, requires: tuple[int, int], intelligence: Person) -> str:
            return f'{" ".join(complexity)} {requires[0]} {requires[1]} {intelligence}'

        class Concat:
            def __init__(self, complexity: list[str], requires: tuple[int, int], intelligence: Person):
                self.kwargs = {"complexity": complexity, "requires": requires, "intelligence": intelligence}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, complexity: list[str], /, requires: tuple[int, int], intelligence: Person):
                self.kwargs = {"complexity": complexity, "requires": requires, "intelligence": intelligence}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            complexity: List[str]
            requires: Tuple[int, int]
            intelligence: Person

            def __eq__(self, other: str) -> bool:
                return other == concat(self.complexity, self.requires, self.intelligence)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass(config=dict(arbitrary_types_allowed=True))  # for Person
            class ConcatDataclassPydantic:
                complexity: List[str]
                requires: Tuple[int, int]
                intelligence: Person

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence)

            class ConcatModel(pydantic.BaseModel):
                if _IS_PYDANTIC_V1:

                    class Config:
                        arbitrary_types_allowed = True  # for Person

                else:
                    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)  # for Person

                complexity: List[str]
                requires: Tuple[int, int]
                intelligence: Person

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=[
                    "--complexity",
                    "complex",
                    "things",
                    "require",
                    "--requires",
                    "1",
                    "0",
                    "--intelligence",
                    "jesse",
                ],
            )

            self.assertEqual(output, "complex things require 1 0 Person(jesse)")

    def test_tapify_complex_types_parameterized_standard(self):
        def concat(complexity: list[int], requires: tuple[int, int], intelligence: Person) -> str:
            return f'{" ".join(map(str, complexity))} {requires[0]} {requires[1]} {intelligence}'

        def concat_with_positionals(complexity: list[int], requires: tuple[int, int], /, intelligence: Person) -> str:
            return f'{" ".join(map(str, complexity))} {requires[0]} {requires[1]} {intelligence}'

        class Concat:
            def __init__(self, complexity: list[int], requires: tuple[int, int], intelligence: Person):
                self.kwargs = {"complexity": complexity, "requires": requires, "intelligence": intelligence}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, complexity: list[int], requires: tuple[int, int], /, intelligence: Person):
                self.kwargs = {"complexity": complexity, "requires": requires, "intelligence": intelligence}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            complexity: list[int]
            requires: tuple[int, int]
            intelligence: Person

            def __eq__(self, other: str) -> bool:
                return other == concat(self.complexity, self.requires, self.intelligence)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass(config=dict(arbitrary_types_allowed=True))  # for Person
            class ConcatDataclassPydantic:
                complexity: list[int]
                requires: tuple[int, int]
                intelligence: Person

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence)

            class ConcatModel(pydantic.BaseModel):
                if _IS_PYDANTIC_V1:

                    class Config:
                        arbitrary_types_allowed = True  # for Person

                else:
                    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)  # for Person

                complexity: list[int]
                requires: tuple[int, int]
                intelligence: Person

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=[
                    "--complexity",
                    "1",
                    "2",
                    "3",
                    "--requires",
                    "1",
                    "0",
                    "--intelligence",
                    "jesse",
                ],
            )

            self.assertEqual(output, "1 2 3 1 0 Person(jesse)")

    def test_tapify_complex_types_defaults(self):
        def concat(
            complexity: List[str],
            requires: Tuple[int, int] = (2, 5),
            intelligence: Person = Person("kyle"),
            maybe: Optional[str] = None,
            possibly: Optional[str] = None,
        ) -> str:
            return f'{" ".join(complexity)} {requires[0]} {requires[1]} {intelligence} {maybe} {possibly}'

        def concat_with_positionals(
            complexity: List[str],
            requires: Tuple[int, int] = (2, 5),
            intelligence: Person = Person("kyle"),
            maybe: Optional[str] = None,
            possibly: Optional[str] = None,
            /,
        ) -> str:
            return f'{" ".join(complexity)} {requires[0]} {requires[1]} {intelligence} {maybe} {possibly}'

        class Concat:
            def __init__(
                self,
                complexity: List[str],
                requires: Tuple[int, int] = (2, 5),
                intelligence: Person = Person("kyle"),
                maybe: Optional[str] = None,
                possibly: Optional[str] = None,
            ):
                self.kwargs = {
                    "complexity": complexity,
                    "requires": requires,
                    "intelligence": intelligence,
                    "maybe": maybe,
                    "possibly": possibly,
                }

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(
                self,
                complexity: List[str],
                requires: Tuple[int, int] = (2, 5),
                intelligence: Person = Person("kyle"),
                maybe: Optional[str] = None,
                possibly: Optional[str] = None,
                /,
            ):
                self.kwargs = {
                    "complexity": complexity,
                    "requires": requires,
                    "intelligence": intelligence,
                    "maybe": maybe,
                    "possibly": possibly,
                }

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            complexity: List[str]
            requires: Tuple[int, int] = (2, 5)
            intelligence: Person = Person("kyle")
            maybe: Optional[str] = None
            possibly: Optional[str] = None

            def __eq__(self, other: str) -> bool:
                return other == concat(self.complexity, self.requires, self.intelligence, self.maybe, self.possibly)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass(config=dict(arbitrary_types_allowed=True))  # for Person
            class ConcatDataclassPydantic:
                complexity: List[str]
                requires: Tuple[int, int] = (2, 5)
                intelligence: Person = Person("kyle")
                maybe: Optional[str] = None
                possibly: Optional[str] = None

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence, self.maybe, self.possibly)

            class ConcatModel(pydantic.BaseModel):
                if _IS_PYDANTIC_V1:

                    class Config:
                        arbitrary_types_allowed = True  # for Person

                else:
                    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)  # for Person

                complexity: List[str]
                requires: Tuple[int, int] = (2, 5)
                intelligence: Person = Person("kyle")
                maybe: Optional[str] = None
                possibly: Optional[str] = None

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.complexity, self.requires, self.intelligence, self.maybe, self.possibly)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=[
                    "--complexity",
                    "complex",
                    "things",
                    "require",
                    "--requires",
                    "-3",
                    "12",
                    "--possibly",
                    "huh?",
                ],
            )

            self.assertEqual(output, "complex things require -3 12 Person(kyle) None huh?")

    def test_tapify_too_few_args(self):
        def concat(so: int, many: float, args: str) -> str:
            return f"{so} {many} {args}"

        def concat_with_positionals(so: int, many: float, /, args: str) -> str:
            return f"{so} {many} {args}"

        class Concat:
            def __init__(self, so: int, many: float, args: str):
                self.kwargs = {"so": so, "many": many, "args": args}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, so: int, many: float, /, args: str):
                self.kwargs = {"so": so, "many": many, "args": args}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            so: int
            many: float
            args: str

            def __eq__(self, other: str) -> bool:
                return other == concat(self.so, self.many, self.args)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                so: int
                many: float
                args: str

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.many, self.args)

            class ConcatModel(pydantic.BaseModel):
                so: int
                many: float
                args: str

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.many, self.args)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            with self.assertRaises(SystemExit):
                tapify(class_or_function, command_line_args=["--so", "23", "--many", "9.3"])

    def test_tapify_too_many_args(self):
        def concat(so: int, few: float) -> str:
            return f"{so} {few}"

        def concat_with_positionals(so: int, few: float, /) -> str:
            return f"{so} {few}"

        class Concat:
            def __init__(self, so: int, few: float):
                self.kwargs = {"so": so, "few": few}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, so: int, few: float):
                self.kwargs = {"so": so, "few": few}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            so: int
            few: float

            def __eq__(self, other: str) -> bool:
                return other == concat(self.so, self.few)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                so: int
                few: float

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.few)

            class ConcatModel(pydantic.BaseModel):
                so: int
                few: float

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.few)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            with self.assertRaises(SystemExit):
                tapify(class_or_function, command_line_args=["--so", "23", "--few", "9.3", "--args", "wow"])

    def test_tapify_too_many_args_known_only(self):
        def concat(so: int, few: float) -> str:
            return f"{so} {few}"

        def concat_with_positionals(so: int, few: float, /) -> str:
            return f"{so} {few}"

        class Concat:
            def __init__(self, so: int, few: float):
                self.kwargs = {"so": so, "few": few}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, so: int, few: float, /):
                self.kwargs = {"so": so, "few": few}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            so: int
            few: float

            def __eq__(self, other: str) -> bool:
                return other == concat(self.so, self.few)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                so: int
                few: float

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.few)

            class ConcatModel(pydantic.BaseModel):
                so: int
                few: float

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.so, self.few)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function, command_line_args=["--so", "23", "--few", "9.3", "--args", "wow"], known_only=True
            )

            self.assertEqual(output, "23 9.3")

    def test_tapify_kwargs(self):
        def concat(i: int, like: float, k: int, w: str = "w", args: str = "argy", always: bool = False) -> str:
            return f"{i} {like} {k} {w} {args} {always}"

        def concat_with_positionals(
            i: int, like: float, k: int, w: str = "w", /, args: str = "argy", *, always: bool = False
        ) -> str:
            return f"{i} {like} {k} {w} {args} {always}"

        class Concat:
            def __init__(self, i: int, like: float, k: int, w: str = "w", args: str = "argy", always: bool = False):
                self.kwargs = {"i": i, "like": like, "k": k, "w": w, "args": args, "always": always}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(
                self, i: int, like: float, k: int, w: str = "w", /, args: str = "argy", *, always: bool = False
            ):
                self.kwargs = {"i": i, "like": like, "k": k, "w": w, "args": args, "always": always}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            i: int
            like: float
            k: int
            w: str = "w"
            args: str = "argy"
            always: bool = False

            def __eq__(self, other: str) -> bool:
                return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                i: int
                like: float
                k: int
                w: str = "w"
                args: str = "argy"
                always: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

            class ConcatModel(pydantic.BaseModel):
                i: int
                like: float
                k: int
                w: str = "w"
                args: str = "argy"
                always: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=[
                    "--i",
                    "23",
                    "--args",
                    "wow",
                    "--like",
                    "3.03",
                ],
                known_only=True,
                w="hello",
                k=5,
                like=3.4,
                extra="arg",
            )

            self.assertEqual(output, "23 3.03 5 hello wow False")

    def test_tapify_kwargs_extra(self):
        def concat(i: int, like: float, k: int, w: str = "w", args: str = "argy", always: bool = False) -> str:
            return f"{i} {like} {k} {w} {args} {always}"

        def concat_with_positionals(
            i: int, like: float, k: int, /, *, w: str = "w", args: str = "argy", always: bool = False
        ) -> str:
            return f"{i} {like} {k} {w} {args} {always}"

        class Concat:
            def __init__(self, i: int, like: float, k: int, w: str = "w", args: str = "argy", always: bool = False):
                self.kwargs = {"i": i, "like": like, "k": k, "w": w, "args": args, "always": always}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(
                self, i: int, like: float, k: int, /, *, w: str = "w", args: str = "argy", always: bool = False
            ):
                self.kwargs = {"i": i, "like": like, "k": k, "w": w, "args": args, "always": always}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            i: int
            like: float
            k: int
            w: str = "w"
            args: str = "argy"
            always: bool = False

            def __eq__(self, other: str) -> bool:
                return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                i: int
                like: float
                k: int
                w: str = "w"
                args: str = "argy"
                always: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

            class ConcatModel(pydantic.BaseModel):
                i: int
                like: float
                k: int
                w: str = "w"
                args: str = "argy"
                always: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.i, self.like, self.k, self.w, self.args, self.always)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            with self.assertRaises(ValueError):
                tapify(
                    class_or_function,
                    command_line_args=[
                        "--i",
                        "23",
                        "--args",
                        "wow",
                        "--like",
                        "3.03",
                    ],
                    w="hello",
                    k=5,
                    like=3.4,
                    mis="direction",
                )

    def test_tapify_unsupported_type(self):
        def concat(problems: Problems) -> str:
            return f"{problems}"

        def concat_with_positionals(problems: Problems, /) -> str:
            return f"{problems}"

        class Concat:
            def __init__(self, problems: Problems):
                self.problems = problems

            def __eq__(self, other: str) -> bool:
                return other == concat(self.problems)

        class ConcatWithPositionals:
            def __init__(self, problems: Problems, /):
                self.problems = problems

            def __eq__(self, other: str) -> bool:
                return other == concat(self.problems)

        @dataclass
        class ConcatDataclass:
            problems: Problems

            def __eq__(self, other: str) -> bool:
                return other == concat(self.problems)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass(config=dict(arbitrary_types_allowed=True))
            class ConcatDataclassPydantic:
                problems: Problems

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.problems)

            class ConcatModel(pydantic.BaseModel):
                if _IS_PYDANTIC_V1:

                    class Config:
                        arbitrary_types_allowed = True  # for Problems

                else:
                    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)  # for Problems

                problems: Problems

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.problems)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(class_or_function, command_line_args=[], problems=Problems("oh", "no!"))

            self.assertEqual(output, "Problems(oh, no!)")

            with self.assertRaises(SystemExit):
                tapify(class_or_function, command_line_args=["--problems", "1", "2"])

    def test_tapify_untyped(self):
        def concat(
            untyped_1, typed_1: int, untyped_2=5, typed_2: str = "now", untyped_3="hi", typed_3: bool = False
        ) -> str:
            return f"{untyped_1} {typed_1} {untyped_2} {typed_2} {untyped_3} {typed_3}"

        def concat_with_positionals(
            untyped_1, typed_1: int, untyped_2=5, typed_2: str = "now", untyped_3="hi", /, typed_3: bool = False
        ) -> str:
            return f"{untyped_1} {typed_1} {untyped_2} {typed_2} {untyped_3} {typed_3}"

        class Concat:
            def __init__(
                self, untyped_1, typed_1: int, untyped_2=5, typed_2: str = "now", untyped_3="hi", typed_3: bool = False
            ):
                self.kwargs = {
                    "untyped_1": untyped_1,
                    "typed_1": typed_1,
                    "untyped_2": untyped_2,
                    "typed_2": typed_2,
                    "untyped_3": untyped_3,
                    "typed_3": typed_3,
                }

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(
                self,
                untyped_1,
                typed_1: int,
                untyped_2=5,
                typed_2: str = "now",
                untyped_3="hi",
                /,
                typed_3: bool = False,
            ):
                self.kwargs = {
                    "untyped_1": untyped_1,
                    "typed_1": typed_1,
                    "untyped_2": untyped_2,
                    "typed_2": typed_2,
                    "untyped_3": untyped_3,
                    "typed_3": typed_3,
                }

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            untyped_1: Any
            typed_1: int
            untyped_2: Any = 5
            typed_2: str = "now"
            untyped_3: Any = "hi"
            typed_3: bool = False

            def __eq__(self, other: str) -> bool:
                return other == concat(
                    self.untyped_1, self.typed_1, self.untyped_2, self.typed_2, self.untyped_3, self.typed_3
                )

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                untyped_1: Any
                typed_1: int
                untyped_2: Any = 5
                typed_2: str = "now"
                untyped_3: Any = "hi"
                typed_3: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(
                        self.untyped_1, self.typed_1, self.untyped_2, self.typed_2, self.untyped_3, self.typed_3
                    )

            class ConcatModel(pydantic.BaseModel):
                untyped_1: Any
                typed_1: int
                untyped_2: Any = 5
                typed_2: str = "now"
                untyped_3: Any = "hi"
                typed_3: bool = False

                def __eq__(self, other: str) -> bool:
                    return other == concat(
                        self.untyped_1, self.typed_1, self.untyped_2, self.typed_2, self.untyped_3, self.typed_3
                    )

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output = tapify(
                class_or_function,
                command_line_args=[
                    "--untyped_1",
                    "why not type?",
                    "--typed_1",
                    "1",
                    "--typed_2",
                    "typing is great!",
                    "--untyped_3",
                    "bye",
                ],
            )

            self.assertEqual(output, "why not type? 1 5 typing is great! bye False")

    def test_double_tapify(self):
        def concat(a: int, b: int, c: int) -> str:
            """Concatenate three numbers."""
            return f"{a} {b} {c}"

        def concat_with_positionals(a: int, b: int, c: int, /) -> str:
            """Concatenate three numbers."""
            return f"{a} {b} {c}"

        class Concat:
            """Concatenate three numbers."""

            def __init__(self, a: int, b: int, c: int):
                """Concatenate three numbers."""
                self.kwargs = {"a": a, "b": b, "c": c}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            """Concatenate three numbers."""

            def __init__(self, a: int, b: int, c: int, /):
                """Concatenate three numbers."""
                self.kwargs = {"a": a, "b": b, "c": c}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            """Concatenate three numbers."""

            a: int
            b: int
            c: int

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.b, self.c)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                """Concatenate three numbers."""

                a: int
                b: int
                c: int

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.b, self.c)

            class ConcatModel(pydantic.BaseModel):
                """Concatenate three numbers."""

                a: int
                b: int
                c: int

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.b, self.c)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []
        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            output_1 = tapify(class_or_function, command_line_args=["--a", "1", "--b", "2", "--c", "3"])
            output_2 = tapify(class_or_function, command_line_args=["--a", "4", "--b", "5", "--c", "6"])

            self.assertEqual(output_1, "1 2 3")
            self.assertEqual(output_2, "4 5 6")

    def test_tapify_args_kwargs(self):
        def concat(a: int, *args, b: int, **kwargs) -> str:
            return f"{a} {args} {b} {kwargs}"

        def concat_with_positionals(a: int, /, *args, b: int, **kwargs) -> str:
            return f"{a} {args} {b} {kwargs}"

        class Concat:
            def __init__(self, a: int, *args, b: int, **kwargs):
                self.a = a
                self.args = args
                self.b = b
                self.kwargs = kwargs

            def __eq__(self, other: str) -> bool:
                return other == concat(a=self.a, *self.args, b=self.b, **self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, a: int, /, *args, b: int, **kwargs):
                self.a = a
                self.args = args
                self.b = b
                self.kwargs = kwargs

            def __eq__(self, other: str) -> bool:
                return other == concat(a=self.a, *self.args, b=self.b, **self.kwargs)

        for class_or_function in [concat, concat_with_positionals, Concat, ConcatWithPositionals]:
            with self.assertRaises(SystemExit):
                tapify(class_or_function, command_line_args=["--a", "1", "--b", "2"])

    def test_tapify_help(self):
        def concat(a: int, b: int, c: int) -> str:
            """Concatenate three numbers.

            :param a: The first number.
            :param b: The second number.
            :param c: The third number.
            """
            return f"{a} {b} {c}"

        def concat_without_docstring_description(a: int, b: int, c: int) -> str:
            """
            :param a: The first number.
            :param b: The second number.
            :param c: The third number.
            """
            # For this function, docstring.short_description and docstring.long_description are None
            return f"{a} {b} {c}"

        def concat_with_positionals(a: int, b: int, /, c: int) -> str:
            """Concatenate three numbers.

            :param a: The first number.
            :param b: The second number.
            :param c: The third number.
            """
            return f"{a} {b} {c}"

        class Concat:
            def __init__(self, a: int, b: int, c: int):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                :param c: The third number.
                """
                self.kwargs = {"a": a, "b": b, "c": c}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, a: int, b: int, /, c: int):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                :param c: The third number.
                """
                self.kwargs = {"a": a, "b": b, "c": c}

            def __eq__(self, other: str) -> bool:
                return other == concat(**self.kwargs)

        @dataclass
        class ConcatDataclass:
            """Concatenate three numbers.

            :param a: The first number.
            :param b: The second number.
            :param c: The third number.
            """

            a: int
            b: int
            c: int

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.b, self.c)

        if _IS_PYDANTIC_V1 is not None:

            @pydantic.dataclasses.dataclass
            class ConcatDataclassPydantic:
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                :param c: The third number.
                """

                a: int
                b: int
                c: int

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.b, self.c)

            class ConcatModel(pydantic.BaseModel):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                :param c: The third number.
                """

                a: int
                b: int
                c: int

                def __eq__(self, other: str) -> bool:
                    return other == concat(self.a, self.b, self.c)

            pydantic_data_models = [ConcatDataclassPydantic, ConcatModel]
        else:
            pydantic_data_models = []

        expected_description = "Concatenate three numbers."
        for class_or_function in [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
            ConcatDataclass,
        ] + pydantic_data_models:
            f = io.StringIO()
            with contextlib.redirect_stdout(f):
                with self.assertRaises(SystemExit):
                    if class_or_function == concat_without_docstring_description:
                        tapify(class_or_function, command_line_args=["-h"], description=expected_description)
                    else:
                        tapify(class_or_function, command_line_args=["-h"])

            # TODO: change the assertIn checks to instead check exact match like in test_to_tap_class
            stdout = f.getvalue()
            self.assertIn(expected_description, stdout)
            self.assertIn("--a A       (int, required) The first number.", stdout)
            self.assertIn("--b B       (int, required) The second number.", stdout)
            self.assertIn("--c C       (int, required) The third number.", stdout)


class TestTapifyExplicitBool(unittest.TestCase):
    def setUp(self) -> None:
        def cool(is_cool: bool = False) -> "Cool":
            """cool.

            :param is_cool: is it cool?
            """
            return Cool(is_cool)

        def cool_with_positionals(is_cool: bool = False, /) -> "Cool":
            """cool.

            :param is_cool: is it cool?
            """
            return Cool(is_cool)

        class Cool:
            def __init__(self, is_cool: bool = False):
                """cool.

                :param is_cool: is it cool?
                """
                self.is_cool = is_cool

            def __eq__(self, other: "Cool") -> bool:
                return other.is_cool == cool(self.is_cool).is_cool

        class CoolWithPositionals:
            def __init__(self, is_cool: bool = False, /):
                """cool.

                :param is_cool: is it cool?
                """
                self.is_cool = is_cool

            def __eq__(self, other: "Cool") -> bool:
                return other.is_cool == cool(self.is_cool).is_cool

        self.class_or_functions = [
            cool,
            cool_with_positionals,
            Cool,
            CoolWithPositionals,
        ]

    def test_explicit_bool_true(self):
        for class_or_function in self.class_or_functions:
            # Since the boolean argument is_cool is set to False by default and explicit_bool is False,
            # the argument is_cool is False.
            a_cool = tapify(class_or_function, command_line_args=[], explicit_bool=False)
            self.assertEqual(a_cool.is_cool, False)

            # If the argument is provided, it is set to True.
            a_cool = tapify(class_or_function, command_line_args=["--is_cool"], explicit_bool=False)
            self.assertEqual(a_cool.is_cool, True)

            # If explicit_bool is True and the argument is not provided then the default of False should be used.
            a_cool = tapify(class_or_function, command_line_args=[], explicit_bool=True)
            self.assertEqual(a_cool.is_cool, False)

            # If explicit_bool is True and the argument is provided without an explicit boolean assigned to it
            # then the argument parser should crash.
            with self.assertRaises(SystemExit):
                tapify(class_or_function, command_line_args=["--is_cool"], explicit_bool=True)

            # If explicit_bool is True and the argument is provided with an explicit boolean assigned to it
            # then the argument should be set to what it's assigned to.
            a_cool = tapify(class_or_function, command_line_args=["--is_cool", "False"], explicit_bool=True)
            self.assertEqual(a_cool.is_cool, False)

            a_cool = tapify(class_or_function, command_line_args=["--is_cool", "True"], explicit_bool=True)
            self.assertEqual(a_cool.is_cool, True)


class TestTapifyKwargs(unittest.TestCase):
    def setUp(self) -> None:
        def concat(a: int, b: int = 2, **kwargs) -> str:
            """Concatenate three numbers.

            :param a: The first number.
            :param b: The second number.
            """
            return f'{a}_{b}_{"-".join(f"{k}={v}" for k, v in kwargs.items())}'

        def concat_with_positionals(a: int, b: int = 2, /, **kwargs) -> str:
            """Concatenate three numbers.

            :param a: The first number.
            :param b: The second number.
            """
            return f'{a}_{b}_{"-".join(f"{k}={v}" for k, v in kwargs.items())}'

        if _IS_PYDANTIC_V1 is not None:

            class ConcatModel(pydantic.BaseModel):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                """

                if _IS_PYDANTIC_V1:

                    class Config:
                        extra = pydantic.Extra.allow  # by default, pydantic ignores extra arguments

                else:
                    model_config = pydantic.ConfigDict(extra="allow")  # by default, pydantic ignores extra arguments

                a: int
                b: int = 2

                def __eq__(self, other: str) -> bool:
                    if _IS_PYDANTIC_V1:
                        # Get the kwarg names in the correct order by parsing other
                        kwargs_str = other.split("_")[-1]
                        if not kwargs_str:
                            kwarg_names = []
                        else:
                            kwarg_names = [kv_str.split("=")[0] for kv_str in kwargs_str.split("-")]
                        kwargs = {name: getattr(self, name) for name in kwarg_names}
                        # Need to explictly check that the extra names from other are identical to what's stored in self
                        # Checking other == concat(...) isn't sufficient b/c self could have more extra fields
                        assert set(kwarg_names) == set(self.__dict__.keys()) - set(self.__fields__.keys())
                    else:
                        kwargs = self.model_extra
                    return other == concat(self.a, self.b, **kwargs)

            pydantic_data_models = [ConcatModel]
        else:
            pydantic_data_models = []

        class Concat:
            def __init__(self, a: int, b: int = 2, **kwargs: dict[str, str]):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                """
                self.a = a
                self.b = b
                self.kwargs = kwargs

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.b, **self.kwargs)

        class ConcatWithPositionals:
            def __init__(self, a: int, /, b: int = 2, **kwargs: dict[str, str]):
                """Concatenate three numbers.

                :param a: The first number.
                :param b: The second number.
                """
                self.a = a
                self.b = b
                self.kwargs = kwargs

            def __eq__(self, other: str) -> bool:
                return other == concat(self.a, self.b, **self.kwargs)

        self.class_or_functions = [
            concat,
            concat_with_positionals,
            Concat,
            ConcatWithPositionals,
        ] + pydantic_data_models

    def test_tapify_empty_kwargs(self) -> None:
        for class_or_function in self.class_or_functions:
            output = tapify(class_or_function, command_line_args=["--a", "1"])

            self.assertEqual(output, "1_2_")

    def test_tapify_has_kwargs(self) -> None:
        for class_or_function in self.class_or_functions:
            output = tapify(class_or_function, command_line_args=["--a", "1", "--c", "3", "--d", "4"])

            self.assertEqual(output, "1_2_c=3-d=4")

    def test_tapify_has_kwargs_replace_default(self) -> None:
        for class_or_function in self.class_or_functions:
            output = tapify(class_or_function, command_line_args=["--a", "1", "--c", "3", "--b", "5", "--d", "4"])

            self.assertEqual(output, "1_5_c=3-d=4")


class TestTapifyUnderscoresToDashes(unittest.TestCase):
    def setUp(self) -> None:
        class MyClass:
            def __init__(self, my_arg: str):
                self.my_arg = my_arg

            def __eq__(self, other: str) -> bool:
                return self.my_arg == other

        @dataclass
        class DataClassTarget:
            my_arg: str

            def __eq__(self, other: str) -> bool:
                return self.my_arg == other

        def my_function(my_arg: str) -> str:
            return my_arg

        self.class_or_functions = [my_function, MyClass, DataClassTarget]

    def test_underscores_to_dashes(self) -> None:
        for target in self.class_or_functions:
            # With underscores_to_dashes True and using dashes in the args.
            instance = tapify(target, command_line_args=["--my-arg", "value"], underscores_to_dashes=True)
            self.assertEqual(instance, "value")

            # With underscores_to_dashes False and using underscore in the args.
            instance = tapify(target, command_line_args=["--my_arg", "value"], underscores_to_dashes=False)
            self.assertEqual(instance, "value")

            # Using underscore when dashes are expected causes a parse error.
            with self.assertRaises(SystemExit):
                tapify(target, command_line_args=["--my_arg", "value"], underscores_to_dashes=True)


if __name__ == "__main__":
    unittest.main()
