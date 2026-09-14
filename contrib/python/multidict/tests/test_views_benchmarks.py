"""codspeed benchmarks for multidict views."""

from typing import Type

from pytest_codspeed import BenchmarkFixture

from multidict import MultiDict


def test_keys_view_equals(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})

    @benchmark
    def _run() -> None:
        assert md1.keys() == md2.keys()


def test_keys_view_not_equals(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(20, 120)})

    @benchmark
    def _run() -> None:
        assert md1.keys() != md2.keys()


def test_keys_view_more(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {str(i) for i in range(50)}

    @benchmark
    def _run() -> None:
        assert md.keys() > s


def test_keys_view_more_or_equal(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {str(i) for i in range(100)}

    @benchmark
    def _run() -> None:
        assert md.keys() >= s


def test_keys_view_less(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {str(i) for i in range(150)}

    @benchmark
    def _run() -> None:
        assert md.keys() < s


def test_keys_view_less_or_equal(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {str(i) for i in range(100)}

    @benchmark
    def _run() -> None:
        assert md.keys() <= s


def test_keys_view_and(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.keys() & md2.keys()) == 50


def test_keys_view_or(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.keys() | md2.keys()) == 150


def test_keys_view_sub(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.keys() - md2.keys()) == 50


def test_keys_view_xor(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.keys() ^ md2.keys()) == 100


def test_keys_view_is_disjoint(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100, 200)})

    @benchmark
    def _run() -> None:
        assert md1.keys().isdisjoint(md2.keys())


def test_keys_view_repr(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})

    @benchmark
    def _run() -> None:
        repr(md.keys())


def test_items_view_equals(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})

    @benchmark
    def _run() -> None:
        assert md1.items() == md2.items()


def test_items_view_not_equals(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(20, 120)})

    @benchmark
    def _run() -> None:
        assert md1.items() != md2.items()


def test_items_view_more(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {(str(i), str(i)) for i in range(50)}

    @benchmark
    def _run() -> None:
        assert md.items() > s


def test_items_view_more_or_equal(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {(str(i), str(i)) for i in range(100)}

    @benchmark
    def _run() -> None:
        assert md.items() >= s


def test_items_view_less(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {(str(i), str(i)) for i in range(150)}

    @benchmark
    def _run() -> None:
        assert md.items() < s


def test_items_view_less_or_equal(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    s = {(str(i), str(i)) for i in range(100)}

    @benchmark
    def _run() -> None:
        assert md.items() <= s


def test_items_view_and(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.items() & md2.items()) == 50


def test_items_view_or(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.items() | md2.items()) == 150


def test_items_view_sub(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.items() - md2.items()) == 50


def test_items_view_xor(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(50, 150)})

    @benchmark
    def _run() -> None:
        assert len(md1.items() ^ md2.items()) == 100


def test_items_view_is_disjoint(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md1: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})
    md2: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100, 200)})

    @benchmark
    def _run() -> None:
        assert md1.items().isdisjoint(md2.items())


def test_items_view_repr(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})

    @benchmark
    def _run() -> None:
        repr(md.items())


def test_values_view_repr(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md: MultiDict[str] = any_multidict_class({str(i): str(i) for i in range(100)})

    @benchmark
    def _run() -> None:
        repr(md.values())
