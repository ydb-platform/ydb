"""codspeed benchmarks for multidict."""

from typing import Dict, Type, Union

from pytest_codspeed import BenchmarkFixture

from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
    MultiDict,
    MultiDictProxy,
    istr,
)

# Note that this benchmark should not be refactored to use pytest.mark.parametrize
# since each benchmark name should be unique.

_SENTINEL = object()


def test_multidict_insert_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class()
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md[i] = i


def test_cimultidict_insert_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class()
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md[i] = i


def test_multidict_add_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    base_md = any_multidict_class()
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for _ in range(100):
            md = base_md.copy()
            for i in items:
                md.add(i, i)


def test_cimultidict_add_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    base_md = case_insensitive_multidict_class()
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for j in range(100):
            md = base_md.copy()
            for i in items:
                md.add(i, i)


def test_multidict_pop_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md_base = any_multidict_class((str(i), str(i)) for i in range(200))
    items = [str(i) for i in range(50, 150)]

    @benchmark
    def _run() -> None:
        md = md_base.copy()
        for i in items:
            md.pop(i)


def test_cimultidict_pop_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md_base = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(200)
    )
    items = [case_insensitive_str_class(i) for i in range(50, 150)]

    @benchmark
    def _run() -> None:
        md = md_base.copy()
        for i in items:
            md.pop(i)


def test_multidict_popitem_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md_base = any_multidict_class((str(i), str(i)) for i in range(100))

    @benchmark
    def _run() -> None:
        md = md_base.copy()
        for _ in range(100):
            md.popitem()


def test_multidict_clear_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(100))

    @benchmark
    def _run() -> None:
        md.clear()


def test_multidict_update_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(150))
    items = {str(i): str(i) for i in range(100, 200)}

    @benchmark
    def _run() -> None:
        md.update(items)


def test_cimultidict_update_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(150)
    )
    items: Dict[Union[str, istr], istr] = {
        case_insensitive_str_class(i): case_insensitive_str_class(i)
        for i in range(100, 200)
    }

    @benchmark
    def _run() -> None:
        md.update(items)


def test_multidict_update_str_with_kwargs(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(150))
    items = {str(i): str(i) for i in range(100, 200)}
    kwargs = {str(i): str(i) for i in range(200, 300)}

    @benchmark
    def _run() -> None:
        md.update(items, **kwargs)


def test_cimultidict_update_istr_with_kwargs(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(150)
    )
    items: Dict[Union[str, istr], istr] = {
        case_insensitive_str_class(i): case_insensitive_str_class(i)
        for i in range(100, 200)
    }
    kwargs = {str(i): case_insensitive_str_class(i) for i in range(200, 300)}

    @benchmark
    def _run() -> None:
        md.update(items, **kwargs)


def test_multidict_extend_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    base_md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = {str(i): str(i) for i in range(200)}

    @benchmark
    def _run() -> None:
        for j in range(100):
            md = base_md.copy()
            md.extend(items)


def test_cimultidict_extend_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    base_md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = {
        case_insensitive_str_class(i): case_insensitive_str_class(i) for i in range(200)
    }

    @benchmark
    def _run() -> None:
        for _ in range(100):
            md = base_md.copy()
            md.extend(items)


def test_multidict_extend_str_with_kwargs(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    base_md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = {str(i): str(i) for i in range(200)}
    kwargs = {str(i): str(i) for i in range(200, 300)}

    @benchmark
    def _run() -> None:
        for j in range(100):
            md = base_md.copy()
            md.extend(items, **kwargs)


def test_cimultidict_extend_istr_with_kwargs(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    base_md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = {
        case_insensitive_str_class(i): case_insensitive_str_class(i) for i in range(200)
    }
    kwargs = {str(i): case_insensitive_str_class(i) for i in range(200, 300)}

    @benchmark
    def _run() -> None:
        for _ in range(100):
            md = base_md.copy()
            md.extend(items, **kwargs)


def test_multidict_delitem_str(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md_base = any_multidict_class((str(i), str(i)) for i in range(100))
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        md = md_base.copy()
        for i in items:
            del md[i]


def test_cimultidict_delitem_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md_base = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        md = md_base.copy()
        for i in items:
            del md[i]


def test_multidict_getall_str_hit(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class(
        (f"key{j}", str(f"{i}-{j}")) for i in range(100) for j in range(10)
    )

    key = "key5"

    @benchmark
    def _run() -> None:
        for i in range(1000):
            md.getall(key)


def test_multidict_getall_str_miss(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class(
        (f"key{j}", str(f"{i}-{j}")) for i in range(100) for j in range(10)
    )

    key = "key-miss"

    @benchmark
    def _run() -> None:
        for i in range(1000):
            md.getall(key, ())


def test_cimultidict_getall_istr_hit(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (f"key{j}", case_insensitive_str_class(f"{i}-{j}"))
        for i in range(100)
        for j in range(10)
    )

    key = case_insensitive_str_class("key5")

    @benchmark
    def _run() -> None:
        for i in range(1000):
            md.getall(key)


def test_cimultidict_getall_istr_miss(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(f"key{j}"), case_insensitive_str_class(f"{i}-{j}"))
        for i in range(100)
        for j in range(10)
    )

    key = case_insensitive_str_class("key-miss")

    @benchmark
    def _run() -> None:
        for i in range(1000):
            md.getall(key, ())


def test_multidict_fetch(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md[i]


def test_cimultidict_fetch_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md[i]


def test_multidict_get_hit(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i)


def test_multidict_get_miss(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = [str(i) for i in range(100, 200)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i)


def test_cimultidict_get_istr_hit(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i)


def test_cimultidict_get_istr_miss(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100, 200)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i)


def test_multidict_get_hit_with_default(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    md = any_multidict_class((str(i), str(i)) for i in range(100))
    items = [str(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i, _SENTINEL)


def test_cimultidict_get_istr_hit_with_default(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i, _SENTINEL)


def test_cimultidict_get_istr_with_default_miss(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    md = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    )
    items = [case_insensitive_str_class(i) for i in range(100, 200)]

    @benchmark
    def _run() -> None:
        for i in items:
            md.get(i, _SENTINEL)


def test_multidict_repr(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [str(i) for i in range(100)]
    md = any_multidict_class([(i, i) for i in items])

    @benchmark
    def _run() -> None:
        repr(md)


def test_create_empty_multidict(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    @benchmark
    def _run() -> None:
        any_multidict_class()


def test_create_multidict_with_items(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]

    @benchmark
    def _run() -> None:
        any_multidict_class(items)


def test_create_cimultidict_with_items_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    items = [
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    ]

    @benchmark
    def _run() -> None:
        case_insensitive_multidict_class(items)


def test_create_multidict_with_dict(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    dct = {str(i): str(i) for i in range(100)}

    @benchmark
    def _run() -> None:
        any_multidict_class(dct)


def test_create_cimultidict_with_dict_istr(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    dct = {
        case_insensitive_str_class(i): case_insensitive_str_class(i) for i in range(100)
    }

    @benchmark
    def _run() -> None:
        case_insensitive_multidict_class(dct)


def test_create_multidict_with_items_with_kwargs(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    kwargs = {str(i): str(i) for i in range(100)}

    @benchmark
    def _run() -> None:
        any_multidict_class(items, **kwargs)


def test_create_cimultidict_with_items_istr_with_kwargs(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    items = [
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    ]
    kwargs = {str(i): case_insensitive_str_class(i) for i in range(100)}

    @benchmark
    def _run() -> None:
        case_insensitive_multidict_class(items, **kwargs)


def test_create_empty_multidictproxy(benchmark: BenchmarkFixture) -> None:
    md: MultiDict[str] = MultiDict()

    @benchmark
    def _run() -> None:
        MultiDictProxy(md)


def test_create_multidictproxy(benchmark: BenchmarkFixture) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    md: MultiDict[str] = MultiDict(items)

    @benchmark
    def _run() -> None:
        MultiDictProxy(md)


def test_create_empty_cimultidictproxy(
    benchmark: BenchmarkFixture,
) -> None:
    md: CIMultiDict[istr] = CIMultiDict()

    @benchmark
    def _run() -> None:
        CIMultiDictProxy(md)


def test_create_cimultidictproxy(
    benchmark: BenchmarkFixture,
    case_insensitive_str_class: type[istr],
) -> None:
    items = [
        (case_insensitive_str_class(i), case_insensitive_str_class(i))
        for i in range(100)
    ]
    md = CIMultiDict(items)

    @benchmark
    def _run() -> None:
        CIMultiDictProxy(md)


def test_create_from_existing_cimultidict(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    existing = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i)) for i in range(5)
    )

    @benchmark
    def _run() -> None:
        case_insensitive_multidict_class(existing)


def test_copy_from_existing_cimultidict(
    benchmark: BenchmarkFixture,
    case_insensitive_multidict_class: Type[CIMultiDict[istr]],
    case_insensitive_str_class: type[istr],
) -> None:
    existing = case_insensitive_multidict_class(
        (case_insensitive_str_class(i), case_insensitive_str_class(i)) for i in range(5)
    )

    @benchmark
    def _run() -> None:
        existing.copy()


def test_iterate_multidict(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    md = any_multidict_class(items)

    @benchmark
    def _run() -> None:
        for _ in md:
            pass


def test_iterate_multidict_keys(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    md = any_multidict_class(items)

    @benchmark
    def _run() -> None:
        for _ in md.keys():
            pass


def test_iterate_multidict_values(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    md = any_multidict_class(items)

    @benchmark
    def _run() -> None:
        for _ in md.values():
            pass


def test_iterate_multidict_items(
    benchmark: BenchmarkFixture, any_multidict_class: Type[MultiDict[str]]
) -> None:
    items = [(str(i), str(i)) for i in range(100)]
    md = any_multidict_class(items)

    @benchmark
    def _run() -> None:
        for _, _ in md.items():
            pass
