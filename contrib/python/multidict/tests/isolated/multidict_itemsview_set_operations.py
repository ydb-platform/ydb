import gc

import objgraph  # type: ignore[import-untyped]

from multidict import MultiDict


class SubtractionValue:
    pass


class ReflectedUnionValue:
    pass


def _run_isolated_case() -> None:
    for _ in range(100):
        subtraction_value = SubtractionValue()
        subtraction_md = MultiDict([("key", subtraction_value)])
        subtraction_result = subtraction_md.items() - set()
        del subtraction_result, subtraction_md, subtraction_value

    for _ in range(100):
        reflected_union_value = ReflectedUnionValue()
        reflected_union_md = MultiDict([("key", reflected_union_value)])
        reflected_union_result = set() | reflected_union_md.items()
        del reflected_union_result, reflected_union_md, reflected_union_value

    gc.collect()
    leaked_subtraction = len(objgraph.by_type("SubtractionValue"))
    leaked_reflected_union = len(objgraph.by_type("ReflectedUnionValue"))
    assert leaked_subtraction == 0, (
        f"{leaked_subtraction} subtraction values not collected by GC"
    )
    assert leaked_reflected_union == 0, (
        f"{leaked_reflected_union} reflected union values not collected by GC"
    )


if __name__ == "__main__":
    _run_isolated_case()
