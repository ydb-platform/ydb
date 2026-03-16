from collections.abc import Container, Mapping, Sequence
from itertools import count
from typing import Optional

from ...datastructures import OrderedUniqueGrouper
from .definitions import RefSource
from .resolver import RefMangler


class IndexRefMangler(RefMangler):
    def __init__(self, start: int = 1, separator: str = "-"):
        self._start = start
        self._separator = separator

    def mangle_refs(
        self,
        occupied_refs: Container[str],
        common_ref: str,
        sources: Sequence[RefSource],
    ) -> Mapping[RefSource, str]:
        result = {}
        counter = count(self._start)
        for source in sources:
            while True:
                idx = next(counter)
                mangled = self._with_index(common_ref, idx)
                if mangled not in occupied_refs:
                    result[source] = mangled
                    break

        return result

    def _with_index(self, common_ref: str, index: int) -> str:
        return f"{common_ref}{self._separator}{index}"


class QualnameRefMangler(RefMangler):
    def mangle_refs(
        self,
        occupied_refs: Container[str],
        common_ref: str,
        sources: Sequence[RefSource],
    ) -> Mapping[RefSource, str]:
        return {source: self._generate_name(source) or common_ref for source in sources}

    def _generate_name(self, source: RefSource) -> Optional[str]:
        tp = source.loc_stack.last.type
        return getattr(tp, "__qualname__", None)


class CompoundRefMangler(RefMangler):
    def __init__(self, base: RefMangler, wrapper: RefMangler):
        self._base = base
        self._wrapper = wrapper

    def mangle_refs(
        self,
        occupied_refs: Container[str],
        common_ref: str,
        sources: Sequence[RefSource],
    ) -> Mapping[RefSource, str]:
        mangled = self._base.mangle_refs(occupied_refs, common_ref, sources)

        grouper = OrderedUniqueGrouper[str, RefSource]()
        for source, ref in mangled.items():
            grouper.add(ref, source)

        for ref, ref_sources in grouper.finalize().items():
            if len(ref_sources) > 1:
                mangled = {**mangled, **self._wrapper.mangle_refs(occupied_refs, ref, ref_sources)}

        return mangled
