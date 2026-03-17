from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Container, Mapping, Sequence

from ...datastructures import OrderedUniqueGrouper
from ...provider.loc_stack_filtering import LocStack
from ...provider.loc_stack_tools import format_loc_stack
from ...utils import Omitted
from .definitions import JSONSchema, RefSource, ResolvedJSONSchema
from .schema_tools import replace_json_schema_ref, traverse_json_schema


class JSONSchemaResolver(ABC):
    @abstractmethod
    def resolve(
        self,
        occupied_refs: Container[str],
        root_schemas: Sequence[JSONSchema],
    ) -> tuple[Mapping[str, ResolvedJSONSchema], Sequence[ResolvedJSONSchema]]:
        ...


class RefGenerator(ABC):
    @abstractmethod
    def generate_ref(self, json_schema: JSONSchema, loc_stack: LocStack) -> str:
        ...


class RefMangler(ABC):
    @abstractmethod
    def mangle_refs(
        self,
        occupied_refs: Container[str],
        common_ref: str,
        sources: Sequence[RefSource],
    ) -> Mapping[RefSource, str]:
        ...


class BuiltinJSONSchemaResolver(JSONSchemaResolver):
    def __init__(self, ref_generator: RefGenerator, ref_mangler: RefMangler):
        self._ref_generator = ref_generator
        self._ref_mangler = ref_mangler

    def resolve(
        self,
        occupied_refs: Container[str],
        root_schemas: Sequence[JSONSchema],
    ) -> tuple[Mapping[str, ResolvedJSONSchema], Sequence[ResolvedJSONSchema]]:
        ref_to_sources = self._collect_ref_to_sources(root_schemas)
        source_determinator = self._get_source_determinator(occupied_refs, ref_to_sources)
        defs = {
            ref: replace_json_schema_ref(source.json_schema, source_determinator)
            for source, ref in source_determinator.items()
        }
        schemas = [
            replace_json_schema_ref(root, source_determinator)
            for root in root_schemas
        ]
        return defs, schemas

    def _collect_ref_to_sources(self, root_schemas: Sequence[JSONSchema]) -> Mapping[str, Sequence[RefSource]]:
        grouper = OrderedUniqueGrouper[str, RefSource[JSONSchema]]()
        for root in root_schemas:
            for schema in traverse_json_schema(root):
                ref_source = schema.ref
                if isinstance(ref_source, Omitted):
                    continue

                ref = (
                    self._ref_generator.generate_ref(ref_source.json_schema, ref_source.loc_stack)
                    if ref_source.value is None else
                    ref_source.value
                )
                grouper.add(ref, ref_source)
        return grouper.finalize()

    def _get_source_determinator(
        self,
        occupied_refs: Container[str],
        ref_to_sources: Mapping[str, Sequence[RefSource]],
    ) -> Mapping[RefSource, str]:
        source_determinator = {}
        for common_ref, sources in ref_to_sources.items():
            if len(sources) == 1 and common_ref not in occupied_refs:
                source_determinator[sources[0]] = common_ref
            else:
                self._validate_sources(common_ref, sources)
                mangling_result = self._ref_mangler.mangle_refs(occupied_refs, common_ref, sources)
                source_determinator.update(mangling_result)
        self._validate_mangling(source_determinator)
        return source_determinator

    def _validate_sources(self, common_ref: str, sources: Sequence[RefSource]) -> None:
        pinned_sources = [source for source in sources if source.value is not None]
        if len(pinned_sources) > 1:
            pinned = ", ".join(f"`{format_loc_stack(pinned.loc_stack)}`" for pinned in pinned_sources)
            raise ValueError(
                f"Cannot create consistent json schema,"
                f" there are different sub schemas with pinned ref {common_ref!r}."
                f" {pinned}",
            )

    def _validate_mangling(self, source_determinator: Mapping[RefSource, str]) -> None:
        ref_to_sources = defaultdict(list)
        for source, ref in source_determinator.items():
            ref_to_sources[ref].append(source)

        unmangled = [(ref, sources) for ref, sources in ref_to_sources.items() if len(sources) > 1]
        if unmangled:
            unmangled_desc = "; ".join(
                f"For ref {ref!r} at "
                + ", and at ".join(
                    f"`{format_loc_stack(source.json_schema)}`" for source in sources
                )
                for ref, sources in unmangled
            )
            raise ValueError(
                f"Cannot create consistent json schema,"
                f" cannot mangle some refs."
                f" {unmangled_desc}",
            )
