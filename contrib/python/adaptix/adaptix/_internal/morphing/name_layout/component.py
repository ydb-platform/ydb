from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar, Union

from ...common import VarTuple
from ...model_tools.definitions import (
    BaseField,
    BaseShape,
    DefaultFactory,
    DefaultFactoryWithSelf,
    DefaultValue,
    InputField,
    NoDefault,
    OutputField,
)
from ...name_style import NameStyle, convert_snake_style
from ...provider.essential import AggregateCannotProvide, CannotProvide, Mediator, Provider
from ...provider.fields import field_to_loc
from ...provider.loc_stack_filtering import LocStackChecker
from ...provider.located_request import LocatedRequest
from ...provider.overlay_schema import Overlay, Schema, provide_schema
from ...retort.operating_retort import OperatingRetort
from ...special_cases_optimization import with_default_clause
from ...utils import Omittable, get_prefix_groups
from ..model.crown_definitions import (
    BaseFieldCrown,
    BaseNameLayoutRequest,
    DictExtraPolicy,
    ExtraCollect,
    ExtraExtract,
    ExtraForbid,
    ExtraKwargs,
    ExtraSaturate,
    ExtraSkip,
    ExtraTargets,
    InpExtraMove,
    InpFieldCrown,
    InpNoneCrown,
    InputNameLayoutRequest,
    LeafBaseCrown,
    LeafInpCrown,
    LeafOutCrown,
    OutExtraMove,
    OutFieldCrown,
    OutNoneCrown,
    OutputNameLayoutRequest,
    Sieve,
)
from .base import (
    ExtraIn,
    ExtraMoveMaker,
    ExtraOut,
    ExtraPoliciesMaker,
    Key,
    KeyPath,
    PathsTo,
    SievesMaker,
    StructureMaker,
)
from .name_mapping import NameMappingRequest


@dataclass(frozen=True)
class StructureSchema(Schema):
    skip: LocStackChecker
    only: LocStackChecker

    map: VarTuple[Provider]
    trim_trailing_underscore: bool
    name_style: Optional[NameStyle]
    as_list: bool


@dataclass(frozen=True)
class StructureOverlay(Overlay[StructureSchema]):
    skip: Omittable[LocStackChecker]
    only: Omittable[LocStackChecker]

    map: Omittable[VarTuple[Provider]]
    trim_trailing_underscore: Omittable[bool]
    name_style: Omittable[Optional[NameStyle]]
    as_list: Omittable[bool]

    def _merge_map(self, old: VarTuple[Provider], new: VarTuple[Provider]) -> VarTuple[Provider]:
        return new + old


AnyField = Union[InputField, OutputField]
LeafCr = TypeVar("LeafCr", bound=LeafBaseCrown)
FieldCr = TypeVar("FieldCr", bound=BaseFieldCrown)
F = TypeVar("F", bound=BaseField)
FieldAndPath = tuple[F, Optional[KeyPath]]


def apply_lsc(
    mediator: Mediator,
    request: BaseNameLayoutRequest,
    loc_stack_checker: LocStackChecker,
    field: BaseField,
) -> bool:
    loc_stack = request.loc_stack.append_with(field_to_loc(field))
    return loc_stack_checker.check_loc_stack(mediator, loc_stack)


class NameMappingRetort(OperatingRetort):
    def provide_name_mapping(self, request: NameMappingRequest) -> Optional[KeyPath]:
        return self._provide_from_recipe(request)


class BuiltinStructureMaker(StructureMaker):
    def _generate_key(self, schema: StructureSchema, shape: BaseShape, field: BaseField) -> Key:
        if schema.as_list:
            return shape.fields.index(field)

        name = field.id
        if schema.trim_trailing_underscore and name.endswith("_") and not name.endswith("__"):
            name = name.rstrip("_")
        if schema.name_style is not None:
            name = convert_snake_style(name, schema.name_style)
        return name

    def _create_name_mapping_retort(self, schema: StructureSchema) -> NameMappingRetort:
        return NameMappingRetort(recipe=schema.map)

    def _map_fields(
        self,
        mediator: Mediator,
        request: BaseNameLayoutRequest,
        schema: StructureSchema,
        extra_move: Union[InpExtraMove, OutExtraMove],
    ) -> Iterable[FieldAndPath]:
        extra_targets = extra_move.fields if isinstance(extra_move, ExtraTargets) else ()
        retort = self._create_name_mapping_retort(schema)
        for field in request.shape.fields:
            if field.id in extra_targets:
                continue

            generated_key = self._generate_key(schema, request.shape, field)
            try:
                path = retort.provide_name_mapping(
                    NameMappingRequest(
                        shape=request.shape,
                        field=field,
                        generated_key=generated_key,
                        loc_stack=request.loc_stack.append_with(field_to_loc(field)),
                    ),
                )
            except CannotProvide:
                path = (generated_key, )

            if path is None:
                yield field, None
            elif (
                not apply_lsc(mediator, request, schema.skip, field)
                and apply_lsc(mediator, request, schema.only, field)
            ):
                yield field, path
            else:
                yield field, None

    def _validate_structure(
        self,
        request: LocatedRequest,
        fields_to_paths: Iterable[FieldAndPath],
    ) -> None:
        paths_to_fields: defaultdict[KeyPath, list[AnyField]] = defaultdict(list)
        for field, path in fields_to_paths:
            if path is not None:
                paths_to_fields[path].append(field)

        duplicates = {
            path: [field.id for field in fields]
            for path, fields in paths_to_fields.items()
            if len(fields) > 1
        }
        if duplicates:
            raise AggregateCannotProvide(
                "Some fields point to the same path (have same alias)",
                [
                    CannotProvide(f"Fields {fields} point to the {path}", is_demonstrative=True)
                    for path, fields in duplicates.items()
                ],
                is_terminal=True,
                is_demonstrative=True,
            )

        prefix_groups = get_prefix_groups([path for field, path in fields_to_paths if path is not None])
        if prefix_groups:
            raise AggregateCannotProvide(
                "Path to the field must not be a prefix of another path",
                [
                    AggregateCannotProvide(
                        f"Field {paths_to_fields[prefix][0].id!r} points to path {prefix} which is prefix of:",
                        [
                            CannotProvide(
                                f"Field {paths_to_fields[path][0].id!r} points to {path}",
                                is_demonstrative=True,
                            )
                            for path in paths
                        ],
                        is_demonstrative=True,
                    )
                    for prefix, paths in prefix_groups
                ],
                is_terminal=True,
                is_demonstrative=True,
            )

        optional_fields_at_list = [
            (field, path)
            for field, path in fields_to_paths
            if path is not None and field.is_optional and isinstance(path[-1], int)
        ]
        if optional_fields_at_list:
            raise AggregateCannotProvide(
                "Optional fields cannot be mapped to list elements",
                [
                    CannotProvide(
                        f"Field {field.id!r} points to {path}",
                        is_demonstrative=True,
                    )
                    for (field, path) in optional_fields_at_list
                ],
                is_terminal=True,
                is_demonstrative=True,
            )

    def _iterate_sub_paths(self, paths: Iterable[KeyPath]) -> Iterable[tuple[KeyPath, Key]]:
        yielded: set[tuple[KeyPath, Key]] = set()
        for path in paths:
            for i in range(len(path) - 1, -1, -1):
                result = path[:i], path[i]
                if result in yielded:
                    break

                yielded.add(result)
                yield result

    def _get_paths_to_list(self, request: LocatedRequest, paths: Iterable[KeyPath]) -> Mapping[KeyPath, Sequence[int]]:
        paths_to_lists: defaultdict[KeyPath, list[int]] = defaultdict(list)
        paths_to_dicts: defaultdict[KeyPath, list[str]] = defaultdict(list)
        for sub_path, key in self._iterate_sub_paths(paths):
            if isinstance(key, int):
                if sub_path in paths_to_dicts:
                    raise CannotProvide(
                        f"Inconsistent path elements at {sub_path}"
                        f" — got string (e.g. {paths_to_dicts[sub_path][-1]!r}) and integer (e.g. {key!r}) keys",
                        is_terminal=True,
                        is_demonstrative=True,
                    )

                paths_to_lists[sub_path].append(key)
            else:
                if sub_path in paths_to_lists:
                    raise CannotProvide(
                        f"Inconsistent path elements at {sub_path}"
                        f" — got string (e.g. {key!r}) and integer (e.g. {paths_to_lists[sub_path][-1]!r}) keys",
                        is_terminal=True,
                        is_demonstrative=True,
                    )

                paths_to_dicts[sub_path].append(key)

        return paths_to_lists

    def _make_paths_to_leaves(
        self,
        request: LocatedRequest,
        fields_to_paths: Iterable[FieldAndPath],
        field_crown: Callable[[str], FieldCr],
        gaps_filler: Callable[[KeyPath], LeafCr],
    ) -> PathsTo[Union[FieldCr, LeafCr]]:
        paths_to_leaves: dict[KeyPath, Union[FieldCr, LeafCr]] = {
            path: field_crown(field.id)
            for field, path in fields_to_paths
            if path is not None
        }

        paths_to_lists = self._get_paths_to_list(request, paths_to_leaves.keys())
        for path, indexes in paths_to_lists.items():
            for i in range(max(indexes)):
                if i not in indexes:
                    complete_path = (*path, i)
                    paths_to_leaves[complete_path] = gaps_filler(complete_path)

        return paths_to_leaves

    def _fill_input_gap(self, path: KeyPath) -> LeafInpCrown:
        return InpNoneCrown()

    def _fill_output_gap(self, path: KeyPath) -> LeafOutCrown:
        return OutNoneCrown(placeholder=DefaultValue(None))

    def make_inp_structure(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
        extra_move: InpExtraMove,
    ) -> PathsTo[LeafInpCrown]:
        schema = provide_schema(StructureOverlay, mediator, request.loc_stack)
        fields_to_paths: list[FieldAndPath[InputField]] = list(
            self._map_fields(mediator, request, schema, extra_move),
        )
        skipped_required_fields = [
            field.id
            for field, path in fields_to_paths
            if path is None and field.is_required
        ]
        if skipped_required_fields:
            raise CannotProvide(
                f"Required fields {skipped_required_fields} are skipped",
                is_terminal=True,
                is_demonstrative=True,
            )
        paths_to_leaves = self._make_paths_to_leaves(request, fields_to_paths, InpFieldCrown, self._fill_input_gap)
        self._validate_structure(request, fields_to_paths)
        return paths_to_leaves

    def make_out_structure(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
        extra_move: OutExtraMove,
    ) -> PathsTo[LeafOutCrown]:
        schema = provide_schema(StructureOverlay, mediator, request.loc_stack)
        fields_to_paths: list[FieldAndPath[OutputField]] = list(
            self._map_fields(mediator, request, schema, extra_move),
        )
        paths_to_leaves = self._make_paths_to_leaves(request, fields_to_paths, OutFieldCrown, self._fill_output_gap)
        self._validate_structure(request, fields_to_paths)
        return paths_to_leaves

    def empty_as_list_inp(self, mediator: Mediator, request: InputNameLayoutRequest) -> bool:
        return provide_schema(StructureOverlay, mediator, request.loc_stack).as_list

    def empty_as_list_out(self, mediator: Mediator, request: OutputNameLayoutRequest) -> bool:
        return provide_schema(StructureOverlay, mediator, request.loc_stack).as_list


@dataclass(frozen=True)
class SievesSchema(Schema):
    omit_default: LocStackChecker


@dataclass(frozen=True)
class SievesOverlay(Overlay[SievesSchema]):
    omit_default: Omittable[LocStackChecker]


class BuiltinSievesMaker(SievesMaker):
    def _create_sieve(self, field: OutputField) -> Sieve:
        if isinstance(field.default, DefaultValue):
            default_value = field.default.value
            return with_default_clause(field.default, lambda obj, value: value != default_value)

        if isinstance(field.default, DefaultFactory):
            default_factory = field.default.factory
            return with_default_clause(field.default, lambda obj, value: value != default_factory())

        if isinstance(field.default, DefaultFactoryWithSelf):
            default_factory_with_self = field.default.factory
            return with_default_clause(field.default, lambda obj, value: value != default_factory_with_self(obj))

        raise ValueError

    def make_sieves(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
        paths_to_leaves: PathsTo[LeafOutCrown],
    ) -> PathsTo[Sieve]:
        schema = provide_schema(SievesOverlay, mediator, request.loc_stack)
        result = {}
        for path, leaf in paths_to_leaves.items():
            if isinstance(leaf, OutFieldCrown):
                field = request.shape.fields_dict[leaf.id]
                if field.default != NoDefault() and apply_lsc(mediator, request, schema.omit_default, field):
                    result[path] = self._create_sieve(field)
        return result


def _paths_to_branches(paths_to_leaves: PathsTo[LeafBaseCrown]) -> Iterable[tuple[KeyPath, Key]]:
    yielded_branch_path: set[KeyPath] = set()
    for path in paths_to_leaves:
        for i in range(len(path) - 1, -2, -1):
            sub_path = path[:i]
            if sub_path in yielded_branch_path:
                break

            yield sub_path, path[i]


@dataclass(frozen=True)
class ExtraMoveAndPoliciesSchema(Schema):
    extra_in: ExtraIn
    extra_out: ExtraOut


@dataclass(frozen=True)
class ExtraMoveAndPoliciesOverlay(Overlay[ExtraMoveAndPoliciesSchema]):
    extra_in: Omittable[ExtraIn]
    extra_out: Omittable[ExtraOut]


class BuiltinExtraMoveAndPoliciesMaker(ExtraMoveMaker, ExtraPoliciesMaker):
    def _create_extra_targets(self, extra: Union[str, Sequence[str]]) -> ExtraTargets:
        if isinstance(extra, str):
            return ExtraTargets((extra,))
        return ExtraTargets(tuple(extra))

    def make_inp_extra_move(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
    ) -> InpExtraMove:
        schema = provide_schema(ExtraMoveAndPoliciesOverlay, mediator, request.loc_stack)
        if schema.extra_in in (ExtraForbid(), ExtraSkip()):
            return None
        if schema.extra_in == ExtraKwargs():
            return ExtraKwargs()
        if callable(schema.extra_in):
            return ExtraSaturate(schema.extra_in)
        return self._create_extra_targets(schema.extra_in)  # type: ignore[arg-type]

    def make_out_extra_move(
        self,
        mediator: Mediator,
        request: OutputNameLayoutRequest,
    ) -> OutExtraMove:
        schema = provide_schema(ExtraMoveAndPoliciesOverlay, mediator, request.loc_stack)
        if schema.extra_out == ExtraSkip():
            return None
        if callable(schema.extra_out):
            return ExtraExtract(schema.extra_out)
        return self._create_extra_targets(schema.extra_out)  # type: ignore[arg-type]

    def _get_extra_policy(self, schema: ExtraMoveAndPoliciesSchema) -> DictExtraPolicy:
        if schema.extra_in == ExtraSkip():
            return ExtraSkip()
        if schema.extra_in == ExtraForbid():
            return ExtraForbid()
        return ExtraCollect()

    def make_extra_policies(
        self,
        mediator: Mediator,
        request: InputNameLayoutRequest,
        paths_to_leaves: PathsTo[LeafInpCrown],
    ) -> PathsTo[DictExtraPolicy]:
        schema = provide_schema(ExtraMoveAndPoliciesOverlay, mediator, request.loc_stack)
        policy = self._get_extra_policy(schema)
        path_to_extra_policy: dict[KeyPath, DictExtraPolicy] = {
            (): policy,
        }
        for path, key in _paths_to_branches(paths_to_leaves):
            if policy == ExtraCollect() and isinstance(key, int):
                raise CannotProvide(
                    f"Cannot use collecting extra_in={schema.extra_in!r} with mapping to list",
                    is_terminal=True,
                    is_demonstrative=True,
                )
            path_to_extra_policy[path] = policy
        return path_to_extra_policy
