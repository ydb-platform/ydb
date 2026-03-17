from __future__ import annotations

import operator
import re
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, NamedTuple, Optional, Protocol, TypeVar, cast, runtime_checkable
from urllib.parse import ParseResult

from pydantic import BaseModel

from datamodel_code_generator.format import (
    DEFAULT_FORMATTERS,
    CodeFormatter,
    DatetimeClassType,
    Formatter,
    PythonVersion,
    PythonVersionMin,
)
from datamodel_code_generator.imports import (
    IMPORT_ANNOTATIONS,
    IMPORT_LITERAL,
    Import,
    Imports,
)
from datamodel_code_generator.model import dataclass as dataclass_model
from datamodel_code_generator.model import msgspec as msgspec_model
from datamodel_code_generator.model import pydantic as pydantic_model
from datamodel_code_generator.model import pydantic_v2 as pydantic_model_v2
from datamodel_code_generator.model.base import (
    ALL_MODEL,
    UNDEFINED,
    BaseClassDataType,
    ConstraintsBase,
    DataModel,
    DataModelFieldBase,
)
from datamodel_code_generator.model.enum import Enum, Member
from datamodel_code_generator.parser import DefaultPutDict, LiteralType
from datamodel_code_generator.reference import ModelResolver, Reference
from datamodel_code_generator.types import DataType, DataTypeManager, StrictTypes

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

SPECIAL_PATH_FORMAT: str = "#-datamodel-code-generator-#-{}-#-special-#"


def get_special_path(keyword: str, path: list[str]) -> list[str]:
    return [*path, SPECIAL_PATH_FORMAT.format(keyword)]


escape_characters = str.maketrans({
    "\u0000": r"\x00",  # Null byte
    "\\": r"\\",
    "'": r"\'",
    "\b": r"\b",
    "\f": r"\f",
    "\n": r"\n",
    "\r": r"\r",
    "\t": r"\t",
})


def to_hashable(item: Any) -> Any:
    if isinstance(
        item,
        (
            list,
            tuple,
        ),
    ):
        return tuple(sorted(to_hashable(i) for i in item))
    if isinstance(item, dict):
        return tuple(
            sorted(
                (
                    k,
                    to_hashable(v),
                )
                for k, v in item.items()
            )
        )
    if isinstance(item, set):  # pragma: no cover
        return frozenset(to_hashable(i) for i in item)
    if isinstance(item, BaseModel):
        return to_hashable(item.dict())
    if item is None:
        return ""
    return item


def dump_templates(templates: list[DataModel]) -> str:
    return "\n\n\n".join(str(m) for m in templates)


ReferenceMapSet = dict[str, set[str]]
SortedDataModels = dict[str, DataModel]

MAX_RECURSION_COUNT: int = sys.getrecursionlimit()


def sort_data_models(  # noqa: PLR0912
    unsorted_data_models: list[DataModel],
    sorted_data_models: SortedDataModels | None = None,
    require_update_action_models: list[str] | None = None,
    recursion_count: int = MAX_RECURSION_COUNT,
) -> tuple[list[DataModel], SortedDataModels, list[str]]:
    if sorted_data_models is None:
        sorted_data_models = OrderedDict()
    if require_update_action_models is None:
        require_update_action_models = []
    sorted_model_count: int = len(sorted_data_models)

    unresolved_references: list[DataModel] = []
    for model in unsorted_data_models:
        if not model.reference_classes:
            sorted_data_models[model.path] = model
        elif model.path in model.reference_classes and len(model.reference_classes) == 1:  # only self-referencing
            sorted_data_models[model.path] = model
            require_update_action_models.append(model.path)
        elif (
            not model.reference_classes - {model.path} - set(sorted_data_models)
        ):  # reference classes have been resolved
            sorted_data_models[model.path] = model
            if model.path in model.reference_classes:
                require_update_action_models.append(model.path)
        else:
            unresolved_references.append(model)
    if unresolved_references:
        if sorted_model_count != len(sorted_data_models) and recursion_count:
            try:
                return sort_data_models(
                    unresolved_references,
                    sorted_data_models,
                    require_update_action_models,
                    recursion_count - 1,
                )
            except RecursionError:  # pragma: no cover
                pass

        # sort on base_class dependency
        while True:
            ordered_models: list[tuple[int, DataModel]] = []
            unresolved_reference_model_names = [m.path for m in unresolved_references]
            for model in unresolved_references:
                indexes = [
                    unresolved_reference_model_names.index(b.reference.path)
                    for b in model.base_classes
                    if b.reference and b.reference.path in unresolved_reference_model_names
                ]
                if indexes:
                    ordered_models.append((
                        max(indexes),
                        model,
                    ))
                else:
                    ordered_models.append((
                        -1,
                        model,
                    ))
            sorted_unresolved_models = [m[1] for m in sorted(ordered_models, key=operator.itemgetter(0))]
            if sorted_unresolved_models == unresolved_references:
                break
            unresolved_references = sorted_unresolved_models

        # circular reference
        unsorted_data_model_names = set(unresolved_reference_model_names)
        for model in unresolved_references:
            unresolved_model = model.reference_classes - {model.path} - set(sorted_data_models)
            base_models = [getattr(s.reference, "path", None) for s in model.base_classes]
            update_action_parent = set(require_update_action_models).intersection(base_models)
            if not unresolved_model:
                sorted_data_models[model.path] = model
                if update_action_parent:
                    require_update_action_models.append(model.path)
                continue
            if not unresolved_model - unsorted_data_model_names:
                sorted_data_models[model.path] = model
                require_update_action_models.append(model.path)
                continue
            # unresolved
            unresolved_classes = ", ".join(
                f"[class: {item.path} references: {item.reference_classes}]" for item in unresolved_references
            )
            msg = f"A Parser can not resolve classes: {unresolved_classes}."
            raise Exception(msg)  # noqa: TRY002
    return unresolved_references, sorted_data_models, require_update_action_models


def relative(current_module: str, reference: str) -> tuple[str, str]:
    """Find relative module path."""

    current_module_path = current_module.split(".") if current_module else []
    *reference_path, name = reference.split(".")

    if current_module_path == reference_path:
        return "", ""

    i = 0
    for x, y in zip(current_module_path, reference_path):
        if x != y:
            break
        i += 1

    left = "." * (len(current_module_path) - i)
    right = ".".join(reference_path[i:])

    if not left:
        left = "."
    if not right:
        right = name
    elif "." in right:
        extra, right = right.rsplit(".", 1)
        left += extra

    return left, right


def exact_import(from_: str, import_: str, short_name: str) -> tuple[str, str]:
    if from_ == len(from_) * ".":
        # Prevents "from . import foo" becoming "from ..foo import Foo"
        # or "from .. import foo" becoming "from ...foo import Foo"
        # when our imported module has the same parent
        return f"{from_}{import_}", short_name
    return f"{from_}.{import_}", short_name


@runtime_checkable
class Child(Protocol):
    @property
    def parent(self) -> Any | None:
        raise NotImplementedError


T = TypeVar("T")


def get_most_of_parent(value: Any, type_: type[T] | None = None) -> T | None:
    if isinstance(value, Child) and (type_ is None or not isinstance(value, type_)):
        return get_most_of_parent(value.parent, type_)
    return value


def title_to_class_name(title: str) -> str:
    classname = re.sub(r"[^A-Za-z0-9]+", " ", title)
    return "".join(x for x in classname.title() if not x.isspace())


def _find_base_classes(model: DataModel) -> list[DataModel]:
    return [b.reference.source for b in model.base_classes if b.reference and isinstance(b.reference.source, DataModel)]


def _find_field(original_name: str, models: list[DataModel]) -> DataModelFieldBase | None:
    def _find_field_and_base_classes(
        model_: DataModel,
    ) -> tuple[DataModelFieldBase | None, list[DataModel]]:
        for field_ in model_.fields:
            if field_.original_name == original_name:
                return field_, []
        return None, _find_base_classes(model_)  # pragma: no cover

    for model in models:
        field, base_models = _find_field_and_base_classes(model)
        if field:
            return field
        models.extend(base_models)  # pragma: no cover  # noqa: B909

    return None  # pragma: no cover


def _copy_data_types(data_types: list[DataType]) -> list[DataType]:
    copied_data_types: list[DataType] = []
    for data_type_ in data_types:
        if data_type_.reference:
            copied_data_types.append(data_type_.__class__(reference=data_type_.reference))
        elif data_type_.data_types:  # pragma: no cover
            copied_data_type = data_type_.copy()
            copied_data_type.data_types = _copy_data_types(data_type_.data_types)
            copied_data_types.append(copied_data_type)
        else:
            copied_data_types.append(data_type_.copy())
    return copied_data_types


class Result(BaseModel):
    body: str
    source: Optional[Path] = None  # noqa: UP045


class Source(BaseModel):
    path: Path
    text: str

    @classmethod
    def from_path(cls, path: Path, base_path: Path, encoding: str) -> Source:
        return cls(
            path=path.relative_to(base_path),
            text=path.read_text(encoding=encoding),
        )


class Parser(ABC):
    def __init__(  # noqa: PLR0913, PLR0915
        self,
        source: str | Path | list[Path] | ParseResult,
        *,
        data_model_type: type[DataModel] = pydantic_model.BaseModel,
        data_model_root_type: type[DataModel] = pydantic_model.CustomRootType,
        data_type_manager_type: type[DataTypeManager] = pydantic_model.DataTypeManager,
        data_model_field_type: type[DataModelFieldBase] = pydantic_model.DataModelField,
        base_class: str | None = None,
        additional_imports: list[str] | None = None,
        custom_template_dir: Path | None = None,
        extra_template_data: defaultdict[str, dict[str, Any]] | None = None,
        target_python_version: PythonVersion = PythonVersionMin,
        dump_resolve_reference_action: Callable[[Iterable[str]], str] | None = None,
        validation: bool = False,
        field_constraints: bool = False,
        snake_case_field: bool = False,
        strip_default_none: bool = False,
        aliases: Mapping[str, str] | None = None,
        allow_population_by_field_name: bool = False,
        apply_default_values_for_required_fields: bool = False,
        allow_extra_fields: bool = False,
        extra_fields: str | None = None,
        force_optional_for_required_fields: bool = False,
        class_name: str | None = None,
        use_standard_collections: bool = False,
        base_path: Path | None = None,
        use_schema_description: bool = False,
        use_field_description: bool = False,
        use_default_kwarg: bool = False,
        reuse_model: bool = False,
        encoding: str = "utf-8",
        enum_field_as_literal: LiteralType | None = None,
        set_default_enum_member: bool = False,
        use_subclass_enum: bool = False,
        strict_nullable: bool = False,
        use_generic_container_types: bool = False,
        enable_faux_immutability: bool = False,
        remote_text_cache: DefaultPutDict[str, str] | None = None,
        disable_appending_item_suffix: bool = False,
        strict_types: Sequence[StrictTypes] | None = None,
        empty_enum_field_name: str | None = None,
        custom_class_name_generator: Callable[[str], str] | None = title_to_class_name,
        field_extra_keys: set[str] | None = None,
        field_include_all_keys: bool = False,
        field_extra_keys_without_x_prefix: set[str] | None = None,
        wrap_string_literal: bool | None = None,
        use_title_as_name: bool = False,
        use_operation_id_as_name: bool = False,
        use_unique_items_as_set: bool = False,
        http_headers: Sequence[tuple[str, str]] | None = None,
        http_ignore_tls: bool = False,
        use_annotated: bool = False,
        use_non_positive_negative_number_constrained_types: bool = False,
        original_field_name_delimiter: str | None = None,
        use_double_quotes: bool = False,
        use_union_operator: bool = False,
        allow_responses_without_content: bool = False,
        collapse_root_models: bool = False,
        special_field_name_prefix: str | None = None,
        remove_special_field_name_prefix: bool = False,
        capitalise_enum_members: bool = False,
        keep_model_order: bool = False,
        use_one_literal_as_default: bool = False,
        known_third_party: list[str] | None = None,
        custom_formatters: list[str] | None = None,
        custom_formatters_kwargs: dict[str, Any] | None = None,
        use_pendulum: bool = False,
        http_query_parameters: Sequence[tuple[str, str]] | None = None,
        treat_dot_as_module: bool = False,
        use_exact_imports: bool = False,
        default_field_extras: dict[str, Any] | None = None,
        target_datetime_class: DatetimeClassType | None = None,
        keyword_only: bool = False,
        frozen_dataclasses: bool = False,
        no_alias: bool = False,
        formatters: list[Formatter] = DEFAULT_FORMATTERS,
        parent_scoped_naming: bool = False,
    ) -> None:
        self.keyword_only = keyword_only
        self.frozen_dataclasses = frozen_dataclasses
        self.data_type_manager: DataTypeManager = data_type_manager_type(
            python_version=target_python_version,
            use_standard_collections=use_standard_collections,
            use_generic_container_types=use_generic_container_types,
            use_non_positive_negative_number_constrained_types=use_non_positive_negative_number_constrained_types,
            strict_types=strict_types,
            use_union_operator=use_union_operator,
            use_pendulum=use_pendulum,
            target_datetime_class=target_datetime_class,
            treat_dot_as_module=treat_dot_as_module,
        )
        self.data_model_type: type[DataModel] = data_model_type
        self.data_model_root_type: type[DataModel] = data_model_root_type
        self.data_model_field_type: type[DataModelFieldBase] = data_model_field_type

        self.imports: Imports = Imports(use_exact_imports)
        self.use_exact_imports: bool = use_exact_imports
        self._append_additional_imports(additional_imports=additional_imports)

        self.base_class: str | None = base_class
        self.target_python_version: PythonVersion = target_python_version
        self.results: list[DataModel] = []
        self.dump_resolve_reference_action: Callable[[Iterable[str]], str] | None = dump_resolve_reference_action
        self.validation: bool = validation
        self.field_constraints: bool = field_constraints
        self.snake_case_field: bool = snake_case_field
        self.strip_default_none: bool = strip_default_none
        self.apply_default_values_for_required_fields: bool = apply_default_values_for_required_fields
        self.force_optional_for_required_fields: bool = force_optional_for_required_fields
        self.use_schema_description: bool = use_schema_description
        self.use_field_description: bool = use_field_description
        self.use_default_kwarg: bool = use_default_kwarg
        self.reuse_model: bool = reuse_model
        self.encoding: str = encoding
        self.enum_field_as_literal: LiteralType | None = enum_field_as_literal
        self.set_default_enum_member: bool = set_default_enum_member
        self.use_subclass_enum: bool = use_subclass_enum
        self.strict_nullable: bool = strict_nullable
        self.use_generic_container_types: bool = use_generic_container_types
        self.use_union_operator: bool = use_union_operator
        self.enable_faux_immutability: bool = enable_faux_immutability
        self.custom_class_name_generator: Callable[[str], str] | None = custom_class_name_generator
        self.field_extra_keys: set[str] = field_extra_keys or set()
        self.field_extra_keys_without_x_prefix: set[str] = field_extra_keys_without_x_prefix or set()
        self.field_include_all_keys: bool = field_include_all_keys

        self.remote_text_cache: DefaultPutDict[str, str] = remote_text_cache or DefaultPutDict()
        self.current_source_path: Path | None = None
        self.use_title_as_name: bool = use_title_as_name
        self.use_operation_id_as_name: bool = use_operation_id_as_name
        self.use_unique_items_as_set: bool = use_unique_items_as_set

        if base_path:
            self.base_path = base_path
        elif isinstance(source, Path):
            self.base_path = source.absolute() if source.is_dir() else source.absolute().parent
        else:
            self.base_path = Path.cwd()

        self.source: str | Path | list[Path] | ParseResult = source
        self.custom_template_dir = custom_template_dir
        self.extra_template_data: defaultdict[str, Any] = extra_template_data or defaultdict(dict)

        if allow_population_by_field_name:
            self.extra_template_data[ALL_MODEL]["allow_population_by_field_name"] = True

        if allow_extra_fields:
            self.extra_template_data[ALL_MODEL]["allow_extra_fields"] = True

        if extra_fields:
            self.extra_template_data[ALL_MODEL]["extra_fields"] = extra_fields

        if enable_faux_immutability:
            self.extra_template_data[ALL_MODEL]["allow_mutation"] = False

        self.model_resolver = ModelResolver(
            base_url=source.geturl() if isinstance(source, ParseResult) else None,
            singular_name_suffix="" if disable_appending_item_suffix else None,
            aliases=aliases,
            empty_field_name=empty_enum_field_name,
            snake_case_field=snake_case_field,
            custom_class_name_generator=custom_class_name_generator,
            base_path=self.base_path,
            original_field_name_delimiter=original_field_name_delimiter,
            special_field_name_prefix=special_field_name_prefix,
            remove_special_field_name_prefix=remove_special_field_name_prefix,
            capitalise_enum_members=capitalise_enum_members,
            no_alias=no_alias,
            parent_scoped_naming=parent_scoped_naming,
        )
        self.class_name: str | None = class_name
        self.wrap_string_literal: bool | None = wrap_string_literal
        self.http_headers: Sequence[tuple[str, str]] | None = http_headers
        self.http_query_parameters: Sequence[tuple[str, str]] | None = http_query_parameters
        self.http_ignore_tls: bool = http_ignore_tls
        self.use_annotated: bool = use_annotated
        if self.use_annotated and not self.field_constraints:  # pragma: no cover
            msg = "`use_annotated=True` has to be used with `field_constraints=True`"
            raise Exception(msg)  # noqa: TRY002
        self.use_non_positive_negative_number_constrained_types = use_non_positive_negative_number_constrained_types
        self.use_double_quotes = use_double_quotes
        self.allow_responses_without_content = allow_responses_without_content
        self.collapse_root_models = collapse_root_models
        self.capitalise_enum_members = capitalise_enum_members
        self.keep_model_order = keep_model_order
        self.use_one_literal_as_default = use_one_literal_as_default
        self.known_third_party = known_third_party
        self.custom_formatter = custom_formatters
        self.custom_formatters_kwargs = custom_formatters_kwargs
        self.treat_dot_as_module = treat_dot_as_module
        self.default_field_extras: dict[str, Any] | None = default_field_extras
        self.formatters: list[Formatter] = formatters

    @property
    def iter_source(self) -> Iterator[Source]:
        if isinstance(self.source, str):
            yield Source(path=Path(), text=self.source)
        elif isinstance(self.source, Path):  # pragma: no cover
            if self.source.is_dir():
                for path in sorted(self.source.rglob("*"), key=lambda p: p.name):
                    if path.is_file():
                        yield Source.from_path(path, self.base_path, self.encoding)
            else:
                yield Source.from_path(self.source, self.base_path, self.encoding)
        elif isinstance(self.source, list):  # pragma: no cover
            for path in self.source:
                yield Source.from_path(path, self.base_path, self.encoding)
        else:
            yield Source(
                path=Path(self.source.path),
                text=self.remote_text_cache.get_or_put(self.source.geturl(), default_factory=self._get_text_from_url),
            )

    def _append_additional_imports(self, additional_imports: list[str] | None) -> None:
        if additional_imports is None:
            additional_imports = []

        for additional_import_string in additional_imports:
            if additional_import_string is None:
                continue
            new_import = Import.from_full_path(additional_import_string)
            self.imports.append(new_import)

    def _get_text_from_url(self, url: str) -> str:
        from datamodel_code_generator.http import get_body  # noqa: PLC0415

        return self.remote_text_cache.get_or_put(
            url,
            default_factory=lambda url_: get_body(  # noqa: ARG005
                url, self.http_headers, self.http_ignore_tls, self.http_query_parameters
            ),
        )

    @classmethod
    def get_url_path_parts(cls, url: ParseResult) -> list[str]:
        return [
            f"{url.scheme}://{url.hostname}",
            *url.path.split("/")[1:],
        ]

    @property
    def data_type(self) -> type[DataType]:
        return self.data_type_manager.data_type

    @abstractmethod
    def parse_raw(self) -> None:
        raise NotImplementedError

    def __delete_duplicate_models(self, models: list[DataModel]) -> None:  # noqa: PLR0912
        model_class_names: dict[str, DataModel] = {}
        model_to_duplicate_models: defaultdict[DataModel, list[DataModel]] = defaultdict(list)
        for model in models.copy():  # noqa: PLR1702
            if isinstance(model, self.data_model_root_type):
                root_data_type = model.fields[0].data_type

                # backward compatible
                # Remove duplicated root model
                if (
                    root_data_type.reference
                    and not root_data_type.is_dict
                    and not root_data_type.is_list
                    and root_data_type.reference.source in models
                    and root_data_type.reference.name
                    == self.model_resolver.get_class_name(model.reference.original_name, unique=False).name
                ):
                    # Replace referenced duplicate model to original model
                    for child in model.reference.children[:]:
                        child.replace_reference(root_data_type.reference)
                    models.remove(model)
                    for data_type in model.all_data_types:
                        if data_type.reference:
                            data_type.remove_reference()
                    continue

                #  Custom root model can't be inherited on restriction of Pydantic
                for child in model.reference.children:
                    # inheritance model
                    if isinstance(child, DataModel):
                        for base_class in child.base_classes[:]:
                            if base_class.reference == model.reference:
                                child.base_classes.remove(base_class)
                        if not child.base_classes:  # pragma: no cover
                            child.set_base_class()

            class_name = model.duplicate_class_name or model.class_name
            if class_name in model_class_names:
                model_key = tuple(
                    to_hashable(v)
                    for v in (
                        model.render(class_name=model.duplicate_class_name),
                        model.imports,
                    )
                )
                original_model = model_class_names[class_name]
                original_model_key = tuple(
                    to_hashable(v)
                    for v in (
                        original_model.render(class_name=original_model.duplicate_class_name),
                        original_model.imports,
                    )
                )
                if model_key == original_model_key:
                    model_to_duplicate_models[original_model].append(model)
                    continue
            model_class_names[class_name] = model
        for model, duplicate_models in model_to_duplicate_models.items():
            for duplicate_model in duplicate_models:
                for child in duplicate_model.reference.children[:]:
                    if isinstance(child, DataType):
                        child.replace_reference(model.reference)
                    # simplify if introduce duplicate base classes
                    if isinstance(child, DataModel):
                        child.base_classes = list(
                            {f"{c.module_name}.{c.type_hint}": c for c in child.base_classes}.values()
                        )
                models.remove(duplicate_model)

    @classmethod
    def __replace_duplicate_name_in_module(cls, models: list[DataModel]) -> None:
        scoped_model_resolver = ModelResolver(
            exclude_names={i.alias or i.import_ for m in models for i in m.imports},
            duplicate_name_suffix="Model",
        )

        model_names: dict[str, DataModel] = {}
        for model in models:
            class_name: str = model.class_name
            generated_name: str = scoped_model_resolver.add([model.path], class_name, unique=True, class_name=True).name
            if class_name != generated_name:
                model.class_name = generated_name
            model_names[model.class_name] = model

        for model in models:
            duplicate_name = model.duplicate_class_name
            # check only first desired name
            if duplicate_name and duplicate_name not in model_names:
                del model_names[model.class_name]
                model.class_name = duplicate_name
                model_names[duplicate_name] = model

    def __change_from_import(
        self,
        models: list[DataModel],
        imports: Imports,
        scoped_model_resolver: ModelResolver,
        init: bool,  # noqa: FBT001
    ) -> None:
        for model in models:
            scoped_model_resolver.add([model.path], model.class_name)
        for model in models:
            before_import = model.imports
            imports.append(before_import)
            for data_type in model.all_data_types:
                # To change from/import

                if not data_type.reference or data_type.reference.source in models:
                    # No need to import non-reference model.
                    # Or, Referenced model is in the same file. we don't need to import the model
                    continue

                if isinstance(data_type, BaseClassDataType):
                    left, right = relative(model.module_name, data_type.full_name)
                    from_ = f"{left}{right}" if left.endswith(".") else f"{left}.{right}"
                    import_ = data_type.reference.short_name
                    full_path = from_, import_
                else:
                    from_, import_ = full_path = relative(model.module_name, data_type.full_name)
                    if imports.use_exact:  # pragma: no cover
                        from_, import_ = exact_import(from_, import_, data_type.reference.short_name)
                    import_ = import_.replace("-", "_")
                    if (
                        len(model.module_path) > 1
                        and model.module_path[-1].count(".") > 0
                        and not self.treat_dot_as_module
                    ):
                        rel_path_depth = model.module_path[-1].count(".")
                        from_ = from_[rel_path_depth:]

                alias = scoped_model_resolver.add(full_path, import_).name

                name = data_type.reference.short_name
                if from_ and import_ and alias != name:
                    data_type.alias = alias if data_type.reference.short_name == import_ else f"{alias}.{name}"

                if init:
                    from_ = "." + from_
                imports.append(
                    Import(
                        from_=from_,
                        import_=import_,
                        alias=alias,
                        reference_path=data_type.reference.path,
                    ),
                )
            after_import = model.imports
            if before_import != after_import:
                imports.append(after_import)

    @classmethod
    def __extract_inherited_enum(cls, models: list[DataModel]) -> None:
        for model in models.copy():
            if model.fields:
                continue
            enums: list[Enum] = []
            for base_model in model.base_classes:
                if not base_model.reference:
                    continue
                source_model = base_model.reference.source
                if isinstance(source_model, Enum):
                    enums.append(source_model)
            if enums:
                models.insert(
                    models.index(model),
                    enums[0].__class__(
                        fields=[f for e in enums for f in e.fields],
                        description=model.description,
                        reference=model.reference,
                    ),
                )
                models.remove(model)

    def __apply_discriminator_type(  # noqa: PLR0912, PLR0915
        self,
        models: list[DataModel],
        imports: Imports,
    ) -> None:
        for model in models:  # noqa: PLR1702
            for field in model.fields:
                discriminator = field.extras.get("discriminator")
                if not discriminator or not isinstance(discriminator, dict):
                    continue
                property_name = discriminator.get("propertyName")
                if not property_name:  # pragma: no cover
                    continue
                field_name, alias = self.model_resolver.get_valid_field_name_and_alias(field_name=property_name)
                discriminator["propertyName"] = field_name
                mapping = discriminator.get("mapping", {})
                for data_type in field.data_type.data_types:
                    if not data_type.reference:  # pragma: no cover
                        continue
                    discriminator_model = data_type.reference.source

                    if not isinstance(  # pragma: no cover
                        discriminator_model,
                        (
                            pydantic_model.BaseModel,
                            pydantic_model_v2.BaseModel,
                            dataclass_model.DataClass,
                            msgspec_model.Struct,
                        ),
                    ):
                        continue  # pragma: no cover

                    type_names: list[str] = []

                    def check_paths(
                        model: pydantic_model.BaseModel | pydantic_model_v2.BaseModel | Reference,
                        mapping: dict[str, str],
                        type_names: list[str] = type_names,
                    ) -> None:
                        """Helper function to validate paths for a given model."""
                        for name, path in mapping.items():
                            if (model.path.split("#/")[-1] != path.split("#/")[-1]) and (
                                path.startswith("#/") or model.path[:-1] != path.split("/")[-1]
                            ):
                                t_path = path[str(path).find("/") + 1 :]
                                t_disc = model.path[: str(model.path).find("#")].lstrip("../")  # noqa: B005
                                t_disc_2 = "/".join(t_disc.split("/")[1:])
                                if t_path not in {t_disc, t_disc_2}:
                                    continue
                            type_names.append(name)

                    # First try to get the discriminator value from the const field
                    for discriminator_field in discriminator_model.fields:
                        if field_name not in {discriminator_field.original_name, discriminator_field.name}:
                            continue
                        if discriminator_field.extras.get("const"):
                            type_names = [discriminator_field.extras["const"]]
                            break

                    # If no const value found, try to get it from the mapping
                    if not type_names:
                        # Check the main discriminator model path
                        if mapping:
                            check_paths(discriminator_model, mapping)  # pyright: ignore[reportArgumentType]

                            # Check the base_classes if they exist
                            if len(type_names) == 0:
                                for base_class in discriminator_model.base_classes:
                                    check_paths(base_class.reference, mapping)  # pyright: ignore[reportArgumentType]
                        else:
                            type_names = [discriminator_model.path.split("/")[-1]]

                    if not type_names:  # pragma: no cover
                        msg = f"Discriminator type is not found. {data_type.reference.path}"
                        raise RuntimeError(msg)

                    has_one_literal = False
                    for discriminator_field in discriminator_model.fields:
                        if field_name not in {discriminator_field.original_name, discriminator_field.name}:
                            continue
                        literals = discriminator_field.data_type.literals
                        if len(literals) == 1 and literals[0] == (type_names[0] if type_names else None):
                            has_one_literal = True
                            if isinstance(discriminator_model, msgspec_model.Struct):  # pragma: no cover
                                discriminator_model.add_base_class_kwarg("tag_field", f"'{field_name}'")
                                discriminator_model.add_base_class_kwarg("tag", discriminator_field.represented_default)
                                discriminator_field.extras["is_classvar"] = True
                            # Found the discriminator field, no need to keep looking
                            break
                        for field_data_type in discriminator_field.data_type.all_data_types:
                            if field_data_type.reference:  # pragma: no cover
                                field_data_type.remove_reference()
                        discriminator_field.data_type = self.data_type(literals=type_names)
                        discriminator_field.data_type.parent = discriminator_field
                        discriminator_field.required = True
                        imports.append(discriminator_field.imports)
                        has_one_literal = True
                    if not has_one_literal:
                        discriminator_model.fields.append(
                            self.data_model_field_type(
                                name=field_name,
                                data_type=self.data_type(literals=type_names),
                                required=True,
                                alias=alias,
                            )
                        )
                    has_imported_literal = any(import_ == IMPORT_LITERAL for import_ in imports)
                    if has_imported_literal:  # pragma: no cover
                        imports.append(IMPORT_LITERAL)

    @classmethod
    def _create_set_from_list(cls, data_type: DataType) -> DataType | None:
        if data_type.is_list:
            new_data_type = data_type.copy()
            new_data_type.is_list = False
            new_data_type.is_set = True
            for data_type_ in new_data_type.data_types:
                data_type_.parent = new_data_type
            return new_data_type
        if data_type.data_types:  # pragma: no cover
            for index, nested_data_type in enumerate(data_type.data_types[:]):
                set_data_type = cls._create_set_from_list(nested_data_type)
                if set_data_type:  # pragma: no cover
                    data_type.data_types[index] = set_data_type
            return data_type
        return None  # pragma: no cover

    def __replace_unique_list_to_set(self, models: list[DataModel]) -> None:
        for model in models:
            for model_field in model.fields:
                if not self.use_unique_items_as_set:
                    continue

                if not (model_field.constraints and model_field.constraints.unique_items):
                    continue
                set_data_type = self._create_set_from_list(model_field.data_type)
                if set_data_type:  # pragma: no cover
                    model_field.data_type.parent = None
                    model_field.data_type = set_data_type
                    set_data_type.parent = model_field

    @classmethod
    def __set_reference_default_value_to_field(cls, models: list[DataModel]) -> None:
        for model in models:
            for model_field in model.fields:
                if not model_field.data_type.reference or model_field.has_default:
                    continue
                if (
                    isinstance(model_field.data_type.reference.source, DataModel)
                    and model_field.data_type.reference.source.default != UNDEFINED
                ):
                    # pragma: no cover
                    model_field.default = model_field.data_type.reference.source.default

    def __reuse_model(self, models: list[DataModel], require_update_action_models: list[str]) -> None:
        if not self.reuse_model:
            return
        model_cache: dict[tuple[str, ...], Reference] = {}
        duplicates = []
        for model in models.copy():
            model_key = tuple(to_hashable(v) for v in (model.render(class_name="M"), model.imports))
            cached_model_reference = model_cache.get(model_key)
            if cached_model_reference:
                if isinstance(model, Enum):
                    for child in model.reference.children[:]:
                        # child is resolved data_type by reference
                        data_model = get_most_of_parent(child)
                        # TODO: replace reference in all modules
                        if data_model in models:  # pragma: no cover
                            child.replace_reference(cached_model_reference)
                    duplicates.append(model)
                else:
                    index = models.index(model)
                    inherited_model = model.__class__(
                        fields=[],
                        base_classes=[cached_model_reference],
                        description=model.description,
                        reference=Reference(
                            name=model.name,
                            path=model.reference.path + "/reuse",
                        ),
                        custom_template_dir=model._custom_template_dir,  # noqa: SLF001
                    )
                    if cached_model_reference.path in require_update_action_models:
                        require_update_action_models.append(inherited_model.path)
                    models.insert(index, inherited_model)
                    models.remove(model)

            else:
                model_cache[model_key] = model.reference

        for duplicate in duplicates:
            models.remove(duplicate)

    def __collapse_root_models(  # noqa: PLR0912
        self,
        models: list[DataModel],
        unused_models: list[DataModel],
        imports: Imports,
        scoped_model_resolver: ModelResolver,
    ) -> None:
        if not self.collapse_root_models:
            return

        for model in models:  # noqa: PLR1702
            for model_field in model.fields:
                for data_type in model_field.data_type.all_data_types:
                    reference = data_type.reference
                    if not reference or not isinstance(reference.source, self.data_model_root_type):
                        # If the data type is not a reference, we can't collapse it.
                        # If it's a reference to a root model type, we don't do anything.
                        continue

                    # Use root-type as model_field type
                    root_type_model = reference.source
                    root_type_field = root_type_model.fields[0]

                    if (
                        self.field_constraints
                        and isinstance(root_type_field.constraints, ConstraintsBase)
                        and root_type_field.constraints.has_constraints
                        and any(d for d in model_field.data_type.all_data_types if d.is_dict or d.is_union or d.is_list)
                    ):
                        continue  # pragma: no cover

                    if root_type_field.data_type.reference:
                        # If the root type field is a reference, we aren't able to collapse it yet.
                        continue

                    # set copied data_type
                    copied_data_type = root_type_field.data_type.copy()
                    if isinstance(data_type.parent, self.data_model_field_type):
                        # for field
                        # override empty field by root-type field
                        model_field.extras = {
                            **root_type_field.extras,
                            **model_field.extras,
                        }
                        model_field.process_const()

                        if self.field_constraints:
                            model_field.constraints = ConstraintsBase.merge_constraints(
                                root_type_field.constraints, model_field.constraints
                            )

                        data_type.parent.data_type = copied_data_type

                    elif data_type.parent is not None and data_type.parent.is_list:
                        if self.field_constraints:
                            model_field.constraints = ConstraintsBase.merge_constraints(
                                root_type_field.constraints, model_field.constraints
                            )
                        if (
                            isinstance(
                                root_type_field,
                                pydantic_model.DataModelField,
                            )
                            and not model_field.extras.get("discriminator")
                            and not any(t.is_list for t in model_field.data_type.data_types)
                        ):
                            discriminator = root_type_field.extras.get("discriminator")
                            if discriminator:
                                model_field.extras["discriminator"] = discriminator
                        assert isinstance(data_type.parent, DataType)
                        data_type.parent.data_types.remove(data_type)  # pragma: no cover
                        data_type.parent.data_types.append(copied_data_type)

                    elif isinstance(data_type.parent, DataType):
                        # for data_type
                        data_type_id = id(data_type)
                        data_type.parent.data_types = [
                            d for d in (*data_type.parent.data_types, copied_data_type) if id(d) != data_type_id
                        ]
                    else:  # pragma: no cover
                        continue

                    for d in root_type_field.data_type.data_types:
                        if d.reference is None:
                            continue
                        from_, import_ = full_path = relative(model.module_name, d.full_name)
                        if from_ and import_:
                            alias = scoped_model_resolver.add(full_path, import_)
                            d.alias = (
                                alias.name
                                if d.reference.short_name == import_
                                else f"{alias.name}.{d.reference.short_name}"
                            )
                            imports.append([
                                Import(
                                    from_=from_,
                                    import_=import_,
                                    alias=alias.name,
                                    reference_path=d.reference.path,
                                )
                            ])

                    original_field = get_most_of_parent(data_type, DataModelFieldBase)
                    if original_field:  # pragma: no cover
                        # TODO: Improve detection of reference type
                        imports.append(original_field.imports)

                    data_type.remove_reference()

                    root_type_model.reference.children = [
                        c for c in root_type_model.reference.children if getattr(c, "parent", None)
                    ]

                    imports.remove_referenced_imports(root_type_model.path)
                    if not root_type_model.reference.children:
                        unused_models.append(root_type_model)

    def __set_default_enum_member(  # noqa: PLR0912
        self,
        models: list[DataModel],
    ) -> None:
        if not self.set_default_enum_member:
            return
        for model in models:  # noqa: PLR1702
            for model_field in model.fields:
                if not model_field.default:
                    continue
                for data_type in model_field.data_type.all_data_types:
                    if data_type.reference and isinstance(data_type.reference.source, Enum):  # pragma: no cover
                        if isinstance(model_field.default, list):
                            enum_member: list[Member] | (Member | None) = [
                                e for e in (data_type.reference.source.find_member(d) for d in model_field.default) if e
                            ]
                        else:
                            enum_member = data_type.reference.source.find_member(model_field.default)
                        if not enum_member:
                            continue
                        model_field.default = enum_member
                        if data_type.alias:
                            if isinstance(enum_member, list):
                                for enum_member_ in enum_member:
                                    enum_member_.alias = data_type.alias
                            else:
                                enum_member.alias = data_type.alias

    def __override_required_field(
        self,
        models: list[DataModel],
    ) -> None:
        for model in models:
            if isinstance(model, (Enum, self.data_model_root_type)):
                continue
            for index, model_field in enumerate(model.fields[:]):
                data_type = model_field.data_type
                if (
                    not model_field.original_name  # noqa: PLR0916
                    or data_type.data_types
                    or data_type.reference
                    or data_type.type
                    or data_type.literals
                    or data_type.dict_key
                ):
                    continue

                original_field = _find_field(model_field.original_name, _find_base_classes(model))
                if not original_field:  # pragma: no cover
                    model.fields.remove(model_field)
                    continue
                copied_original_field = original_field.copy()
                if original_field.data_type.reference:
                    data_type = self.data_type_manager.data_type(
                        reference=original_field.data_type.reference,
                    )
                elif original_field.data_type.data_types:
                    data_type = original_field.data_type.copy()
                    data_type.data_types = _copy_data_types(original_field.data_type.data_types)
                    for data_type_ in data_type.data_types:
                        data_type_.parent = data_type
                else:
                    data_type = original_field.data_type.copy()
                data_type.parent = copied_original_field
                copied_original_field.data_type = data_type
                copied_original_field.parent = model
                copied_original_field.required = True
                model.fields.insert(index, copied_original_field)
                model.fields.remove(model_field)

    def __sort_models(
        self,
        models: list[DataModel],
        imports: Imports,
    ) -> None:
        if not self.keep_model_order:
            return

        models.sort(key=lambda x: x.class_name)

        imported = {i for v in imports.values() for i in v}
        model_class_name_baseclasses: dict[DataModel, tuple[str, set[str]]] = {}
        for model in models:
            class_name = model.class_name
            model_class_name_baseclasses[model] = (
                class_name,
                {b.type_hint for b in model.base_classes if b.reference} - {class_name},
            )

        changed: bool = True
        while changed:
            changed = False
            resolved = imported.copy()
            for i in range(len(models) - 1):
                model = models[i]
                class_name, baseclasses = model_class_name_baseclasses[model]
                if not baseclasses - resolved:
                    resolved.add(class_name)
                    continue
                models[i], models[i + 1] = models[i + 1], model
                changed = True

    def __change_field_name(
        self,
        models: list[DataModel],
    ) -> None:
        if self.data_model_type != pydantic_model_v2.BaseModel:
            return
        for model in models:
            if "Enum" in model.base_class:
                continue

            for field in model.fields:
                filed_name = field.name
                filed_name_resolver = ModelResolver(snake_case_field=self.snake_case_field, remove_suffix_number=True)
                for data_type in field.data_type.all_data_types:
                    if data_type.reference:
                        filed_name_resolver.exclude_names.add(data_type.reference.short_name)
                new_filed_name = filed_name_resolver.add(["field"], cast("str", filed_name)).name
                if filed_name != new_filed_name:
                    field.alias = filed_name
                    field.name = new_filed_name

    def __set_one_literal_on_default(self, models: list[DataModel]) -> None:
        if not self.use_one_literal_as_default:
            return
        for model in models:
            for model_field in model.fields:
                if not model_field.required or len(model_field.data_type.literals) != 1:
                    continue
                model_field.default = model_field.data_type.literals[0]
                model_field.required = False
                if model_field.nullable is not True:  # pragma: no cover
                    model_field.nullable = False

    @classmethod
    def __postprocess_result_modules(cls, results: dict[tuple[str, ...], Result]) -> dict[tuple[str, ...], Result]:
        def process(input_tuple: tuple[str, ...]) -> tuple[str, ...]:
            r = []
            for item in input_tuple:
                p = item.split(".")
                if len(p) > 1:
                    r.extend(p[:-1])
                    r.append(p[-1])
                else:
                    r.append(item)

            r = [*r[:-2], f"{r[-2]}.{r[-1]}"]
            return tuple(r)

        results = {process(k): v for k, v in results.items()}

        init_result = next(v for k, v in results.items() if k[-1] == "__init__.py")
        folders = {t[:-1] if t[-1].endswith(".py") else t for t in results}
        for folder in folders:
            for i in range(len(folder)):
                subfolder = folder[: i + 1]
                init_file = (*subfolder, "__init__.py")
                results.update({init_file: init_result})
        return results

    def __change_imported_model_name(  # noqa: PLR6301
        self,
        models: list[DataModel],
        imports: Imports,
        scoped_model_resolver: ModelResolver,
    ) -> None:
        imported_names = {
            imports.alias[from_][i] if i in imports.alias[from_] and i != imports.alias[from_][i] else i
            for from_, import_ in imports.items()
            for i in import_
        }
        for model in models:
            if model.class_name not in imported_names:  # pragma: no cover
                continue

            model.reference.name = scoped_model_resolver.add(  # pragma: no cover
                path=get_special_path("imported_name", model.path.split("/")),
                original_name=model.reference.name,
                unique=True,
                class_name=True,
            ).name

    def __alias_shadowed_imports(  # noqa: PLR6301
        self,
        models: list[DataModel],
        all_model_field_names: set[str],
    ) -> None:
        for model in models:
            for model_field in model.fields:
                if (
                    model_field.data_type.type in all_model_field_names
                    and model_field.data_type.type == model_field.name
                ):
                    alias = model_field.data_type.type + "_aliased"
                    model_field.data_type.type = alias
                    if model_field.data_type.import_:  # pragma: no cover
                        model_field.data_type.import_ = Import(
                            from_=model_field.data_type.import_.from_,
                            import_=model_field.data_type.import_.import_,
                            alias=alias,
                            reference_path=model_field.data_type.import_.reference_path,
                        )

    def parse(  # noqa: PLR0912, PLR0914, PLR0915
        self,
        with_import: bool | None = True,  # noqa: FBT001, FBT002
        format_: bool | None = True,  # noqa: FBT001, FBT002
        settings_path: Path | None = None,
        disable_future_imports: bool = False,  # noqa: FBT001, FBT002
    ) -> str | dict[tuple[str, ...], Result]:
        self.parse_raw()

        if with_import and not disable_future_imports:
            self.imports.append(IMPORT_ANNOTATIONS)

        if format_:
            code_formatter: CodeFormatter | None = CodeFormatter(
                self.target_python_version,
                settings_path,
                self.wrap_string_literal,
                skip_string_normalization=not self.use_double_quotes,
                known_third_party=self.known_third_party,
                custom_formatters=self.custom_formatter,
                custom_formatters_kwargs=self.custom_formatters_kwargs,
                encoding=self.encoding,
                formatters=self.formatters,
            )
        else:
            code_formatter = None

        _, sorted_data_models, require_update_action_models = sort_data_models(self.results)

        results: dict[tuple[str, ...], Result] = {}

        def module_key(data_model: DataModel) -> tuple[str, ...]:
            return tuple(data_model.module_path)

        def sort_key(data_model: DataModel) -> tuple[int, tuple[str, ...]]:
            return (len(data_model.module_path), tuple(data_model.module_path))

        # process in reverse order to correctly establish module levels
        grouped_models = groupby(
            sorted(sorted_data_models.values(), key=sort_key, reverse=True),
            key=module_key,
        )

        module_models: list[tuple[tuple[str, ...], list[DataModel]]] = []
        unused_models: list[DataModel] = []
        model_to_module_models: dict[DataModel, tuple[tuple[str, ...], list[DataModel]]] = {}
        module_to_import: dict[tuple[str, ...], Imports] = {}

        previous_module: tuple[str, ...] = ()
        for module, models in ((k, [*v]) for k, v in grouped_models):
            for model in models:
                model_to_module_models[model] = module, models
            self.__delete_duplicate_models(models)
            self.__replace_duplicate_name_in_module(models)
            if len(previous_module) - len(module) > 1:
                module_models.extend(
                    (
                        previous_module[:parts],
                        [],
                    )
                    for parts in range(len(previous_module) - 1, len(module), -1)
                )
            module_models.append((
                module,
                models,
            ))
            previous_module = module

        class Processed(NamedTuple):
            module: tuple[str, ...]
            models: list[DataModel]
            init: bool
            imports: Imports
            scoped_model_resolver: ModelResolver

        processed_models: list[Processed] = []

        for module_, models in module_models:
            imports = module_to_import[module_] = Imports(self.use_exact_imports)
            init = False
            if module_:
                parent = (*module_[:-1], "__init__.py")
                if parent not in results:
                    results[parent] = Result(body="")
                if (*module_, "__init__.py") in results:
                    module = (*module_, "__init__.py")
                    init = True
                else:
                    module = tuple(part.replace("-", "_") for part in (*module_[:-1], f"{module_[-1]}.py"))
            else:
                module = ("__init__.py",)

            all_module_fields = {field.name for model in models for field in model.fields if field.name is not None}
            scoped_model_resolver = ModelResolver(exclude_names=all_module_fields)

            self.__alias_shadowed_imports(models, all_module_fields)
            self.__override_required_field(models)
            self.__replace_unique_list_to_set(models)
            self.__change_from_import(models, imports, scoped_model_resolver, init)
            self.__extract_inherited_enum(models)
            self.__set_reference_default_value_to_field(models)
            self.__reuse_model(models, require_update_action_models)
            self.__collapse_root_models(models, unused_models, imports, scoped_model_resolver)
            self.__set_default_enum_member(models)
            self.__sort_models(models, imports)
            self.__change_field_name(models)
            self.__apply_discriminator_type(models, imports)
            self.__set_one_literal_on_default(models)

            processed_models.append(Processed(module, models, init, imports, scoped_model_resolver))

        for processed_model in processed_models:
            for model in processed_model.models:
                processed_model.imports.append(model.imports)

        for unused_model in unused_models:
            module, models = model_to_module_models[unused_model]
            if unused_model in models:  # pragma: no cover
                imports = module_to_import[module]
                imports.remove(unused_model.imports)
                models.remove(unused_model)

        for processed_model in processed_models:
            # postprocess imports to remove unused imports.
            model_code = str("\n".join([str(m) for m in processed_model.models]))
            unused_imports = [
                (from_, import_)
                for from_, imports_ in processed_model.imports.items()
                for import_ in imports_
                if import_ not in model_code
            ]
            for from_, import_ in unused_imports:
                processed_model.imports.remove(Import(from_=from_, import_=import_))

        for module, models, init, imports, scoped_model_resolver in processed_models:  # noqa: B007
            # process after removing unused models
            self.__change_imported_model_name(models, imports, scoped_model_resolver)

        for module, models, init, imports, scoped_model_resolver in processed_models:  # noqa: B007
            result: list[str] = []
            if models:
                if with_import:
                    result += [str(self.imports), str(imports), "\n"]

                code = dump_templates(models)
                result += [code]

                if self.dump_resolve_reference_action is not None:
                    result += [
                        "\n",
                        self.dump_resolve_reference_action(
                            m.reference.short_name for m in models if m.path in require_update_action_models
                        ),
                    ]
            if not result and not init:
                continue
            body = "\n".join(result)
            if code_formatter:
                body = code_formatter.format_code(body)

            results[module] = Result(body=body, source=models[0].file_path if models else None)

        # retain existing behaviour
        if [*results] == [("__init__.py",)]:
            return results["__init__.py",].body

        results = {tuple(i.replace("-", "_") for i in k): v for k, v in results.items()}
        return (
            self.__postprocess_result_modules(results)
            if self.treat_dot_as_module
            else {
                tuple((part[: part.rfind(".")].replace(".", "_") + part[part.rfind(".") :]) for part in k): v
                for k, v in results.items()
            }
        )
