from collections.abc import Iterable, Mapping, Sequence
from dataclasses import fields
from textwrap import dedent, indent
from typing import Any, Callable, Optional, TypeVar, Union

from adaptix import TypeHint

from ...utils import Omittable, Omitted
from .definitions import JSONSchema, RefSource, ResolvedJSONSchema
from .schema_model import JSONNumeric, JSONObject, JSONSchemaBuiltinFormat, JSONSchemaT, JSONSchemaType, JSONValue, RefT

_non_generic_fields_types = [
    Omittable[Union[JSONSchemaType, Sequence[JSONSchemaType]]],  # type: ignore[misc]
    Omittable[Union[JSONSchemaBuiltinFormat, str]],  # type: ignore[misc]
    Omittable[JSONNumeric],  # type: ignore[misc]
    Omittable[int],  # type: ignore[misc]
    Omittable[str],  # type: ignore[misc]
    Omittable[bool],  # type: ignore[misc]
    Omittable[Sequence[str]],  # type: ignore[misc]
    Omittable[JSONObject[Sequence[str]]],  # type: ignore[misc]
    Omittable[JSONValue],  # type: ignore[misc]
    Omittable[Sequence[JSONValue]],  # type: ignore[misc]
    Omittable[JSONObject[bool]],  # type: ignore[misc]
    JSONObject[JSONValue],
]

_base_json_schema_templates = {
    **{tp: None for tp in _non_generic_fields_types},
    Omittable[Sequence[JSONSchemaT]]: dedent(  # type: ignore[misc, valid-type]
        """
        if __value__ != Omitted():
            for item in __value__:
                yield from __traverser__(item)
        """,
    ),
    Omittable[JSONSchemaT]: dedent(  # type: ignore[misc, valid-type]
        """
        if __value__ != Omitted():
            yield from __traverser__(__value__)
        """,
    ),
    Omittable[JSONObject[JSONSchemaT]]: dedent(  # type: ignore[misc, valid-type]
        """
        if __value__ != Omitted():
            for item in __value__.values():
                yield from __traverser__(item)
        """,
    ),
    Omittable[RefT]: None,  # type: ignore[misc, valid-type]
}
_json_schema_templates = {
    **_base_json_schema_templates,
    Omittable[RefT]: dedent(  # type: ignore[misc, valid-type]
        """
        if __value__ != Omitted():
            yield from __traverser__(__value__.json_schema)
        """,
    ),
}


def _generate_json_schema_traverser(
    function_name: str,
    file_name: str,
    templates: Mapping[TypeHint, Optional[str]],
    cls: type[JSONSchemaT],
) -> Callable[[JSONSchemaT], Iterable[JSONSchemaT]]:
    result = []
    for fld in fields(cls):  # type: ignore[arg-type]
        template = templates[fld.type]
        if template is None:
            continue

        result.append(
            template
            .replace("__value__", f"obj.{fld.name}")
            .replace("__traverser__", function_name)
            .strip("\n"),
        )

    module_code = dedent(
        f"""
        def {function_name}(obj, /):
            if isinstance(obj, bool):
                return

            yield obj

        """,
    ) + "\n\n".join(indent(item, " " * 4) for item in result)
    namespace: dict[str, Any] = {"Omitted": Omitted}
    exec(compile(module_code, file_name, "exec"), namespace, namespace)  # noqa: S102
    return namespace[function_name]


traverse_json_schema = _generate_json_schema_traverser(
    function_name="traverse_json_schema",
    file_name="<traverse_json_schema generation>",
    templates=_json_schema_templates,
    cls=JSONSchema,
)
traverse_resolved_json_schema = _generate_json_schema_traverser(
    function_name="traverse_resolved_json_schema",
    file_name="<traverse_resolved_json_schema generation>",
    templates=_base_json_schema_templates,
    cls=ResolvedJSONSchema,
)


_to_resolved_json_schema_templates = {
    **{tp: "__value__" for tp in _non_generic_fields_types},
    Omittable[Sequence[JSONSchemaT]]: (  # type: ignore[misc, valid-type]
        "Omitted() if __value__ == Omitted() else [__replacer__(item, __ctx__) for item in __value__]"
    ),
    Omittable[JSONSchemaT]: "Omitted() if __value__ == Omitted() else __replacer__(__value__, __ctx__)",  # type: ignore[misc, valid-type]
    Omittable[JSONObject[JSONSchemaT]]: (  # type: ignore[misc, valid-type]
        "Omitted() if __value__ == Omitted() else"
        " {key: __replacer__(value, __ctx__) for key, value in __value__.items()}"
    ),
    Omittable[RefT]: "Omitted() if __value__ == Omitted() else __ctx__[__value__]",  # type: ignore[misc, valid-type]
}


JSONSchemaSourceT = TypeVar("JSONSchemaSourceT")
JSONSchemaTargetT = TypeVar("JSONSchemaTargetT")
ContextT = TypeVar("ContextT")


def _generate_json_schema_replacer(
    function_name: str,
    file_name: str,
    templates: Mapping[TypeHint, Optional[str]],
    source_cls: type[JSONSchemaSourceT],
    target_cls: type[JSONSchemaTargetT],
    context: type[ContextT],
) -> Callable[[JSONSchemaSourceT, ContextT], JSONSchemaTargetT]:
    result = []
    for fld in fields(target_cls):  # type: ignore[arg-type]
        template = templates[fld.type]
        if template is None:
            continue

        result.append(
            template
            .replace("__value__", f"obj.{fld.name}")
            .replace("__replacer__", function_name)
            .replace("__ctx__", "ctx")
            + ",",
        )

    body = "\n".join(indent(item, " " * 8) for item in result)
    module_code = dedent(
        f"""
        def {function_name}(obj, ctx, /):
            if isinstance(obj, bool):
                return obj

            return {target_cls.__name__}(
            {body}
            )
        """,
    )
    namespace: dict[str, Any] = {target_cls.__name__: target_cls, "Omitted": Omitted}
    exec(compile(module_code, file_name, "exec"), namespace, namespace)  # noqa: S102
    return namespace[function_name]


replace_json_schema_ref = _generate_json_schema_replacer(
    function_name="replace_json_schema_ref",
    file_name="<replace_json_schema_ref generation>",
    templates=_to_resolved_json_schema_templates,
    source_cls=JSONSchema,
    target_cls=ResolvedJSONSchema,
    context=Mapping[RefSource[JSONSchema], str],  # type: ignore[type-abstract]
)
