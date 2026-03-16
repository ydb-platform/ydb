from typing import Any, get_args, get_origin

from dishka.entities.factory_type import FactoryData, FactoryType
from dishka.entities.key import DependencyKey
from dishka.entities.marker import Has, HasContext


def _render_args(hint: Any) -> str:
    args = get_args(hint)
    return ", ".join(
        get_name(arg, include_module=False)
        for arg in args
    )


def get_name(hint: Any, *, include_module: bool) -> str:
    if isinstance(hint, list):
        res = ",".join(
            get_name(item, include_module=include_module)
            for item in hint
        )
        return f"[{res}]"
    if type(hint) in (Has, HasContext):
        value_name = get_name(hint.value, include_module=include_module)
        return f"{type(hint).__name__}({value_name})"
    if hint is ...:
        return "..."
    if func := getattr(object, "__func__", None):
        return get_name(func, include_module=include_module)

    if include_module:
        module = getattr(hint, "__module__", "")
        if module == "builtins":
            module = ""
        elif module:
            module += "."
    else:
        module = ""

    name = (
        getattr(hint, "__qualname__", None) or
        getattr(hint, "__name__", None)
    )
    if name:
        if get_origin(hint):
            args = f"[{_render_args(hint)}]"
        else:
            args = ""
        return f"{module}{name}{args}"
    return str(hint)


def get_source_name(factory: FactoryData) -> str:
    source = factory.source
    if source == factory.provides.type_hint:
        return ""
    if factory.type is FactoryType.ALIAS:
        return "alias"
    if factory.type is FactoryType.SELECTOR:
        return "select"
    if factory.type is FactoryType.COLLECTION:
        return "collect"

    return get_name(source, include_module=False)


def get_key_name(key: DependencyKey) -> str:
    return get_name(key.type_hint, include_module=True)
