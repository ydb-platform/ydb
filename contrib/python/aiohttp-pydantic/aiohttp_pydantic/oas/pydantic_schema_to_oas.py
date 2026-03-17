from operator import itemgetter
from typing import Callable, List, Tuple, Union
import warnings

from packaging import version


def pydantic_schema_to_oas_3_0(schema):
    if (properties := schema.get("properties")) is not None:
        for property_ in properties.values():
            pydantic_schema_to_oas_3_0(property_)

    if (any_of := schema.get("anyOf")) is not None:
        pop_null_at = -1
        for i, element in enumerate(any_of):
            if element["type"] == "null":
                pop_null_at = i
                break

        if pop_null_at != -1:
            del any_of[pop_null_at]
            if len(any_of) == 1:
                schema["type"] = schema.pop("anyOf")[0]["type"]
            schema["nullable"] = True
            if schema.get("default", ...) is None:
                del schema["default"]

        for element in any_of:
            if (const_value := element.pop("const", None)) is not None:
                element["enum"] = [const_value]


def pydantic_schema_to_oas_3_1(schema):
    pass


_PYDANTIC_SCHEMA_TO_OAS: List[Tuple[version.Version, Callable]] = [
    (
        version.Version("3.0.0"),
        pydantic_schema_to_oas_3_0,
    ),
    (version.Version("3.1.0"), pydantic_schema_to_oas_3_1),
]

_PYDANTIC_SCHEMA_TO_OAS.sort(key=itemgetter(0))


def translater(open_api_version: Union[version.Version, str]):
    """
    Return the most appropriate translater based on the required OpenAPI version.

    Chooses the highest available version <= requested version.

    The Open Api existing version:

    https://spec.openapis.org/oas/#specification-versions
    """
    if isinstance(open_api_version, str):
        requested = version.parse(open_api_version)
    else:
        requested = open_api_version

    compatible = [item for item in _PYDANTIC_SCHEMA_TO_OAS if item[0] <= requested]

    if compatible:
        best_version, best_translater = compatible[-1]
    else:
        best_version, best_translater = _PYDANTIC_SCHEMA_TO_OAS[0]

    if best_version != requested:
        warnings.warn(
            f"Requested OAS version {requested} not found. "
            f"Using closest available version: {best_version}.",
            UserWarning,
        )

    return best_translater


def add_or_replace_translater(
    open_api_version: Union[version.Version, str], translater_func: Callable
):
    """
    Add a new translater for a given OpenAPI version, or replace the existing one.
    """
    if isinstance(open_api_version, str):
        open_api_version = version.parse(open_api_version)

    for i, (ver, _) in enumerate(_PYDANTIC_SCHEMA_TO_OAS):
        if ver == open_api_version:
            _PYDANTIC_SCHEMA_TO_OAS[i] = (open_api_version, translater_func)
            break
    else:
        _PYDANTIC_SCHEMA_TO_OAS.append((open_api_version, translater_func))

    _PYDANTIC_SCHEMA_TO_OAS.sort(key=itemgetter(0))
