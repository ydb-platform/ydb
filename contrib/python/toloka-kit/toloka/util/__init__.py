__all__: list = [
    'AsyncMultithreadWrapper',
    'get_signature',
    'identity',
]
from typing import Any, Callable, Dict, Iterator
from inspect import signature, Signature

from .async_utils import AsyncMultithreadWrapper


def rename_dict_keys(data: dict, mapping: Dict[str, str]):
    renamed_dict = {}
    for key, value in data.items():
        renamed_key = mapping.get(key, key)
        if renamed_key in renamed_dict:
            raise ValueError(f'Key {renamed_key} repeats')
        renamed_dict[renamed_key] = value

    return renamed_dict


def make_camel_from_snake_dict_keys(data: dict) -> dict:
    return {camel_case_from_snake_case(key): value for key, value in data.items()}


def make_snake_from_camel_dict_keys(data: dict) -> dict:
    return {snake_case_from_camel_case(key): value for key, value in data.items()}


def camel_case_from_snake_case(name: str) -> str:
    split = name.split('_')
    return split[0] + ''.join(word.title() for word in split[1:])


def snake_case_from_camel_case(name: str) -> str:
    return ''.join('_' + c.lower() if c.isupper() else c for c in name)


def traverse_dicts_recursively(obj: Any) -> Iterator[dict]:
    if isinstance(obj, dict):
        yield from traverse_dicts_recursively(list(obj.values()))
        yield obj
    elif isinstance(obj, list):
        for value in obj:
            yield from traverse_dicts_recursively(value)


def get_signature(func: Callable) -> Signature:
    """
    Correctly processes a signature for a callable. Correctly processes
    classes
    """
    if isinstance(func, type):
        sig = signature(func.__init__)  # type: ignore
        params = list(sig.parameters.values())
        return sig.replace(parameters=params[1:])
    return signature(func)


def identity(arg: Any) -> Any:
    return arg
