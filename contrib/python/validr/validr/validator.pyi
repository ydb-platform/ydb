from typing import Callable, Dict, List


class ValidrError(ValueError):

    @property
    def value(self): ...

    @property
    def field(self) -> str: ...

    @property
    def position(self) -> str: ...

    @property
    def message(self) -> str: ...


class Invalid(ValidrError):
    ...


class ModelInvalid(Invalid):
    ...


class SchemaError(ValidrError):
    ...


def validator(string: bool = None, *, accept=None, output=None):
    ...


builtin_validators: Dict[str, Callable]


def create_enum_validator(
    name: str,
    items: List,
    string: bool = True,
) -> Callable:
    ...


def create_re_validator(
    name: str,
    r: str,
    maxlen: int = None,
    strip: bool = False,
) -> Callable:
    ...


class py_mark_index():
    def __init__(self, index: int = -1):
        ...

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...


class py_mark_key():
    def __init__(self, key: str):
        ...

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        ...
