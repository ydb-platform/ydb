from typing import Callable, Iterable, Union


class Schema:
    def repr(self, *, prefix=True, desc=True) -> str:
        ...

    def copy(self) -> "Schema":
        ...


class Builder:
    def __getitem__(self, keys: Iterable[str]) -> "Builder": ...
    def __getattr__(self, name: str) -> "Builder": ...
    def __call__(self, *args, **kwargs) -> "Builder": ...


T: Builder


class Compiler:
    def __init__(self, validators: dict = None):
        ...

    def compile(self, schema: Union[Schema, Builder]) -> Callable:
        ...
