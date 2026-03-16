from __future__ import annotations

import typing as tp

from dataclasses import dataclass

from ....lib.functions import Fn1

from ....interfaces.transformers import (
    IterableTransformer,
    IteratorTransformer,
)


# ---

def make_iterable_transformer[Item, **Arguments, NewValue](
    transformer_fn: IterableTransformerFn[Item, Arguments, NewValue],
) -> IterableTransformer[Item, Arguments, NewValue]:
    return _make_transformer(_IterableTransformer, transformer_fn)


def make_iterator_transformer[Item, **Arguments, NewValue](
    transformer_fn: IteratorTransformerFn[Item, Arguments, NewValue],
) -> IteratorTransformer[Item, Arguments, NewValue]:
    return _make_transformer(_IteratorTransformer, transformer_fn)


# ---

class IterableTransformerFn[
    Item,
    **Arguments,
    NewValue,
](tp.Protocol):
    def __call__(
        self,
        iterable: tp.Iterable[Item],
        /,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> NewValue: ...


class IterableTransformerGiven[
    Item,
    **Arguments,
    NewValue,
](tp.Protocol):
    def __call__(
        self,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> Fn1[tp.Iterable[Item], NewValue]: ...


class IteratorTransformerFn[
    Item,
    **Arguments,
    NewValue,
](tp.Protocol):
    def __call__(
        self,
        iterator: tp.Iterator[Item],
        /,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> NewValue: ...


class IteratorTransformerGiven[
    Item,
    **Arguments,
    NewValue,
](tp.Protocol):
    def __call__(
        self,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> Fn1[tp.Iterator[Item], NewValue]: ...


# ---

@dataclass(frozen=True, slots=True)
class _IterableTransformer[Item, **Arguments, NewValue]:
    given: IterableTransformerGiven[Item, Arguments, NewValue]
    __call__: IterableTransformerFn[Item, Arguments, NewValue]

    def __repr__(self) -> str:
        return f'IterableTransformer[{self.__call__!r}]'


@dataclass(frozen=True, slots=True)
class _IteratorTransformer[Item, **Arguments, NewValue]:
    given: IteratorTransformerGiven[Item, Arguments, NewValue]
    __call__: IteratorTransformerFn[Item, Arguments, NewValue]

    def __repr__(self) -> str:
        return f'IteratorTransformer[{self.__call__!r}]'


# ---

def _make_transformer(transformer_cls, transformer_fn):
    return transformer_cls(
        __call__=transformer_fn,
        given=_make_given_fn(transformer_fn),
    )


def _make_given_fn(transformer_fn):
    return (lambda *args, **kwargs: lambda items: (
        transformer_fn(items, *args, **kwargs)
    ))
