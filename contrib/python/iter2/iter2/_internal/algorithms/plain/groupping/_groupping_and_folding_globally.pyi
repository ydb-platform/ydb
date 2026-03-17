import typing as tp

from ....lib.functions import (
    Thunk, Fn1, Fn2
)


# ---

type HomoFoldFn[T] = Fn2[T, T, T]
type HeteroFoldFn[S, T] = Fn2[S, T, S]


# ---

@tp.overload
def group_and_fold_values_globally_with_same_computable_key[
    Key: tp.Hashable, Value,
](
    iterable: tp.Iterable[Value],
    *,
    key_fn: Fn1[Value, Key],
    fold_fn: HomoFoldFn[Value],
) -> tp.Dict[Key, Value]: ...


@tp.overload
def group_and_fold_values_globally_with_same_computable_key[
    Key: tp.Hashable, Value, State,
](
    iterable: tp.Iterable[Value],
    *,
    key_fn: Fn1[Value, Key],
    initial_fn: Thunk[State],
    fold_fn: HeteroFoldFn[State, Value],
) -> tp.Dict[Key, State]:
    ...


# ---

@tp.overload
def group_and_fold_values_globally_by_key[
    Key: tp.Hashable, Value,
](
    iterable: tp.Iterable[tp.Tuple[Key, Value]],
    *,
    fold_fn: HomoFoldFn[Value],
) -> tp.Dict[Key, Value]: ...


@tp.overload
def group_and_fold_values_globally_by_key[
    Key: tp.Hashable, Value, State,
](
    iterable: tp.Iterable[tp.Tuple[Key, Value]],
    *,
    initial_fn: Thunk[State],
    fold_fn: HeteroFoldFn[State, Value],
) -> tp.Dict[Key, State]: ...
