import typing as tp
import _typeshed as tp_shed

from ....lib.functions import (
    Thunk,
    Fn1,
    Fn2,
)
from ....lib.boxed_values import Option2


# ---

type HomoFoldFn[T] = Fn2[T, T, T]
type HeteroFoldFn[S, T] = Fn2[S, T, S]


# ---

fold_items: _FoldItemsIterableTransformer


class _FoldItemsIterableTransformer(tp.Protocol):
    @tp.overload
    def __call__[Item](
        self,
        iterable: tp.Iterable[Item],
        fn: HomoFoldFn[Item],
    ) -> Option2[Item]: ...

    @tp.overload
    def __call__[Item, State](
        self,
        iterable: tp.Iterable[Item],
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> State: ...

    @tp.overload
    def __call__[Item, State](
        self,
        iterable: tp.Iterable[Item],
        *,
        initial_fn: Thunk[State],
        fn: HeteroFoldFn[State, Item],
    ) -> State: ...

    # ---

    @tp.overload
    def given[Item](
        self,
        fn: HomoFoldFn[Item],
    ) -> Fn1[tp.Iterable[Item], Option2[Item]]: ...

    @tp.overload
    def given[Item, State](
        self,
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> Fn1[tp.Iterable[Item], State]: ...

    @tp.overload
    def given[Item, State](
        self,
        *,
        initial_fn: Thunk[State],
        fn: HeteroFoldFn[State, Item],
    ) -> Fn1[tp.Iterable[Item], State]: ...


# ---

fold_cumulative: _FoldCumulativeIterableTransformer


class _FoldCumulativeIterableTransformer(tp.Protocol):
    @tp.overload
    def __call__[Item](
        self,
        iterable: tp.Iterable[Item],
        fn: HomoFoldFn[Item],
    ) -> tp.Iterator[Item]: ...

    @tp.overload
    def __call__[State, Item](
        self,
        iterable: tp.Iterable[Item],
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> tp.Iterator[State]: ...

    # ---

    @tp.overload
    def given[Item](
        self,
        fn: HomoFoldFn[Item],
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[Item]]: ...

    @tp.overload
    def given[State, Item](
        self,
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[State]]: ...


# ---

unfold_cumulative: _UnfoldCumulativeIterableTransformer


class _UnfoldCumulativeIterableTransformer(tp.Protocol):
    @tp.overload
    def __call__[Item](
        self,
        iterable: tp.Iterable[Item],
        fn: HomoFoldFn[Item],
    ) -> tp.Iterator[Item]: ...


    @tp.overload
    def __call__[State, Item](
        self,
        iterable: tp.Iterable[Item],
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> tp.Iterator[Item]: ...

    # ---

    @tp.overload
    def given[Item](
        self,
        fn: HomoFoldFn[Item],
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[Item]]: ...


    @tp.overload
    def given[State, Item](
        self,
        initial: State,
        fn: HeteroFoldFn[State, Item],
    ) -> tp.Iterator[Item]: ...


# --- groupping ---

group_and_fold_values_globally_with_same_computable_key: _GroupAndFoldValuesGloballyWithSameComputableKeyIterableTransformer


class _GroupAndFoldValuesGloballyWithSameComputableKeyIterableTransformer(tp.Protocol):
    @tp.overload
    def __call__[
        Key: tp.Hashable, Value,
    ](
        self,
        iterable: tp.Iterable[Value],
        *,
        key_fn: Fn1[Value, Key],
        fold_fn: HomoFoldFn[Value],
    ) -> tp.Dict[Key, Value]: ...

    @tp.overload
    def __call__[
        Key: tp.Hashable, Value, State,
    ](
        self,
        iterable: tp.Iterable[Value],
        *,
        key_fn: Fn1[Value, Key],
        initial_fn: Thunk[State],
        fold_fn: HeteroFoldFn[State, Value],
    ) -> tp.Dict[Key, State]: ...

    # ---

    @tp.overload
    def given[
        Key: tp.Hashable, Value,
    ](
        self,
        *,
        key_fn: Fn1[Value, Key],
        fold_fn: HomoFoldFn[Value],
    ) -> Fn1[tp.Iterable[Value], tp.Dict[Key, Value]]: ...

    @tp.overload
    def given[
        Key: tp.Hashable, Value, State,
    ](
        self,
        *,
        key_fn: Fn1[Value, Key],
        initial_fn: Thunk[State],
        fold_fn: HeteroFoldFn[State, Value],
    ) -> Fn1[tp.Iterable[Value], tp.Dict[Key, State]]: ...


# ---

group_and_fold_values_globally_by_key: _GroupAndFoldValuesGloballyByKeyIterableTransformer


class _GroupAndFoldValuesGloballyByKeyIterableTransformer(tp.Protocol):
    @tp.overload
    def __call__[
        Key: tp.Hashable, Value,
    ](
        self,
        iterable: tp.Iterable[tp.Tuple[Key, Value]],
        *,
        fold_fn: HomoFoldFn[Value],
    ) -> tp.Dict[Key, Value]: ...

    @tp.overload
    def __call__[
        Key: tp.Hashable, Value, State,
    ](
        self,
        iterable: tp.Iterable[tp.Tuple[Key, Value]],
        *,
        initial_fn: Thunk[State],
        fold_fn: HeteroFoldFn[State, Value],
    ) -> tp.Dict[Key, State]: ...

    # ---

    @tp.overload
    def given[
        Key: tp.Hashable, Value,
    ](
        self,
        *,
        fold_fn: HomoFoldFn[Value],
    ) -> Fn1[tp.Iterable[tp.Tuple[Key, Value]], tp.Dict[Key, Value]]: ...

    @tp.overload
    def given[
        Key: tp.Hashable, Value, State,
    ](
        self,
        *,
        initial_fn: Thunk[State],
        fold_fn: HeteroFoldFn[State, Value],
    ) -> Fn1[tp.Iterable[tp.Tuple[Key, Value]], tp.Dict[Key, State]]: ...


# ---

iterate_in_chunks_of_exact_size_with_padding: _IterateInChunksOfExactSizeWithPadding


class _IterateInChunksOfExactSizeWithPadding(tp.Protocol):
    @tp.overload
    def __call__[Item](
        self,
        iterable: tp.Iterable[Item],
        *,
        size: int,
    ) -> tp.Iterator[tp.Tuple[tp.Optional[Item], ...]]: ...


    @tp.overload
    def __call__[Item, FillItem](
        self,
        iterable: tp.Iterable[Item],
        *,
        size: int,
        fill_value: FillItem,
    ) -> tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]: ...


    @tp.overload
    def __call__[Item, FillItem](
        self,
        iterable: tp.Iterable[Item],
        *,
        size: int,
        fill_value_fn: Thunk[FillItem],
    ) -> tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]: ...

    # ---

    @tp.overload
    def given[Item](
        self,
        *,
        size: int,
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[tp.Tuple[tp.Optional[Item], ...]]]: ...


    @tp.overload
    def given[Item, FillItem](
        self,
        *,
        size: int,
        fill_value: FillItem,
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]]: ...


    @tp.overload
    def given[Item, FillItem](
        self,
        *,
        size: int,
        fill_value_fn: Thunk[FillItem],
    ) -> Fn1[tp.Iterable[Item], tp.Iterator[tp.Tuple[tp.Union[Item, FillItem], ...]]]: ...


# --- searching ---

find_min_value: _FindMinValue


class _FindMinValue(tp.Protocol):
    @tp.overload
    def __call__[
        Item: tp_shed.SupportsRichComparison
    ](
        self,
        iterable: tp.Iterable[Item],
        *,
        key: tp.Literal[None] = None,
    ) -> Option2[Item]: ...


    @tp.overload
    def __call__[
        Item, Key: tp_shed.SupportsRichComparison
    ](
        self,
        iterable: tp.Iterable[Item],
        *,
        key: Fn1[Item, Key],
    ) -> Option2[Item]: ...

    # ---

    @tp.overload
    def given[
        Item: tp_shed.SupportsRichComparison
    ](
        self,
        *,
        key: tp.Literal[None] = None,
    ) -> Fn1[tp.Iterable[Item], Option2[Item]]: ...


    @tp.overload
    def given[
        Item, Key: tp_shed.SupportsRichComparison
    ](
        self,
        *,
        key: Fn1[Item, Key],
    ) -> Fn1[tp.Iterable[Item], Option2[Item]]: ...


# ---

find_max_value: _FindMaxValue


class _FindMaxValue(tp.Protocol):
    @tp.overload
    def __call__[
        Item: tp_shed.SupportsRichComparison
    ](
        self,
        iterable: tp.Iterable[Item],
        *,
        key: tp.Literal[None] = None,
    ) -> Option2[Item]: ...


    @tp.overload
    def __call__[
        Item, Key: tp_shed.SupportsRichComparison
    ](
        self,
        iterable: tp.Iterable[Item],
        *,
        key: Fn1[Item, Key],
    ) -> Option2[Item]: ...

    # ---

    @tp.overload
    def given[
        Item: tp_shed.SupportsRichComparison
    ](
        self,
        *,
        key: tp.Literal[None] = None,
    ) -> Fn1[tp.Iterable[Item], Option2[Item]]: ...


    @tp.overload
    def given[
        Item, Key: tp_shed.SupportsRichComparison
    ](
        self,
        *,
        key: Fn1[Item, Key],
    ) -> Fn1[tp.Iterable[Item], Option2[Item]]: ...


# --- splitting ---

unzip_into_lists: _UnzipIntoListsIterableTransformer


class _UnzipIntoListsIterableTransformer(tp.Protocol):
    # -----------------
    # GENERATED SECTION
    # -----------------

    # # [Generating code]
    # MAX_ARGS = 16

    # type_var_template = lambda idx: f'''_G{idx} = tp.TypeVar('_G{idx}')'''
    # types = lambda n: ', '.join(f'_G{idx}' for idx in range(n))
    # it_of_tuples = lambda n: f'tp.Iterable[tp.Tuple[{types(n)}]]'
    # arity_hint = lambda n: f'tp.Literal[{n}]'
    # lists_of_types = lambda n: f', '.join(f'tp.List[_G{idx}]' for idx in range(n))
    # unzip_call_template = lambda n: f'''@tp.overload\ndef __call__(self, iterable: {it_of_tuples(n)}, *, arity_hint: {arity_hint(n)}) -> tp.Tuple[{lists_of_types(n)}]: ...'''
    # unzip_given_template = lambda n: f'''@tp.overload\ndef given(self, *, arity_hint: {arity_hint(n)}) -> Fn1[{it_of_tuples(n)}, tp.Tuple[{lists_of_types(n)}]]: ...'''


    # print('\n'.join((
    #     '# START >>',
    #     '\n# --- Type Vars ---',
    #     *(type_var_template(x) for x in range(MAX_ARGS + 1)),
    #     '\n# --- unzip_into_lists (__call__) ---',
    #     *(unzip_call_template(x) for x in range(2, MAX_ARGS + 1)),
    #     '\n# --- unzip_into_lists (given) ---',
    #     *(unzip_given_template(x) for x in range(2, MAX_ARGS + 1)),
    #     '\n# << END',
    # )))
    # # [/Generating code]

    # START >>

    # --- Type Vars ---
    _G0 = tp.TypeVar('_G0')
    _G1 = tp.TypeVar('_G1')
    _G2 = tp.TypeVar('_G2')
    _G3 = tp.TypeVar('_G3')
    _G4 = tp.TypeVar('_G4')
    _G5 = tp.TypeVar('_G5')
    _G6 = tp.TypeVar('_G6')
    _G7 = tp.TypeVar('_G7')
    _G8 = tp.TypeVar('_G8')
    _G9 = tp.TypeVar('_G9')
    _G10 = tp.TypeVar('_G10')
    _G11 = tp.TypeVar('_G11')
    _G12 = tp.TypeVar('_G12')
    _G13 = tp.TypeVar('_G13')
    _G14 = tp.TypeVar('_G14')
    _G15 = tp.TypeVar('_G15')
    _G16 = tp.TypeVar('_G16')

    # --- unzip_into_lists (__call__) ---
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1]], *, arity_hint: tp.Literal[2]) -> tp.Tuple[tp.List[_G0], tp.List[_G1]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2]], *, arity_hint: tp.Literal[3]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3]], *, arity_hint: tp.Literal[4]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4]], *, arity_hint: tp.Literal[5]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5]], *, arity_hint: tp.Literal[6]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6]], *, arity_hint: tp.Literal[7]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7]], *, arity_hint: tp.Literal[8]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8]], *, arity_hint: tp.Literal[9]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9]], *, arity_hint: tp.Literal[10]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10]], *, arity_hint: tp.Literal[11]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11]], *, arity_hint: tp.Literal[12]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12]], *, arity_hint: tp.Literal[13]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13]], *, arity_hint: tp.Literal[14]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14]], *, arity_hint: tp.Literal[15]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14]]: ...
    @tp.overload
    def __call__(self, iterable: tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14, _G15]], *, arity_hint: tp.Literal[16]) -> tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14], tp.List[_G15]]: ...

    # --- unzip_into_lists (given) ---
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[2]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1]], tp.Tuple[tp.List[_G0], tp.List[_G1]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[3]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[4]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[5]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[6]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[7]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[8]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[9]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[10]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[11]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[12]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[13]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[14]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[15]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14]]]: ...
    @tp.overload
    def given(self, *, arity_hint: tp.Literal[16]) -> Fn1[tp.Iterable[tp.Tuple[_G0, _G1, _G2, _G3, _G4, _G5, _G6, _G7, _G8, _G9, _G10, _G11, _G12, _G13, _G14, _G15]], tp.Tuple[tp.List[_G0], tp.List[_G1], tp.List[_G2], tp.List[_G3], tp.List[_G4], tp.List[_G5], tp.List[_G6], tp.List[_G7], tp.List[_G8], tp.List[_G9], tp.List[_G10], tp.List[_G11], tp.List[_G12], tp.List[_G13], tp.List[_G14], tp.List[_G15]]]: ...

    # << END
