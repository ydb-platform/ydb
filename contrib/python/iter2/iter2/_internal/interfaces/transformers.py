import typing as tp

from ..lib.functions import Fn1


# ---

class IterableTransformer[
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

    def given(
        self,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> Fn1[tp.Iterable[Item], NewValue]: ...


class IteratorTransformer[
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

    def given(
        self,
        *args: Arguments.args,
        **kwargs: Arguments.kwargs,
    ) -> Fn1[tp.Iterator[Item], NewValue]: ...
