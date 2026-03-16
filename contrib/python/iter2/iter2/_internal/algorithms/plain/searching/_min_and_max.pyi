import typing as tp
import _typeshed as tp_shed

from ....lib.functions import Fn1
from ....lib.boxed_values import Option2


# ---

@tp.overload
def find_min_value[
    Item: tp_shed.SupportsRichComparison
](
    iterable: tp.Iterable[Item],
    *,
    key: tp.Literal[None] = None,
) -> Option2[Item]: ...


@tp.overload
def find_min_value[
    Item, Key: tp_shed.SupportsRichComparison
](
    iterable: tp.Iterable[Item],
    *,
    key: Fn1[Item, Key],
) -> Option2[Item]: ...


# ---

@tp.overload
def find_max_value[
    Item: tp_shed.SupportsRichComparison
](
    iterable: tp.Iterable[Item],
    *,
    key: tp.Literal[None] = None,
) -> Option2[Item]: ...


@tp.overload
def find_max_value[
    Item, Key: tp_shed.SupportsRichComparison
](
    iterable: tp.Iterable[Item],
    *,
    key: Fn1[Item, Key],
) -> Option2[Item]: ...
