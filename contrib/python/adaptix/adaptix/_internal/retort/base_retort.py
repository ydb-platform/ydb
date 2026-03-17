import itertools
from abc import ABCMeta
from collections.abc import Iterable, Sequence
from typing import ClassVar, TypeVar

from ..common import VarTuple
from ..provider.essential import Provider
from ..utils import Cloneable, ForbiddingDescriptor


class RetortMeta(ABCMeta):  # inherits from ABCMeta to be compatible with ABC
    _own_class_recipe: VarTuple[Provider]
    recipe = ForbiddingDescriptor()

    def __new__(mcs, name, bases, namespace, **kwargs):
        try:
            _cls_recipe = tuple(namespace.get("recipe", []))
        except TypeError:
            raise TypeError("Recipe attributes must be Iterable[Provider]") from None

        if not all(isinstance(el, Provider) for el in _cls_recipe):
            raise TypeError("Recipe attributes must be Iterable[Provider]")

        namespace["_own_class_recipe"] = _cls_recipe
        namespace["recipe"] = ForbiddingDescriptor()
        return super().__new__(mcs, name, bases, namespace, **kwargs)


T = TypeVar("T")


class BaseRetort(Cloneable, metaclass=RetortMeta):
    recipe: ClassVar[Iterable[Provider]] = []
    _full_class_recipe: ClassVar[VarTuple[Provider]]

    def __init_subclass__(cls, **kwargs):
        # noinspection PyProtectedMember
        cls._full_class_recipe = tuple(
            itertools.chain.from_iterable(
                parent._own_class_recipe
                for parent in cls.mro()
                if isinstance(parent, RetortMeta)
            ),
        )

    def __init__(self, *, recipe: Iterable[Provider] = ()):
        self._instance_recipe = tuple(recipe)
        self._calculate_derived()

    def _get_recipe_head(self) -> Sequence[Provider]:
        return ()

    def _get_recipe_tail(self) -> Sequence[Provider]:
        return ()

    def _get_full_recipe(self) -> Sequence[Provider]:
        return self._full_recipe

    def _calculate_derived(self) -> None:
        super()._calculate_derived()
        self._full_recipe = tuple(
            itertools.chain(
                self._get_recipe_head(),
                self._instance_recipe,
                self._full_class_recipe,
                self._get_recipe_tail(),
            ),
        )
