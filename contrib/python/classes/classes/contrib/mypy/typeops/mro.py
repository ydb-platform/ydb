from typing import List, Optional

from mypy.plugin import MethodContext
from mypy.subtypes import is_equivalent
from mypy.types import Instance
from mypy.types import Type as MypyType
from mypy.types import UnionType, union_items
from typing_extensions import final

from classes.contrib.mypy.typeops import type_loader


@final
class MetadataInjector(object):
    """
    Injects fake ``Supports[TypeClass]`` parent classes into ``mro``.

    Ok, this is wild. Why do we need this?
    Because, otherwise expressing ``Supports`` is not possible,
    here's an example:

    .. code:: python

        >>> from classes import AssociatedType, Supports, typeclass

        >>> class ToStr(AssociatedType):
        ...     ...

        >>> @typeclass(ToStr)
        ... def to_str(instance) -> str:
        ...     ...

        >>> @to_str.instance(int)
        ... def _to_str_int(instance: int) -> str:
        ...      return 'Number: {0}'.format(instance)

        >>> assert to_str(1) == 'Number: 1'

    Now, let's use ``Supports`` to only pass specific
    typeclass instances in a function:

    .. code:: python

        >>> def convert_to_string(arg: Supports[ToStr]) -> str:
        ...     return to_str(arg)

    This is possible, due to a fact that we insert ``Supports[ToStr]``
    into all classes that are mentioned as ``.instance()`` for ``ToStr``
    typeclass.

    So, we can call:

    .. code:: python

        >>> assert convert_to_string(1) == 'Number: 1'

    But, ``convert_to_string(None)`` will raise a type error.
    """

    __slots__ = (
        '_associated_type',
        '_instance_types',
        '_ctx',
        '_added_types',
    )

    def __init__(
        self,
        associated_type: MypyType,
        instance_type: MypyType,
        ctx: MethodContext,
    ) -> None:
        """
        Smart constructor for the metadata injector.

        It is smart, because it handles ``instance_type`` properly.
        It supports ``Instance`` and ``Union`` types.
        """
        self._associated_type = associated_type
        self._ctx = ctx
        self._instance_types = union_items(instance_type)

        # Why do we store added types in a mutable global state?
        # Because, these types are hard to replicate without the proper context.
        # So, we just keep them here. Based on usage, it is fine.
        self._added_types: List[Instance] = []

    def add_supports_metadata(self) -> None:
        """Injects ``Supports`` metadata into instance types' mro."""
        if not isinstance(self._associated_type, Instance):
            return

        for instance_type in self._instance_types:
            if not isinstance(instance_type, Instance):
                continue

            supports_type = _load_supports_type(
                associated_type=self._associated_type,
                instance_type=instance_type,
                ctx=self._ctx,
            )

            index = self._find_supports_index(instance_type, supports_type)
            if index is not None:
                # We already have `Supports` base class inserted,
                # it means that we need to unify them:
                # `Supports[A] + Supports[B] == Supports[Union[A, B]]`
                self._add_unified_type(instance_type, supports_type, index)
            else:
                # This is the first time this type is referenced in
                # a typeclass'es instance defintinion.
                # Just inject `Supports` with no extra steps:
                instance_type.type.bases.append(supports_type)

            if supports_type.type not in instance_type.type.mro:
                # We only need to add `Supports` type to `mro` once:
                instance_type.type.mro.append(supports_type.type)

            self._added_types.append(supports_type)

    def remove_supports_metadata(self) -> None:
        """Removes ``Supports`` metadata from instance types' mro."""
        if not isinstance(self._associated_type, Instance):
            return

        for instance_type in self._instance_types:
            if isinstance(instance_type, Instance):
                self._clean_instance_type(instance_type)
        self._added_types = []

    def _clean_instance_type(self, instance_type: Instance) -> None:
        remove_mro = True
        for added_type in self._added_types:
            index = self._find_supports_index(instance_type, added_type)
            if index is not None:
                remove_mro = self._remove_unified_type(
                    instance_type,
                    added_type,
                    index,
                )

            if remove_mro and added_type.type in instance_type.type.mro:
                # We remove `Supports` type from `mro` only if
                # there are not associated types left.
                # For example, `Supports[A, B] - Supports[B] == Supports[A]`
                # then `Supports[A]` stays.
                # `Supports[A] - Supports[A] == None`
                # then `Supports` is removed from `mro` as well.
                instance_type.type.mro.remove(added_type.type)

    def _find_supports_index(
        self,
        instance_type: Instance,
        supports_type: Instance,
    ) -> Optional[int]:
        for index, base in enumerate(instance_type.type.bases):
            if is_equivalent(base, supports_type, ignore_type_params=True):
                return index
        return None

    def _add_unified_type(
        self,
        instance_type: Instance,
        supports_type: Instance,
        index: int,
    ) -> None:
        unified_arg = UnionType.make_union([
            *supports_type.args,
            *instance_type.type.bases[index].args,
        ])
        instance_type.type.bases[index] = supports_type.copy_modified(
            args=[unified_arg],
        )

    def _remove_unified_type(
        self,
        instance_type: Instance,
        supports_type: Instance,
        index: int,
    ) -> bool:
        base = instance_type.type.bases[index]
        union_types = [
            type_arg
            for type_arg in union_items(base.args[0])
            if type_arg not in supports_type.args
        ]
        instance_type.type.bases[index] = supports_type.copy_modified(
            args=[UnionType.make_union(union_types)],
        )
        return not bool(union_types)


def _load_supports_type(
    associated_type: Instance,
    instance_type: Instance,
    ctx: MethodContext,
) -> Instance:
    # Why do have to modify args of `associated_type`?
    # Because `mypy` requires `type_var.id` to match,
    # otherwise, they would be treated as different variables.
    # That's why we copy the typevar definition from instance itself.
    supports_spec = associated_type.copy_modified(
        args=instance_type.type.defn.type_vars,
    )

    return type_loader.load_supports_type(
        supports_spec,
        ctx,
    )
