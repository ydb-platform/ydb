"""
Module defining common API types for use by rules and policies.

In principle, these aren't relevant to the high-level validation API.
"""

import logging
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Callable, Iterable, Optional, Tuple, Type, TypeVar, Union

from pyhanko.pdf_utils.generic import (
    ArrayObject,
    Dereferenceable,
    DictionaryObject,
    IndirectObject,
    Reference,
    TrailerReference,
)
from pyhanko.pdf_utils.reader import HistoricalResolver, RawPdfPath

from ...pdf_utils import misc
from ...pdf_utils.rw_common import PdfHandler
from ...pdf_utils.xref import TrailerDictionary
from .policy_api import ModificationLevel

logger = logging.getLogger(__name__)


__all__ = [
    'QualifiedWhitelistRule',
    'WhitelistRule',
    'ReferenceUpdate',
    'Context',
    'RelativeContext',
    'AbsoluteContext',
]


def _eq_deref(a: Dereferenceable, b: Dereferenceable):
    if isinstance(a, TrailerReference) and isinstance(b, TrailerReference):
        return True
    elif isinstance(a, (Reference, IndirectObject)) and isinstance(
        b, (Reference, IndirectObject)
    ):
        return a.idnum == b.idnum and a.generation == b.generation
    else:
        return False


def _hash_deref(a: Dereferenceable):
    if isinstance(a, TrailerReference):
        return hash((0, 0, 0))
    elif isinstance(a, (Reference, IndirectObject)):
        return hash((1, a.idnum, a.generation))


class Context:
    @classmethod
    def from_absolute(
        cls, pdf_handler: PdfHandler, absolute_path: RawPdfPath
    ) -> 'AbsoluteContext':
        return AbsoluteContext(pdf_handler=pdf_handler, path=absolute_path)

    @classmethod
    def relative_to(
        cls,
        start: Union[DictionaryObject, ArrayObject, TrailerDictionary],
        path: Union[RawPdfPath, int, str],
    ) -> 'RelativeContext':
        container_ref: Optional[Dereferenceable] = start.container_ref
        assert container_ref is not None
        cur_ref: Dereferenceable = container_ref
        if isinstance(path, (int, str)):
            path = RawPdfPath(path)
        walk = path.walk_nodes(start, transparent_dereference=False)
        rel_path = RawPdfPath()
        for ix, (node, obj) in enumerate(walk):
            # make sure we don't do a reset on the last node
            # (# of nodes is the path length + 1)
            if isinstance(obj, IndirectObject) and ix < len(path):
                cur_ref = obj.reference
                rel_path = RawPdfPath()
            elif isinstance(node, (int, str)):
                rel_path += node
        return RelativeContext(cur_ref, rel_path)

    def descend(self, path: Union[RawPdfPath, int, str]) -> 'Context':
        raise NotImplementedError


@dataclass(frozen=True)
class RelativeContext(Context):
    anchor: Dereferenceable
    """
    Reference to the container object. In comparisons, this should be
    the reference tied to the older revision.
    """

    relative_path: RawPdfPath
    """
    Path to the object from the container.
    """

    def descend(self, path: Union[RawPdfPath, int, str]) -> 'RelativeContext':
        root = self.anchor.get_object()
        containers = (DictionaryObject, ArrayObject, TrailerDictionary)
        if not isinstance(root, containers):
            raise misc.PdfReadError(
                f"Anchor {self.anchor} is not a container object"
            )
        return self.__class__.relative_to(root, self.relative_path + path)

    def __hash__(self):
        return hash(('rel', _hash_deref(self.anchor), self.relative_path))

    def __eq__(self, other):
        return (
            isinstance(other, RelativeContext)
            and other.relative_path == self.relative_path
            and _eq_deref(other.anchor, self.anchor)
        )


@dataclass(frozen=True)
class AbsoluteContext(Context):
    path: RawPdfPath
    """
    Absolute path from the trailer.
    """

    pdf_handler: PdfHandler = dataclass_field(
        repr=False, hash=False, compare=False
    )
    """
    The PDF handler to which this context is tied.
    """

    @property
    def relative_view(self) -> RelativeContext:
        return RelativeContext.relative_to(
            self.pdf_handler.trailer_view, self.path
        )

    def descend(self, path: Union[RawPdfPath, int, str]) -> 'AbsoluteContext':
        return AbsoluteContext(self.path + path, self.pdf_handler)


class ApprovalType(misc.OrderedEnum):
    BLANKET_APPROVE = 0
    APPROVE_RELATIVE_CONTEXT = 1
    APPROVE_PATH = 2


RefUpdateType = TypeVar('RefUpdateType', bound='ReferenceUpdate')


@dataclass(frozen=True)
class ReferenceUpdate:
    updated_ref: Reference
    """
    Reference that was (potentially) updated.
    """

    context_checked: Optional[Context] = None

    @classmethod
    def curry_ref(
        cls: Type[RefUpdateType], **kwargs
    ) -> Callable[[Reference], RefUpdateType]:
        return lambda ref: cls(updated_ref=ref, **kwargs)

    @property
    def approval_type(self) -> ApprovalType:
        context = self.context_checked
        if isinstance(context, RelativeContext):
            return ApprovalType.APPROVE_RELATIVE_CONTEXT
        elif isinstance(context, AbsoluteContext):
            return ApprovalType.APPROVE_PATH
        else:
            return ApprovalType.BLANKET_APPROVE


class QualifiedWhitelistRule:
    """
    Abstract base class for a whitelisting rule that outputs references together
    with the modification level at which they're cleared.

    This is intended for use by complicated whitelisting rules that need to
    differentiate between multiple levels.
    """

    def apply_qualified(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[Tuple[ModificationLevel, ReferenceUpdate]]:
        """
        Apply the rule to the changes between two revisions.

        :param old:
            The older, base revision.
        :param new:
            The newer revision to be vetted.
        """
        raise NotImplementedError


class WhitelistRule:
    """
    Abstract base class for a whitelisting rule that simply outputs
    cleared references without specifying a modification level.

    These rules are more flexible than rules of type
    :class:`.QualifiedWhitelistRule`, since the modification level can be
    specified separately (see :meth:`.WhitelistRule.as_qualified`).
    """

    def apply(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[ReferenceUpdate]:
        """
        Apply the rule to the changes between two revisions.

        :param old:
            The older, base revision.
        :param new:
            The newer revision to be vetted.
        """
        raise NotImplementedError

    def as_qualified(self, level: ModificationLevel) -> QualifiedWhitelistRule:
        """
        Construct a new :class:`QualifiedWhitelistRule` that whitelists the
        object references from this rule at the level specified.

        :param level:
            The modification level at which the output of this rule should be
            cleared.
        :return:
            A :class:`.QualifiedWhitelistRule` backed by this rule.
        """
        return _WrappingQualifiedWhitelistRule(self, level)


class _WrappingQualifiedWhitelistRule(QualifiedWhitelistRule):
    def __init__(self, rule: WhitelistRule, level: ModificationLevel):
        self.rule = rule
        self.level = level

    def apply_qualified(
        self, old: HistoricalResolver, new: HistoricalResolver
    ) -> Iterable[Tuple[ModificationLevel, ReferenceUpdate]]:
        for ref in self.rule.apply(old, new):
            yield self.level, ref
