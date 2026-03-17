"""
Module defining common helpers for use by rules and policies.

In principle, these aren't relevant to the high-level validation API.
"""

import logging
from typing import Callable, FrozenSet, Generator, Optional, Set, Tuple, TypeVar

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.generic import PdfObject, Reference
from pyhanko.pdf_utils.reader import HistoricalResolver

from .policy_api import ModificationLevel, SuspiciousModification
from .rules_api import ReferenceUpdate

logger = logging.getLogger(__name__)

__all__ = [
    'qualify',
    'qualify_transforming',
    'safe_whitelist',
    'compare_key_refs',
    'compare_dicts',
    'assert_not_stream',
]

TwoVersions = Tuple[Optional[generic.PdfObject], Optional[generic.PdfObject]]


def assert_not_stream(obj):
    """
    Throw :class:`.SuspiciousModification` if the argument is a stream object.
    """
    if isinstance(obj, generic.StreamObject):
        raise SuspiciousModification(
            f"Unexpected stream encountered at {obj.container_ref}!"
        )


def safe_whitelist(
    old: HistoricalResolver, old_ref, new_ref
) -> Generator[Reference, None, None]:
    """
    Checks whether an indirect reference in a PDF structure
    can be updated without clobbering an older object in a way
    that causes ramifications at the PDF syntax level.

    The following are verified:

     - Does the old reference point to a non-stream object?
     - If the new reference is equal to the old one, does the new reference point
       to a non-stream object?
     - If the new reference is not equal to the old one,
       is the new reference a newly defined object?

    This is a generator for syntactical convenience and integration
    with internal APIs, but it will always yield at most one element.
    """

    if old_ref:
        assert_not_stream(old_ref.get_object())

    if old_ref == new_ref:
        assert_not_stream(new_ref.get_object())
        yield new_ref
    elif old.is_ref_available(new_ref):
        yield new_ref
    else:
        raise SuspiciousModification(
            f"Update clobbers or reuses {new_ref} in an unexpected way."
        )


def compare_key_refs(
    key,
    old: HistoricalResolver,
    old_dict: generic.DictionaryObject,
    new_dict: generic.DictionaryObject,
) -> Generator[Reference, None, TwoVersions]:
    """
    Ensure that updating a key in a dictionary has no undesirable side effects.
    The following scenarios are allowed:

    0. replacing a direct value with another direct value
    1. adding a key in new_dict
    2. replacing a direct value in old_dict with a reference in new_dict
    3. the reverse (allowed by default)
    4. replacing a reference with another reference (that doesn't override
       anything else)

    The restrictions of `safe_whitelist` apply to this function as well.

    Note: this routine is only safe to use if the structure of the resulting
    values is also checked. Otherwise, it can lead to reference leaks if
    one is not careful.
    """

    try:
        old_value = old_dict.raw_get(key)
        if isinstance(old_value, generic.IndirectObject):
            old_value_ref = old_value.reference
            old_value = old_value.get_object()
        else:
            old_value_ref = None
    except KeyError:
        old_value_ref = old_value = None

    try:
        new_value = new_dict.raw_get(key)
        if isinstance(new_value, generic.IndirectObject):
            new_value_ref = new_value.reference
            new_value = new_value.get_object()
        else:
            new_value_ref = None
    except KeyError:
        if old_value is not None:
            raise SuspiciousModification(
                f"Key {key} was deleted from dictionary"
            )
        return None, None  # nothing to do

    if new_value_ref is not None:
        yield from safe_whitelist(old, old_value_ref, new_value_ref)

    return old_value, new_value


R = TypeVar('R', covariant=True)
QualifyIn = TypeVar('QualifyIn', contravariant=True)
RefToUpd = TypeVar('RefToUpd', bound=ReferenceUpdate)
OutRefUpd = TypeVar('OutRefUpd', bound=ReferenceUpdate, covariant=True)


def qualify(
    level: ModificationLevel,
    rule_result: Generator[RefToUpd, None, R],
) -> Generator[Tuple[ModificationLevel, RefToUpd], None, R]:
    """
    This is a helper function for rule implementors.
    It attaches a fixed modification level to an existing reference update
    generator, respecting the original generator's return value (if relevant).

    A prototypical use would be of the following form:

    .. code-block:: python

        def some_generator_function():
            # do stuff
            for ref in some_list:
                # do stuff
                yield ref

            # do more stuff
            return summary_value

        # ...

        def some_qualified_generator_function():
            summary_value = yield from qualify(
                ModificationLevel.FORM_FILLING,
                some_generator_function()
            )

    Provided that ``some_generator_function`` yields
    :class:`~.generic.ReferenceUpdate` objects, the yield type of the resulting
    generator will be tuples of the form ``(level, ref)``.

    :param level:
        The modification level to set.
    :param rule_result:
        A generator that outputs references to be whitelisted.
    :return:
        A converted generator that outputs references qualified at the
        modification level specified.
    """
    return qualify_transforming(level, rule_result, transform=lambda x: x)


def qualify_transforming(
    level: ModificationLevel,
    rule_result: Generator[QualifyIn, None, R],
    transform: Callable[[QualifyIn], OutRefUpd],
) -> Generator[Tuple[ModificationLevel, OutRefUpd], None, R]:
    """
    This is a version of :func:`.qualify` that additionally allows
    a transformation to be applied to the output of the rule.

    :param level:
        The modification level to set.
    :param rule_result:
        A generator that outputs references to be whitelisted.
    :param transform:
        Function to apply to the reference object before appending
        the modification level and yielding it.
    :return:
        A converted generator that outputs references qualified at the
        modification level specified.
    """
    return misc.map_with_return(
        rule_result, lambda ref: (level, transform(ref))
    )


def compare_dicts(
    old_dict: Optional[PdfObject],
    new_dict: Optional[PdfObject],
    ignored: FrozenSet[str] = frozenset(),
    raise_exc=True,
) -> bool:
    """
    Compare entries in two dictionaries, optionally ignoring certain keys.
    """

    if not isinstance(old_dict, generic.DictionaryObject):
        raise misc.PdfReadError(
            "Encountered unexpected non-dictionary object in prior revision."
        )  # pragma: nocover
    if not isinstance(new_dict, generic.DictionaryObject):
        raise SuspiciousModification(
            "Dict is overridden by non-dict in new revision"
        )

    assert_not_stream(old_dict)
    assert_not_stream(new_dict)
    new_dict_keys = set(new_dict.keys()) - ignored
    old_dict_keys = set(old_dict.keys()) - ignored
    if new_dict_keys != old_dict_keys:
        if raise_exc:
            raise SuspiciousModification(
                f"Dict keys differ: {new_dict_keys} vs. " f"{old_dict_keys}."
            )
        else:
            return False

    for k in new_dict_keys:
        new_val = new_dict.raw_get(k)
        old_val = old_dict.raw_get(k)
        if new_val != old_val:
            if raise_exc:
                raise SuspiciousModification(
                    f"Values for dict key {k} differ:"
                    f"{old_val} changed to {new_val}"
                )
            else:
                return False

    return True
