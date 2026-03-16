#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module contains a function and a class for validating XSD content models,
plus a set of functions for manipulating encoded content.
"""
import warnings
from collections import defaultdict, deque, Counter
from collections.abc import Iterable, Iterator, MutableMapping, MutableSequence
from copy import copy
from typing import Any, Optional, Union

from xmlschema.aliases import ModelGroupType, ModelParticleType, SchemaElementType, \
    OccursCounterType
from xmlschema.exceptions import XMLSchemaRuntimeError, XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema import _limits

from .exceptions import XMLSchemaModelError, XMLSchemaModelDepthError
from .wildcards import XsdAnyElement, Xsd11AnyElement
from . import groups

AdvanceYieldedType = tuple[ModelParticleType, int, list[SchemaElementType]]
ContentItemType = tuple[Union[int, str], Any]
EncodedContentType = Union[MutableMapping[Union[int, str], Any], Iterable[ContentItemType]]
StepType = Union[str, SchemaElementType, tuple[Union[str, SchemaElementType], int]]


def distinguishable_paths(path1: list[ModelParticleType], path2: list[ModelParticleType]) -> bool:
    """
    Checks if two model paths are distinguishable in a deterministic way, without looking forward
    or backtracking. The arguments are lists containing paths from the base group of the model to
    a couple of leaf elements. Returns `True` if there is a deterministic separation between paths,
    `False` if the paths are ambiguous.
    """
    e: ModelParticleType

    for k, e in enumerate(path1):
        if e not in path2:
            if not k:
                return True
            depth = k - 1
            break
    else:
        depth = 0

    if path1[depth].max_occurs == 0:
        return True

    univocal1 = univocal2 = True
    if path1[depth].model == 'sequence':  # type: ignore[union-attr]
        idx1 = path1[depth].index(path1[depth + 1])
        idx2 = path2[depth].index(path2[depth + 1])
        before1 = any(not e.is_emptiable() for e in path1[depth][:idx1])
        after1 = before2 = any(not e.is_emptiable() for e in path1[depth][idx1 + 1:idx2])
        after2 = any(not e.is_emptiable() for e in path1[depth][idx2 + 1:])
    else:
        before1 = after1 = before2 = after2 = False

    for k in range(depth + 1, len(path1) - 1):
        univocal1 &= path1[k].is_univocal()
        idx = path1[k].index(path1[k + 1])
        if path1[k].model == 'sequence':  # type: ignore[union-attr]
            before1 |= any(not e.is_emptiable() for e in path1[k][:idx])
            after1 |= any(not e.is_emptiable() for e in path1[k][idx + 1:])
        elif any(e.is_emptiable() for e in path1[k] if e is not path1[k][idx]):
            univocal1 = False

    for k in range(depth + 1, len(path2) - 1):
        univocal2 &= path2[k].is_univocal()
        idx = path2[k].index(path2[k + 1])
        if path2[k].model == 'sequence':  # type: ignore[union-attr]
            before2 |= any(not e.is_emptiable() for e in path2[k][:idx])
            after2 |= any(not e.is_emptiable() for e in path2[k][idx + 1:])
        elif any(e.is_emptiable() for e in path2[k] if e is not path2[k][idx]):
            univocal2 = False

    if path1[depth].model != 'sequence':  # type: ignore[union-attr]
        if before1 and before2:
            return True
        elif before1:
            return univocal1 and path1[-1].is_univocal() or after1 or path1[depth].max_occurs == 1
        elif before2:
            return univocal2 and path2[-1].is_univocal() or after2 or path2[depth].max_occurs == 1
        else:
            return False
    elif path1[depth].max_occurs == 1:
        return before2 or (before1 or univocal1) and (path1[-1].is_univocal() or after1)
    else:
        return (before2 or (before1 or univocal1) and (path1[-1].is_univocal() or after1)) and \
               (before1 or (before2 or univocal2) and (path2[-1].is_univocal() or after2))


def check_model(group: ModelGroupType) -> None:
    """
    Checks if the model group is deterministic. Element Declarations Consistent and
    Unique Particle Attribution constraints are checked.

    :param group: the model group to check.
    :raises: an `XMLSchemaModelError` at first violated constraint.
    """
    def safe_iter_path() -> Iterator[SchemaElementType]:
        iterators: list[Iterator[ModelParticleType]] = []
        particles = iter(group)

        while True:
            for item in particles:
                if isinstance(item, groups.XsdGroup):
                    current_path.append(item)
                    iterators.append(particles)
                    particles = iter(item)
                    if len(iterators) > _limits.MAX_MODEL_DEPTH:
                        raise XMLSchemaModelDepthError(group)
                    break
                else:
                    yield item
            else:
                try:
                    current_path.pop()
                    particles = iterators.pop()
                except IndexError:
                    return

    paths: Any = {}
    current_path: list[ModelParticleType] = [group]

    try:
        any_element = group.parent.open_content.any_element  # type: ignore[union-attr]
    except AttributeError:
        any_element = None

    for e in safe_iter_path():

        previous_path: list[ModelParticleType]
        for pe, previous_path in paths.values():
            # EDC check
            if not e.is_consistent(pe) or any_element and not any_element.is_consistent(pe):
                msg = _("Element Declarations Consistent violation between {0!r} and {1!r}"
                        ": match the same name but with different types").format(e, pe)
                raise XMLSchemaModelError(group, msg)

            # UPA check
            if pe is e or not pe.is_overlap(e):
                continue
            elif pe.parent is e.parent:
                if pe.parent.model in {'all', 'choice'}:
                    if isinstance(pe, Xsd11AnyElement) and not isinstance(e, XsdAnyElement):
                        pe.add_precedence(e, group)
                    elif isinstance(e, Xsd11AnyElement) and not isinstance(pe, XsdAnyElement):
                        e.add_precedence(pe, group)
                    else:
                        msg = _("{0!r} and {1!r} overlap and are in the same {2!r} group")
                        raise XMLSchemaModelError(group, msg.format(pe, e, pe.parent.model))
                elif pe.is_univocal():
                    continue

            if distinguishable_paths(previous_path + [pe], current_path + [e]):
                continue
            elif isinstance(pe, Xsd11AnyElement) and not isinstance(e, XsdAnyElement):
                pe.add_precedence(e, group)
            elif isinstance(e, Xsd11AnyElement) and not isinstance(pe, XsdAnyElement):
                e.add_precedence(pe, group)
            else:
                msg = _("Unique Particle Attribution violation between {0!r} and {1!r}")
                raise XMLSchemaModelError(group, msg.format(pe, e))

        paths[e.name] = e, current_path[:]


class ModelVisitor:
    """
    A visitor design pattern class that can be used for validating XML data related to an XSD
    model group. The visit of the model is done using an external match information,
    counting the occurrences and yielding tuples in case of model's item occurrence errors.
    Ends setting the current element to `None`.

    :param root: the root model group.
    :ivar occurs: the Counter instance for keeping track of occurrences of XSD elements and groups.
    :ivar element: the current XSD element, initialized to the first element of the model.
    :ivar group: the current XSD model group, initialized to *root* argument.
    :ivar items: the current XSD group's items iterator.
    :ivar match: if the XSD group has an effective item match.
    """
    _groups: list[tuple[ModelGroupType, Iterator[ModelParticleType], bool]]
    element: Optional[SchemaElementType]
    occurs: OccursCounterType

    __slots__ = '_groups', 'root', 'occurs', 'element', 'group', 'items', 'match'

    def __init__(self, root: ModelGroupType) -> None:
        self._groups = []
        self.root = root
        self.occurs = Counter()
        self.element = None
        self.group = root
        self.items = self.iter_group()
        self.match = False
        self._start()

    def __repr__(self) -> str:
        return '%s(root=%r)' % (self.__class__.__name__, self.root)

    def clear(self) -> None:
        del self._groups[:]
        self.occurs.clear()
        self.element = None
        self.group = self.root
        self.items = self.iter_group()
        self.match = False

    def _start(self) -> None:
        while True:
            item = next(self.items, None)
            if item is None:
                if not self._groups:
                    break
                self.group, self.items, self.match = self._groups.pop()
            elif not isinstance(item, groups.XsdGroup):
                self.element = item
                break
            elif item:
                self._groups.append((self.group, self.items, self.match))
                self.group = item
                self.items = self.iter_group()
                self.match = False

    @property
    def expected(self) -> list[SchemaElementType]:
        """Returns the expected elements of the current and descendant groups."""
        return self.group.get_expected(self.occurs)

    def restart(self) -> None:
        self.clear()
        self._start()

    def stop(self) -> Iterator[AdvanceYieldedType]:
        """Stop the model and returns the errors, if any."""
        while self.element is not None:
            yield from self.advance()

    def iter_group(self) -> Iterator[ModelParticleType]:
        """Returns an iterator for the current model group."""
        if self.group.model == 'all':
            for e in self.group.iter_elements():
                if not e.is_over(self.occurs):
                    yield e
        elif self.group.max_occurs == 0:
            return
        else:
            yield from self.group.content

    def match_element(self, tag: str) -> Optional[SchemaElementType]:
        if self.element is None:
            raise XMLSchemaValueError(f"can't match the tag, {self!r} is ended!")
        elif self.element.max_occurs == 0:
            return None
        else:
            return self.element.match(tag, group=self.root, occurs=self.occurs)

    def advance(self, match: bool = False) -> Iterator[AdvanceYieldedType]:
        """
        Generator function for advance to the next element. Yields tuples with
        particles information when occurrence violation is found.

        :param match: provides current element match.
        """
        item: ModelParticleType
        item_occurs: int

        def stop_item() -> bool:
            """
            Stops element or group matching, incrementing current group counter.

            :return: `True` if the item has violated the minimum occurrences for itself \
            or for the current group, `False` otherwise.
            """
            nonlocal item
            nonlocal item_occurs

            item_occurs = occurs[item]
            if isinstance(item, groups.XsdGroup):
                self.group, self.items, self.match = self._groups.pop()

            if self.group.model == 'choice':
                if not item_occurs:
                    return False

                high_occurs = occurs[item.oid] or item_occurs
                min_occurs = item.min_occurs
                max_occurs = item.max_occurs

                if max_occurs is None:
                    occurs[self.group] += 1
                elif item_occurs % max_occurs:
                    occurs[self.group] += 1 + item_occurs // max_occurs
                else:
                    occurs[self.group] += item_occurs // max_occurs

                occurs[self.group.oid] += (high_occurs // (min_occurs or 1)) or 1

                occurs[item] = occurs[item.oid] = 0
                self.items = self.iter_group()
                self.match = False
                return min_occurs > high_occurs

            elif self.group.model == 'all':
                return False  # 'all' models can only be checked at the end
            elif self.match:
                pass
            elif item_occurs:
                self.match = True
            elif item.is_emptiable():
                return False
            elif self._groups:
                item = self.group
                return stop_item()
            elif self.group.is_missing(occurs):
                return True
            else:
                item = self.group
                return stop_item()

            if item is self.group.content[-1]:
                for k, item2 in enumerate(self.group.content, start=1):  # pragma: no cover
                    low_occurs = occurs[item2]
                    if not low_occurs:
                        continue

                    high_occurs = occurs[item2.oid] or low_occurs
                    if high_occurs == 1 or \
                            any(not x.is_emptiable() for x in self.group.content[k:]):
                        occurs[self.group] += 1
                        occurs[self.group.oid] += 1
                        break

                    occurs[self.group] += (low_occurs // (item2.max_occurs or low_occurs)) or 1
                    occurs[self.group.oid] += (high_occurs // (item2.min_occurs or 1)) or 1
                    break

            return item.is_missing(occurs)

        def model_error_tuple() -> AdvanceYieldedType:
            if occurs[item]:
                expected = item.get_expected(occurs)
            else:
                occurs[item] = item_occurs
                expected = item.get_expected(occurs)
                occurs[item] = 0

            return item, item_occurs, expected

        if self.element is None:
            raise XMLSchemaValueError(f"can't advance, {self!r} is ended!")

        item = self.element
        occurs = self.occurs
        item_occurs = occurs[item]

        if match:
            occurs[item] += 1
            self.match = True
            if self.group.model == 'all':
                self.items = self.iter_group()
            elif not item.is_over(occurs) or \
                    self.group.model == 'choice' and item.is_ambiguous():
                return

        try:
            if stop_item():
                yield model_error_tuple()

            while True:
                while self.group.is_over(occurs):
                    item = self.group
                    stop_item()

                for obj in self.items:
                    if isinstance(obj, groups.XsdGroup):
                        # inner 'sequence' or 'choice' XsdGroup
                        self._groups.append((self.group, self.items, self.match))
                        self.group = obj
                        self.items = self.iter_group()
                        self.match = False
                        occurs[obj] = occurs[obj.oid] = 0
                        break
                    else:
                        # XsdElement or XsdAnyElement
                        self.element = obj
                        if self.group.model == 'sequence':
                            occurs[obj] = 0
                        return
                else:
                    if self.match:
                        self.items, self.match = self.iter_group(), False
                    elif self.group.model == 'all':
                        self.group, self.items, self.match = self._groups.pop()
                    else:
                        item = self.group
                        if stop_item():
                            yield model_error_tuple()

        except IndexError:
            # Model visit ended
            self.element = None
            if self.group.model == 'all':
                yield from self._iter_all_model_errors(occurs)
            elif self.group.is_missing(occurs) or self.group.is_exceeded(occurs):
                yield self.group, occurs[self.group], self.expected

    def _iter_all_model_errors(self, occurs: OccursCounterType) -> Iterator[AdvanceYieldedType]:
        """Validate occurrences in an 'all' model, yielding error tuples."""
        stack: list[tuple[groups.XsdGroup, Iterator[ModelParticleType]]] = []
        group = self.group if self.group.ref is None else self.group.ref
        particles = iter(group)
        zero_missing: list[tuple[groups.XsdGroup, ModelParticleType]] = []

        while True:
            for item in particles:
                if occurs[item]:
                    occurs[group] = 1

                if isinstance(item, groups.XsdGroup):
                    if item.max_occurs == 0:
                        continue

                    stack.append((group, particles))
                    group = item
                    particles = iter(item.content)
                    if len(stack) > _limits.MAX_MODEL_DEPTH:
                        raise XMLSchemaModelDepthError(self.group)
                    break

                if item.is_missing(occurs) or item.is_exceeded(occurs):
                    if occurs[item]:
                        yield item, occurs[item], item.get_expected(occurs)
                    else:
                        zero_missing.append((group, item))
            else:
                if group.is_missing(occurs) or group.is_exceeded(occurs):
                    if occurs[group] or not stack:
                        yield group, occurs[group], group.get_expected(occurs)
                    else:
                        zero_missing.append((stack[-1][0], group))

                if not stack:
                    break
                group, particles = stack.pop()

        # Late check on missing items that never occurs
        for group, item in zero_missing:
            if occurs[group]:
                yield item, occurs[item], item.get_expected(occurs)

    # Kept for backward compatibility
    def iter_unordered_content(
            self, content: EncodedContentType,
            default_namespace: Optional[str] = None) -> Iterator[ContentItemType]:

        msg = f"{self.__class__.__name__}.iter_unordered_content() method will " \
              "be removed in v4.0, use iter_unordered_content() function instead."
        if default_namespace is not None:
            msg += " Don't provide default_namespace argument, it's ignored."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)

        return iter_unordered_content(content, self.root)

    def iter_collapsed_content(
            self, content: Iterable[ContentItemType],
            default_namespace: Optional[str] = None) -> Iterator[ContentItemType]:

        msg = f"{self.__class__.__name__}.iter_collapsed_content() method will " \
              "be removed in v4.0, use iter_collapsed_content() function instead."
        if default_namespace is not None:
            msg += " Don't provide default_namespace argument, it's ignored."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)

        return iter_collapsed_content(content, self.root)

    ###
    # Additional properties and methods, not used by validation. These methods can
    # be used ad helpers for a content model builder.

    def __copy__(self) -> 'ModelVisitor':
        model: 'ModelVisitor' = object.__new__(self.__class__)
        model.root = self.root
        model.element = self.element
        model.group = self.group
        model.match = self.match
        model.occurs = self.occurs.copy()

        # Can't copy iterators so create new ones and iter them at the same item
        model._groups = []
        group = self.group

        for parent, _items, match in reversed(self._groups):
            items = iter(parent if parent.ref is None else parent.ref)
            for obj in items:
                if obj is group:
                    model._groups.append((parent, items, match))
                    group = parent
                    break

        model._groups.reverse()

        model.items = model.iter_group()
        for obj in model.items:
            if obj is model.element:
                break

        return model

    @property
    def stoppable(self) -> bool:
        """Returns `True` if the model is stoppable from the current status without errors."""
        if self.element is None:
            return True

        model = copy(self)
        for _error in model.stop():
            return False
        else:
            return True

    def get_model_particle(self, particle: Optional[ModelParticleType] = None) \
            -> ModelParticleType:
        """
        Checks if the provided particle belongs to the current model, raising
        a `XMLSchemaModelError` in case if it's not. Defaults to current element
        if no particle is provided, raising a `XMLSchemaValueError` if the model
        is ended.
        """
        if particle is not None:
            for _subgroups in self.root.iter_subgroups(particle):
                break
            return particle
        elif self.element is not None:
            return self.element
        else:
            raise XMLSchemaValueError(f"can't defaults to current element, {self!r} is ended!")

    def overall_min_occurs(self, particle: Optional[ModelParticleType] = None) -> int:
        """
        Returns the overall min occurs of a particle in the model subtracting the
        occurrences already registered by the occurs counter. Defaults to current
        element.
        """
        result = []
        particle = self.get_model_particle(particle)

        for subgroups in self.root.iter_subgroups(particle):
            min_occurs = 1
            for group in subgroups:
                group_min_occurs = group.min_occurs - self.occurs[group]
                if group_min_occurs <= 0 or group.model == 'choice' and len(group) > 1:
                    result.append(0)
                    break
                min_occurs *= group_min_occurs
            else:
                result.append(min_occurs * particle.min_occurs - self.occurs[particle])

        return max(0, min(result))

    def overall_max_occurs(self, particle: Optional[ModelParticleType] = None) -> Optional[int]:
        """
        Returns the overall max occurs of a particle in the model subtracting the
        occurrences already registered by the occurs counter. Defaults to current
        element.
        """
        results = [0]
        particle = self.get_model_particle(particle)
        max_occurs: Optional[int]

        for subgroups in self.root.iter_subgroups(particle):
            max_occurs = 1
            for group in subgroups:
                group_max_occurs = group.max_occurs
                if group_max_occurs == 0:
                    results.append(0)
                    break
                elif max_occurs is None:
                    continue
                elif group_max_occurs is None:
                    max_occurs = None
                else:
                    group_max_occurs -= self.occurs[group]
                    if group_max_occurs <= 0:
                        results.append(0)
                        break
                    max_occurs *= group_max_occurs
            else:
                if particle.max_occurs == 0:
                    results.append(0)
                elif particle.max_occurs is None or max_occurs is None:
                    return None
                else:
                    results.append(max_occurs * particle.max_occurs - self.occurs[particle])

        return max(results)

    def is_optional(self, particle: Optional[ModelParticleType] = None) -> bool:
        """
        Tests if the particle can be omitted in the current model status.
        Defaults to current element.
        """
        particle = self.get_model_particle(particle)
        return self.overall_min_occurs(particle) == 0

    def is_missing(self, particle: Optional[ModelParticleType] = None) -> bool:
        """
        Tests if particle occurrences are under the minimum. If the argument is
        `None` then tests the current element.
        """
        return self.get_model_particle(particle).is_missing(self.occurs)

    def is_over(self, particle: Optional[ModelParticleType] = None) -> bool:
        """
        Tests if particle occurrences are equal or over the maximum. If the
        argument is `None` then tests the current element.
        """
        return self.get_model_particle(particle).is_over(self.occurs)

    def is_exceeded(self, particle: Optional[ModelParticleType] = None) -> bool:
        """
        Tests if particle occurrences are over the maximum. If the argument
        is `None` then tests the current element.
        """
        return self.get_model_particle(particle).is_exceeded(self.occurs)

    def advance_to(self, element: SchemaElementType) -> Iterator[AdvanceYieldedType]:
        """
        Advances to the XSD element of the model. Stops after an error in advancing.
        If the elements hasn't residual occurs or if the model ends before the XSD
        element is reached throws an `XMLSchemaValueError`.
        """
        if self.overall_max_occurs(element) == 0:
            raise XMLSchemaValueError(f"{self!r} hasn't residual occurs")

        _err: Optional[AdvanceYieldedType] = None
        while True:
            if _err is not None:
                return
            elif self.element is None:
                raise XMLSchemaValueError(f"can't advance, {self!r} is ended!")
            elif self.element is element:
                return
            else:
                for _err in self.advance(False):
                    yield _err

    def advance_until(self, target: Union[str, SchemaElementType],
                      occurs: int = 1) -> Iterator[AdvanceYieldedType]:
        """
        Advances until an element matching `target` is found. Stops after
        an error in advancing. If the model ends before the tag is found,
        it throws an `XMLSchemaValueError`.

        :param target: can be a tag or an XSD element/wildcard of the model.
        :param occurs: number of occurrences to consume for target element, \
        for default consumes one occurrence. The consumed occurrences can be \
        non-consecutive.
        """
        _err: Optional[AdvanceYieldedType] = None
        while True:
            if _err is not None:
                return
            elif self.element is None:
                raise XMLSchemaValueError(f"can't advance, {self!r} is ended!")
            elif isinstance(target, str):
                while self.match_element(target):
                    if occurs >= 1:
                        yield from self.advance(True)
                    occurs -= 1
                    if occurs <= 0:
                        return
                else:
                    for _err in self.advance(False):
                        yield _err
            else:
                while target is self.element:
                    if occurs >= 1:
                        yield from self.advance(True)
                    occurs -= 1
                    if occurs <= 0:
                        return
                else:
                    for _err in self.advance(False):
                        yield _err

    def check_following(self, *steps: StepType) -> bool:
        """
        Returns `True` if the model can be advanced without errors applying
        the provided sequence of steps.

        :param steps: sequence of steps to apply, each step can be an XSD element \
        of the model or a tag, or the same info coupled with a non-negative integer \
        that represents the occurs to be applied on the element (1 for default).
        """
        if not steps:
            raise XMLSchemaTypeError("at least one step must be provided")

        model = copy(self)
        for step in steps:
            target, occurs = step if isinstance(step, tuple) else (step, 1)

            try:
                for _err in model.advance_until(target, occurs):
                    return False
            except XMLSchemaValueError:
                return False
        else:
            return True

    def advance_safe(self, *steps: str) -> bool:
        """
        Advance the model with the provided sequence of steps if the advance doesn't
        produce errors or the ending of the model. Returns `True` if the advance has
        been done, `False` otherwise.
        """
        if not self.check_following(*steps):
            return False

        for step in steps:
            target, occurs = step if isinstance(step, tuple) else (step, 1)
            for _err in self.advance_until(target, occurs):
                raise XMLSchemaRuntimeError("Unexpected advance error")
        else:
            return True


class InterleavedModelVisitor(ModelVisitor):
    """
    A visitor for openContent interleaved models. Memorizes an internal state
    for deciding when to advance the model. The model doesn't advance if the
    last match_element() call is with the wildcard.
    """
    __slots__ = 'wildcard', '_advance_model'

    def __init__(self, root: ModelGroupType, wildcard: XsdAnyElement) -> None:
        super().__init__(root)
        self.wildcard = wildcard
        self._advance_model = True
        if self.element is None:
            self.element = wildcard

    def clear(self) -> None:
        super().clear()
        self._advance_model = True
        if self.element is None:
            self.element = self.wildcard

    def match_element(self, tag: str) -> Optional[SchemaElementType]:
        xsd_element = super().match_element(tag)
        if xsd_element is not None or self.element is self.wildcard:
            return xsd_element
        elif not self.wildcard.is_matching(tag, group=self.root, occurs=self.occurs):
            return None

        for xsd_element in self.group.elements:
            if xsd_element.is_matching(tag, group=self.root, occurs=self.occurs):
                if not xsd_element.is_over(self.occurs):
                    return None
        else:
            if self.wildcard.process_contents != 'strict' or tag in self.root.maps.elements:
                self._advance_model = False
                return self.wildcard
            return None

    def advance(self, match: bool = False) -> Iterator[AdvanceYieldedType]:
        if self.element is None:
            yield from super().advance(match)
        elif self.element is self.wildcard:
            if not match:
                self.element = None
        elif not self._advance_model:
            self._advance_model = True
        else:
            yield from super().advance(match)
            if self.element is None:
                self.element = self.wildcard


class SuffixedModelVisitor(ModelVisitor):
    """A visitor for openContent suffixed models."""

    __slots__ = 'wildcard',

    def __init__(self, root: ModelGroupType, wildcard: XsdAnyElement) -> None:
        super().__init__(root)
        self.wildcard = wildcard
        if self.element is None:
            self.element = wildcard

    def clear(self) -> None:
        super().clear()
        if self.element is None:
            self.element = self.wildcard

    def advance(self, match: bool = False) -> Iterator[AdvanceYieldedType]:
        if self.element is None:
            yield from super().advance(match)
        elif self.element is not self.wildcard:
            yield from super().advance(match)
            if self.element is None:
                self.element = self.wildcard
        elif not match:
            self.element = None


#
# Functions for manipulating encoded content

def iter_unordered_content(content: EncodedContentType, group: ModelGroupType) \
        -> Iterator[ContentItemType]:
    """
    Takes an unordered content stored in a dictionary of lists and yields the
    content elements sorted with the ordering defined by the model group. Character
    data parts are yielded at start and between child elements.

    Ordering is inferred from ModelVisitor instance with any elements that
    don't fit the schema placed at the end of the returned sequence. Checking
    the yielded content validity is the responsibility of method *iter_encode*
    of class :class:`XsdGroup`.

    :param content: a dictionary of element names to list of element contents \
    or an iterable composed of couples of name and value. In case of a \
    dictionary the values must be lists where each item is the content \
    of a single element.
    :param group: the model group related to content.
    """
    consumable_content: dict[str, Any]

    if isinstance(content, MutableMapping):
        cdata_content = sorted(
            ((k, v) for k, v in content.items() if isinstance(k, int)), reverse=True
        )
        consumable_content = {
            k: deque(v) if isinstance(v, MutableSequence) else deque([v])
            for k, v in content.items() if not isinstance(k, int)
        }
    else:
        cdata_content = sorted(((k, v) for k, v in content if isinstance(k, int)), reverse=True)
        consumable_content = defaultdict(deque)
        for k, v in content:
            if isinstance(k, str):
                consumable_content[k].append(v)

    if cdata_content:
        yield cdata_content.pop()

    model = ModelVisitor(group)
    while model.element is not None and consumable_content:  # pragma: no cover
        for name in consumable_content:
            if model.element.is_matching(name, group=group):
                yield name, consumable_content[name].popleft()
                if not consumable_content[name]:
                    del consumable_content[name]
                for _err in model.advance(True):
                    pass
                if cdata_content:
                    yield cdata_content.pop()
                break
        else:
            # Consume the return of advance otherwise we get stuck in an infinite loop.
            for _err in model.advance(False):
                pass

    # Add the remaining consumable content onto the end of the data.
    for name, values in consumable_content.items():
        for v in values:
            yield name, v
            if cdata_content:
                yield cdata_content.pop()

    while cdata_content:
        yield cdata_content.pop()


def sort_content(content: EncodedContentType, group: ModelGroupType) \
        -> list[ContentItemType]:
    return [x for x in iter_unordered_content(content, group)]


def iter_collapsed_content(content: Iterable[ContentItemType], group: ModelGroupType) \
        -> Iterator[ContentItemType]:
    """
    Iterates a content stored in a sequence of couples *(name, value)*, yielding
    items in the same order of the sequence, except for repetitions of the same
    tag that don't match with the current element of the :class:`ModelVisitor`
    instance. These items are included in an unsorted buffer and yielded asap
    when there is a match with the model's element or at the end of the iteration.

    This iteration mode, in cooperation with the method *iter_encode* of the class
    XsdGroup, facilitates the encoding of content formatted with a convention that
    collapses the children with the same tag into a list (e.g. BadgerFish).

    :param content: an iterable containing couples of names and values.
    :param group: the model group related to content.
    """
    prev_name = None
    unordered_content: dict[str, Any] = defaultdict(deque)

    model = ModelVisitor(group)
    for name, value in content:
        if isinstance(name, int) or model.element is None:
            yield name, value
            continue

        while model.element is not None:
            if model.element.is_matching(name, group=group):
                yield name, value
                prev_name = name
                for _err in model.advance(True):
                    pass
                break

            for key in unordered_content:
                if model.element.is_matching(key, group=group):
                    break
            else:
                if prev_name == name:
                    unordered_content[name].append(value)
                    break

                for _err in model.advance(False):
                    pass
                continue

            try:
                yield key, unordered_content[key].popleft()
            except IndexError:
                del unordered_content[key]
            else:
                for _err in model.advance(True):
                    pass
        else:
            yield name, value
            prev_name = name

    # Yields the remaining consumable content after the end of the data.
    for name, values in unordered_content.items():
        for v in values:
            yield name, v
