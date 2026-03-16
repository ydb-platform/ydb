# Purpose: Query language and manipulation object for DXF entities
# Copyright (c) 2013-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    Callable,
    Hashable,
    Sequence,
    Union,
    Optional,
)
import re
import operator
from collections import abc

from ezdxf.entities.dxfentity import DXFEntity
from ezdxf.groupby import groupby
from ezdxf.math import Vec3, Vec2
from ezdxf.queryparser import EntityQueryParser


class _AttributeDescriptor:
    def __init__(self, name: str):
        self.name = name

    def __get__(self, obj, objtype=None):
        return obj.__getitem__(self.name)

    def __set__(self, obj, value):
        obj.__setitem__(self.name, value)

    def __delete__(self, obj):
        obj.__delitem__(self.name)


class EntityQuery(abc.Sequence):
    """EntityQuery is a result container, which is filled with dxf entities
    matching the query string. It is possible to add entities to the container
    (extend), remove entities from the container and to filter the container.

    Query String
    ============

    QueryString := EntityQuery ("[" AttribQuery "]")*

    The query string is the combination of two queries, first the required
    entity query and second the optional attribute query, enclosed in square
    brackets.

    Entity Query
    ------------

    The entity query is a whitespace separated list of DXF entity names or the
    special name ``*``. Where ``*`` means all DXF entities, exclude some entity
    types by appending their names with a preceding ``!`` (e.g. all entities
    except LINE = ``* !LINE``). All DXF names have to be uppercase.

    Attribute Query
    ---------------

    The attribute query is used to select DXF entities by its DXF attributes.
    The attribute query is an addition to the entity query and matches only if
    the entity already match the entity query.
    The attribute query is a boolean expression, supported operators are:

      - not: !term is true, if term is false
      - and: term & term is true, if both terms are true
      - or: term | term is true, if one term is true
      - and arbitrary nested round brackets

    Attribute selection is a term: "name comparator value", where name is a DXF
    entity attribute in lowercase, value is a integer, float or double quoted
    string, valid comparators are:

      - "==" equal "value"
      - "!=" not equal "value"
      - "<" lower than "value"
      - "<=" lower or equal than "value"
      - ">" greater than "value"
      - ">=" greater or equal than "value"
      - "?" match regular expression "value"
      - "!?" does not match regular expression "value"

    Query Result
    ------------

    The EntityQuery() class based on the abstract Sequence() class, contains all
    DXF entities of the source collection, which matches one name of the entity
    query AND the whole attribute query. If a DXF entity does not have or
    support a required attribute, the corresponding attribute search term is
    false.

    Examples:

        - 'LINE[text ? ".*"]' is always empty, because the LINE entity has no
          text attribute.
        - 'LINE CIRCLE[layer=="construction"]' => all LINE and CIRCLE entities
          on layer "construction"
        - '*[!(layer=="construction" & color<7)]' => all entities except those
          on layer == "construction" and color < 7

    """

    layer = _AttributeDescriptor("layer")
    color = _AttributeDescriptor("color")
    linetype = _AttributeDescriptor("linetype")
    lineweight = _AttributeDescriptor("lineweight")
    ltscale = _AttributeDescriptor("ltscale")
    invisible = _AttributeDescriptor("invisible")
    true_color = _AttributeDescriptor("true_color")
    transparency = _AttributeDescriptor("transparency")

    def __init__(
        self, entities: Optional[Iterable[DXFEntity]] = None, query: str = "*"
    ):
        """
        Setup container with entities matching the initial query.

        Args:
            entities: sequence of wrapped DXF entities (at least GraphicEntity class)
            query: query string, see class documentation

        """
        # Selected DXF attribute for operator selection:
        self.selected_dxf_attribute: str = ""
        # Text selection mode, but only for operator comparisons:
        self.ignore_case = True

        self.entities: list[DXFEntity]
        if entities is None:
            self.entities = []
        elif query == "*":
            self.entities = list(entities)
        else:
            match = entity_matcher(query)
            self.entities = [entity for entity in entities if match(entity)]

    def __len__(self) -> int:
        """Returns count of DXF entities."""
        return len(self.entities)

    def __iter__(self) -> Iterator[DXFEntity]:
        """Returns iterable of DXFEntity objects."""
        return iter(self.entities)

    def __getitem__(self, item):
        """Returns DXFEntity at index `item`, supports negative indices and
        slicing. Returns all entities which support a specific DXF attribute,
        if `item` is a DXF attribute name as string.
        """
        if isinstance(item, str):
            return self._get_entities_with_supported_attribute(item)
        return self.entities.__getitem__(item)

    def __setitem__(self, key, value):
        """Set the DXF attribute `key` for all supported DXF entities to `value`.
        """
        if not isinstance(key, str):
            raise TypeError("key has to be a string (DXF attribute name)")
        self._set_dxf_attribute_for_all(key, value)

    def __delitem__(self, key):
        """Discard the DXF attribute `key` from all supported DXF entities."""
        if not isinstance(key, str):
            raise TypeError("key has to be a string (DXF attribute name)")
        self._discard_dxf_attribute_for_all(key)

    def purge(self) -> EntityQuery:
        """Remove destroyed entities."""
        self.entities = [e for e in self.entities if e.is_alive]
        return self  # fluent interface

    def _get_entities_with_supported_attribute(
        self, attribute: str
    ) -> EntityQuery:
        query = self.__class__(
            e for e in self.entities if e.dxf.is_supported(attribute)
        )
        query.selected_dxf_attribute = attribute
        return query

    def _set_dxf_attribute_for_all(self, key, value):
        for e in self.entities:
            try:
                e.dxf.set(key, value)
            except AttributeError:  # ignore unsupported attributes
                pass
            # But raises ValueError/TypeError for invalid values!

    def _discard_dxf_attribute_for_all(self, key):
        for e in self.entities:
            e.dxf.discard(key)

    def __eq__(self, other):
        """Equal selector (self == other).
        Returns all entities where the selected DXF attribute is equal to
        `other`.
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.eq)

    def __ne__(self, other):
        """Not equal selector (self != other). Returns all entities where the
        selected DXF attribute is not equal to `other`.
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.ne)

    def __lt__(self, other):
        """Less than selector (self < other). Returns all entities where the
        selected DXF attribute is less than `other`.

        Raises:
             TypeError: for vector based attributes like `center` or `insert`
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.lt, vectors=False)

    def __gt__(self, other):
        """Greater than selector (self > other). Returns all entities where the
        selected DXF attribute is greater than `other`.

        Raises:
             TypeError: for vector based attributes like `center` or `insert`
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.gt, vectors=False)

    def __le__(self, other):
        """Less equal selector (self <= other). Returns all entities where the
        selected DXF attribute is less or equal `other`.

        Raises:
             TypeError: for vector based attributes like `center` or `insert`
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.le, vectors=False)

    def __ge__(self, other):
        """Greater equal selector (self >= other). Returns all entities where
        the selected DXF attribute is greater or equal `other`.

        Raises:
             TypeError: for vector based attributes like `center` or `insert`
        """
        if not self.selected_dxf_attribute:
            raise TypeError("no DXF attribute selected")
        return self._select_by_operator(other, operator.ge, vectors=False)

    def __or__(self, other):
        """Union operator, see :meth:`union`."""
        if isinstance(other, EntityQuery):
            return self.union(other)
        return NotImplemented

    def __and__(self, other):
        """Intersection operator, see :meth:`intersection`."""
        if isinstance(other, EntityQuery):
            return self.intersection(other)
        return NotImplemented

    def __sub__(self, other):
        """Difference operator, see :meth:`difference`."""
        if isinstance(other, EntityQuery):
            return self.difference(other)
        return NotImplemented

    def __xor__(self, other):
        """Symmetric difference operator, see :meth:`symmetric_difference`."""
        if isinstance(other, EntityQuery):
            return self.symmetric_difference(other)
        return NotImplemented

    def _select_by_operator(self, value, op, vectors=True) -> EntityQuery:
        attribute = self.selected_dxf_attribute
        if self.ignore_case and isinstance(value, str):
            value = value.lower()

        query = self.__class__()
        query.selected_dxf_attribute = attribute
        entities = query.entities
        if attribute:
            for entity in self.entities:
                try:
                    entity_value = entity.dxf.get_default(attribute)
                except AttributeError:
                    continue
                if not vectors and isinstance(entity_value, (Vec2, Vec3)):
                    raise TypeError(
                        f"unsupported operation '{str(op.__name__)}' for DXF "
                        f"attribute {attribute}"
                    )
                if self.ignore_case and isinstance(entity_value, str):
                    entity_value = entity_value.lower()
                if op(entity_value, value):
                    entities.append(entity)
        return query

    def match(self, pattern: str) -> EntityQuery:
        """Returns all entities where the selected DXF attribute matches the
        regular expression `pattern`.

        Raises:
             TypeError: for non-string based attributes

        """

        def match(value, regex):
            if isinstance(value, str):
                return regex.match(value) is not None
            raise TypeError(
                f"cannot apply regular expression to DXF attribute: "
                f"{self.selected_dxf_attribute}"
            )

        return self._regex_match(pattern, match)

    def _regex_match(self, pattern: str, func) -> EntityQuery:
        ignore_case = self.ignore_case
        self.ignore_case = False  # deactivate string manipulation
        re_flags = re.IGNORECASE if ignore_case else 0

        # always match whole pattern
        if not pattern.endswith("$"):
            pattern += "$"
        result = self._select_by_operator(
            re.compile(pattern, flags=re_flags), func
        )
        self.ignore_case = ignore_case  # restore state
        return result

    @property
    def first(self):
        """First entity or ``None``."""
        if len(self.entities):
            return self.entities[0]
        else:
            return None

    @property
    def last(self):
        """Last entity or ``None``."""
        if len(self.entities):
            return self.entities[-1]
        else:
            return None

    def extend(
        self,
        entities: Iterable[DXFEntity],
        query: str = "*",
    ) -> EntityQuery:
        """Extent the :class:`EntityQuery` container by entities matching an
        additional query.

        """
        self.entities = self.union(self.__class__(entities, query)).entities
        return self  # fluent interface

    def remove(self, query: str = "*") -> EntityQuery:
        """Remove all entities from :class:`EntityQuery` container matching this
        additional query.

        """
        self.entities = self.difference(
            self.__class__(self.entities, query)
        ).entities
        return self  # fluent interface

    def query(self, query: str = "*") -> EntityQuery:
        """Returns a new :class:`EntityQuery` container with all entities
        matching this additional query.

        Raises:
            pyparsing.ParseException: query string parsing error

        """
        return self.__class__(self.entities, query)

    def groupby(
        self,
        dxfattrib: str = "",
        key: Optional[Callable[[DXFEntity], Hashable]] = None,
    ) -> dict[Hashable, list[DXFEntity]]:
        """Returns a dict of entity lists, where entities are grouped by a DXF
        attribute or a key function.

        Args:
            dxfattrib: grouping DXF attribute as string like ``'layer'``
            key: key function, which accepts a DXFEntity as argument, returns
                grouping key of this entity or ``None`` for ignore this object.
                Reason for ignoring: a queried DXF attribute is not supported by
                this entity

        """
        return groupby(self.entities, dxfattrib, key)

    def filter(self, func: Callable[[DXFEntity], bool]) -> EntityQuery:
        """Returns a new :class:`EntityQuery` with all entities from this
        container for which the callable `func` returns ``True``.

        Build your own operator to filter by attributes which are not DXF
        attributes or to build complex queries::

            result = msp.query().filter(
                lambda e: hasattr(e, "rgb") and e.rbg == (0, 0, 0)
            )
        """
        return self.__class__(filter(func, self.entities))

    def union(self, other: EntityQuery) -> EntityQuery:
        """Returns a new :class:`EntityQuery` with entities from `self` and
        `other`. All entities are unique - no duplicates.
        """
        return self.__class__(set(self.entities) | set(other.entities))

    def intersection(self, other: EntityQuery) -> EntityQuery:
        """Returns a new :class:`EntityQuery` with entities common to `self`
        and `other`.
        """
        return self.__class__(set(self.entities) & set(other.entities))

    def difference(self, other: EntityQuery) -> EntityQuery:
        """Returns a new :class:`EntityQuery` with all entities from `self` that
        are not in `other`.
        """
        return self.__class__(set(self.entities) - set(other.entities))

    def symmetric_difference(self, other: EntityQuery) -> EntityQuery:
        """Returns a new :class:`EntityQuery` with entities in either `self` or
        `other` but not both.
        """
        return self.__class__(set(self.entities) ^ set(other.entities))


def entity_matcher(query: str) -> Callable[[DXFEntity], bool]:
    query_args = EntityQueryParser.parseString(query, parseAll=True)
    entity_matcher_ = build_entity_name_matcher(query_args.EntityQuery)
    attrib_matcher = build_entity_attributes_matcher(
        query_args.AttribQuery, query_args.AttribQueryOptions
    )

    def matcher(entity: DXFEntity) -> bool:
        return entity_matcher_(entity) and attrib_matcher(entity)

    return matcher


def build_entity_name_matcher(
    names: Sequence[str],
) -> Callable[[DXFEntity], bool]:
    def match(e: DXFEntity) -> bool:
        return _match(e.dxftype())

    _match = name_matcher(query=" ".join(names))
    return match


class Relation:
    CMP_OPERATORS = {
        "==": operator.eq,
        "!=": operator.ne,
        "<": operator.lt,
        "<=": operator.le,
        ">": operator.gt,
        ">=": operator.ge,
        "?": lambda e, regex: regex.match(e) is not None,
        "!?": lambda e, regex: regex.match(e) is None,
    }
    VALID_CMP_OPERATORS = frozenset(CMP_OPERATORS.keys())

    def __init__(self, relation: Sequence, ignore_case: bool):
        name, op, value = relation
        self.dxf_attrib = name
        self.compare = Relation.CMP_OPERATORS[op]
        self.convert_case = to_lower if ignore_case else lambda x: x

        re_flags = re.IGNORECASE if ignore_case else 0
        if "?" in op:
            self.value = re.compile(
                value + "$", flags=re_flags
            )  # always match whole pattern
        else:
            self.value = self.convert_case(value)

    def evaluate(self, entity: DXFEntity) -> bool:
        try:
            value = self.convert_case(entity.dxf.get_default(self.dxf_attrib))
            return self.compare(value, self.value)
        except AttributeError:  # entity does not support this attribute
            return False
        except ValueError:  # entity supports this attribute, but has no value for it
            return False


def to_lower(value):
    return value.lower() if hasattr(value, "lower") else value


class BoolExpression:
    OPERATORS = {
        "&": operator.and_,
        "|": operator.or_,
    }

    def __init__(self, tokens: Sequence):
        self.tokens = tokens

    def __iter__(self):
        return iter(self.tokens)

    def evaluate(self, entity: DXFEntity) -> bool:
        if isinstance(
            self.tokens, Relation
        ):  # expression is just one relation, no bool operations
            return self.tokens.evaluate(entity)

        values = []  # first in, first out
        operators = []  # first in, first out
        for token in self.tokens:
            if hasattr(token, "evaluate"):
                values.append(token.evaluate(entity))
            else:  # bool operator
                operators.append(token)
        values.reverse()
        for op in operators:  # as queue -> first in, first out
            if op == "!":
                value = not values.pop()
            else:
                value = BoolExpression.OPERATORS[op](values.pop(), values.pop())
            values.append(value)
        return values.pop()


def _compile_tokens(
    tokens: Union[str, Sequence], ignore_case: bool
) -> Union[str, Relation, BoolExpression]:
    def is_relation(tokens: Sequence) -> bool:
        return len(tokens) == 3 and tokens[1] in Relation.VALID_CMP_OPERATORS

    if isinstance(tokens, str):  # bool operator as string
        return tokens

    tokens = tuple(tokens)
    if is_relation(tokens):
        return Relation(tokens, ignore_case)
    else:
        return BoolExpression(
            [_compile_tokens(token, ignore_case) for token in tokens]
        )


def build_entity_attributes_matcher(
    tokens: Sequence, options: str
) -> Callable[[DXFEntity], bool]:
    if not len(tokens):
        return lambda x: True
    ignore_case = "i" == options  # at this time just one option is supported
    expr = BoolExpression(_compile_tokens(tokens, ignore_case))  # type: ignore

    def match_bool_expr(entity: DXFEntity) -> bool:
        return expr.evaluate(entity)

    return match_bool_expr


def unique_entities(entities: Iterable[DXFEntity]) -> Iterator[DXFEntity]:
    """Yield all unique entities, order of all entities will be preserved."""
    done: set[DXFEntity] = set()
    for entity in entities:
        if entity not in done:
            done.add(entity)
            yield entity


def name_query(names: Iterable[str], query: str = "*") -> Iterator[str]:
    """Filters `names` by `query` string. The `query` string of entity names
    divided by spaces. The special name "*" matches any given name, a
    preceding "!" means exclude this name. Excluding names is only useful if
    the match any name is also given (e.g. "LINE !CIRCLE" is equal to just
    "LINE", where "* !CIRCLE" matches everything except CIRCLE").

    Args:
        names: iterable of names to test
        query: query string of entity names separated by spaces

    Returns: yield matching names

    """
    match = name_matcher(query)
    return (name for name in names if match(name))


def name_matcher(query: str = "*") -> Callable[[str], bool]:
    def match(e: str) -> bool:
        if take_all:
            return e not in exclude
        else:
            return e in include

    match_strings = set(query.upper().split())
    take_all = False
    exclude = set()
    include = set()
    for name in match_strings:
        if name == "*":
            take_all = True
        elif name.startswith("!"):
            exclude.add(name[1:])
        else:
            include.add(name)

    return match


def new(
    entities: Optional[Iterable[DXFEntity]] = None, query: str = "*"
) -> EntityQuery:
    """Start a new query based on sequence `entities`. The `entities` argument
    has to be an iterable of :class:`~ezdxf.entities.DXFEntity` or inherited
    objects and returns an :class:`EntityQuery` object.

    """
    return EntityQuery(entities, query)
