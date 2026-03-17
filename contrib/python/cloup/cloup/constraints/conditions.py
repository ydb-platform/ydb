"""
This modules contains predicates with an associated description that you can use
as conditions of conditional constraints (see :class:`cloup.constraints.If`).

Predicates should be treated as immutable objects, even though immutability
is not (at the moment) enforced.
"""
import abc
from typing import Any, Dict, Generic, TypeVar

import click

from ._support import ensure_constraints_support
from .common import (
    get_param_labels,
    get_param_name,
    join_with_and,
    param_label_by_name,
    param_value_by_name,
    param_value_is_set,
)
from .._util import make_repr

P = TypeVar('P', bound='Predicate')


class Predicate(abc.ABC):
    """
    A ``Callable`` that takes a ``click.Context`` and returns a boolean, with an
    associated description. Meant to be used as condition in a conditional
    constraint (see :class:`~cloup.constraints.If`).
    """

    @abc.abstractmethod
    def description(self, ctx: click.Context) -> str:
        """Succinct description of the predicate (alias: `desc`)."""

    def negated_description(self, ctx: click.Context) -> str:
        """Succinct description of the negation of this predicate (alias: `neg_desc`)."""
        return 'NOT(%s)' % self.description(ctx)

    def desc(self, ctx: click.Context) -> str:
        """Short alias for :meth:`description`."""
        return self.description(ctx)

    def neg_desc(self, ctx: click.Context) -> str:
        """Short alias for :meth:`negated_description`."""
        return self.negated_description(ctx)

    def negated(self) -> "Predicate":
        return ~self

    @abc.abstractmethod
    def __call__(self, ctx: click.Context) -> bool:
        """Evaluate the predicate on the given context."""

    def __invert__(self) -> "Predicate":
        return Not(self)

    def __or__(self, other: "Predicate") -> "Predicate":
        return _Or(self, other)

    def __and__(self, other: "Predicate") -> "Predicate":
        return _And(self, other)

    def __repr__(self) -> str:
        return make_repr(self, *self._public_fields().values())

    def _public_fields(self) -> Dict[str, Any]:
        return {k: v for k, v in vars(self).items() if not k.startswith('_')}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and (
            self._public_fields() == other._public_fields()
        )


class Not(Predicate, Generic[P]):
    """Logical NOT of a predicate."""

    def __init__(self, predicate: P):
        self.predicate = predicate

    def description(self, ctx: click.Context) -> str:
        return self.predicate.negated_description(ctx)

    def negated_description(self, ctx: click.Context) -> str:
        return self.predicate.description(ctx)

    def __call__(self, ctx: click.Context) -> bool:
        return not self.predicate(ctx)

    def __invert__(self) -> P:
        return self.predicate

    def __repr__(self) -> str:
        return 'Not(%r)' % self.predicate


class _Operator(Predicate, metaclass=abc.ABCMeta):
    """Operator between two or more predicates."""
    DESC_SEP: str

    def __init__(self, *predicates: Predicate):
        if len(predicates) < 2:
            raise ValueError('provide at least 2 predicates')
        self.predicates = predicates

    def description(self, ctx: click.Context) -> str:
        return self.DESC_SEP.join(
            '(%s)' % p.description(ctx) if isinstance(p, _Operator)
            else p.description(ctx)
            for p in self.predicates
        )

    def __repr__(self) -> str:
        return make_repr(self, *self.predicates)


class _And(_Operator):
    """Logical AND of two or more predicates."""
    DESC_SEP = ' and '

    def negated_description(self, ctx: click.Context) -> str:
        return ' or '.join(
            '(%s)' % p.neg_desc(ctx) if isinstance(p, _Operator) else p.neg_desc(ctx)
            for p in self.predicates
        )

    def __call__(self, ctx: click.Context) -> bool:
        return all(p(ctx) for p in self.predicates)

    def __and__(self, other: 'Predicate') -> Predicate:
        if isinstance(other, _And):
            return _And(*self.predicates, *other.predicates)
        return _And(*self.predicates, other)


class _Or(_Operator):
    """Logical OR of two or more predicates."""
    DESC_SEP = ' or '

    def negated_description(self, ctx: click.Context) -> str:
        return ' and '.join(
            '(%s)' % p.neg_desc(ctx) if isinstance(p, _Operator) else p.neg_desc(ctx)
            for p in self.predicates
        )

    def __call__(self, ctx: click.Context) -> bool:
        return any(p(ctx) for p in self.predicates)

    def __or__(self, other: 'Predicate') -> Predicate:
        if isinstance(other, _Or):
            return _Or(*self.predicates, *other.predicates)
        return _Or(*self.predicates, other)


class IsSet(Predicate):
    """True if the parameter is set."""

    def __init__(self, param_name: str):
        self.param_name = param_name

    def description(self, ctx: click.Context) -> str:
        return '%s is set' % param_label_by_name(ctx, self.param_name)

    def negated_description(self, ctx: click.Context) -> str:
        return '%s is not set' % param_label_by_name(ctx, self.param_name)

    def __call__(self, ctx: click.Context) -> bool:
        command = ensure_constraints_support(ctx.command)
        param = command.get_param_by_name(self.param_name)
        value = param_value_by_name(ctx, self.param_name)
        return param_value_is_set(param, value)

    def __and__(self, other: Predicate) -> Predicate:
        if isinstance(other, IsSet):
            return AllSet(self.param_name, other.param_name)
        return super().__and__(other)

    def __or__(self, other: Predicate) -> Predicate:
        if isinstance(other, IsSet):
            return AnySet(self.param_name, other.param_name)
        return super().__or__(other)


class AllSet(Predicate):
    """True if all listed parameters are set.

    .. versionadded:: 0.8.0
    """

    def __init__(self, *param_names: str):
        if not param_names:
            raise ValueError('you must provide at least one param name')
        self.param_names = param_names

    def negated_description(self, ctx: click.Context) -> str:
        labels = get_param_labels(ctx, self.param_names)
        if len(labels) == 1:
            return f'{labels[0]} is not set'
        pronoun = 'both' if len(labels) == 2 else 'all'
        return f'{join_with_and(labels)} are not {pronoun} set'

    def description(self, ctx: click.Context) -> str:
        labels = get_param_labels(ctx, self.param_names)
        if len(labels) == 1:
            return f'{labels[0]} is set'
        pronoun = 'both' if len(labels) == 2 else 'all'
        return f'{join_with_and(labels)} are {pronoun} set'

    def __call__(self, ctx: click.Context) -> bool:
        command = ensure_constraints_support(ctx.command)
        params = command.get_params_by_name(self.param_names)
        return all(param_value_is_set(param, ctx.params[get_param_name(param)])
                   for param in params)

    def __and__(self, other: Predicate) -> Predicate:
        if isinstance(other, AllSet):
            return AllSet(*self.param_names, *other.param_names)
        return super().__and__(other)


class AnySet(Predicate):
    """True if any of the listed parameters is set.

    .. versionadded:: 0.8.0
    """

    def __init__(self, *param_names: str):
        if not param_names:
            raise ValueError('you must provide at least one param name')
        self.param_names = param_names

    def negated_description(self, ctx: click.Context) -> str:
        labels = get_param_labels(ctx, self.param_names)
        if len(labels) == 1:
            return f'{labels[0]} is not set'
        if len(labels) == 2:
            return 'neither {} nor {} is set'.format(*labels)
        return f'none of {join_with_and(labels)} is set'

    def description(self, ctx: click.Context) -> str:
        labels = get_param_labels(ctx, self.param_names)
        if len(labels) == 1:
            return f'{labels[0]} is set'
        if len(labels) == 2:
            return 'either {} or {} is set'.format(*labels)
        return f'any of {join_with_and(labels)} is set'

    def __call__(self, ctx: click.Context) -> bool:
        command = ensure_constraints_support(ctx.command)
        params = command.get_params_by_name(self.param_names)
        return any(param_value_is_set(param, ctx.params[get_param_name(param)])
                   for param in params)

    def __or__(self, other: Predicate) -> Predicate:
        if isinstance(other, AnySet):
            return AnySet(*self.param_names, *other.param_names)
        return super().__or__(other)


class Equal(Predicate):
    """True if the parameter value equals ``value``."""

    def __init__(self, param_name: str, value: Any):
        self.param_name = param_name
        self.value = value

    def description(self, ctx: click.Context) -> str:
        param_label = param_label_by_name(ctx, self.param_name)
        return f'{param_label}="{self.value}"'

    def negated_description(self, ctx: click.Context) -> str:
        param_label = param_label_by_name(ctx, self.param_name)
        return f'{param_label}!="{self.value}"'

    def __call__(self, ctx: click.Context) -> bool:
        return param_value_by_name(ctx, self.param_name) == self.value  # type: ignore
