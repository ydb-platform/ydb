"""
This modules contains classes for creating conditional constraints.
"""
from typing import Optional, Sequence, Union

from click import Context, Parameter

from ._core import Constraint
from .conditions import AllSet, IsSet, Predicate
from .exceptions import ConstraintViolated
from .._util import make_repr


def as_predicate(arg: Union[str, Sequence[str], Predicate]) -> Predicate:
    if isinstance(arg, str):
        return IsSet(arg)
    elif isinstance(arg, Predicate):
        return arg
    elif isinstance(arg, Sequence):
        return AllSet(*arg)
    else:
        raise TypeError("`arg` should be a string, a list of strings or a `Predicate`")


class If(Constraint):
    """
    Checks one constraint or another depending on the truth value of the condition.

    .. versionadded:: 0.8.0
        you can now pass a sequence of parameter names as condition, which
        corresponds to the predicate ``AllSet(*param_names)``.

    :param condition:
        can be either an instance of ``Predicate`` or (more often) the name of a
        parameter or a list/tuple of parameters that must be all set for the
        condition to be true.
    :param then:
        a constraint checked if the condition is true.
    :param else_:
        an (optional) constraint checked if the condition is false.
    """

    def __init__(
        self,
        condition: Union[str, Sequence[str], Predicate],
        then: Constraint,
        else_: Optional[Constraint] = None,
    ):
        self._condition = as_predicate(condition)
        self._then = then
        self._else = else_

    def help(self, ctx: Context) -> str:
        condition = self._condition.description(ctx)
        then_help = self._then.help(ctx)
        else_help = self._else.help(ctx) if self._else else None
        if not self._else:
            return f"{then_help} if {condition}"
        else:
            return f"{then_help} if {condition}, otherwise {else_help}"

    def check_consistency(self, params: Sequence[Parameter]) -> None:
        self._then.check_consistency(params)
        if self._else:
            self._else.check_consistency(params)

    def check_values(self, params: Sequence[Parameter], ctx: Context) -> None:
        condition = self._condition
        condition_is_true = condition(ctx)
        branch = self._then if condition_is_true else self._else
        if branch is None:
            return
        try:
            branch.check_values(params, ctx=ctx)
        except ConstraintViolated as err:
            desc = (
                condition.description(ctx)
                if condition_is_true
                else condition.negated_description(ctx)
            )
            raise ConstraintViolated(
                f"when {desc}, {err}", ctx=ctx, constraint=self, params=params
            )

    def __repr__(self) -> str:
        if self._else:
            return make_repr(self, self._condition, then=self._then, else_=self._else)
        return make_repr(self, self._condition, then=self._then)
