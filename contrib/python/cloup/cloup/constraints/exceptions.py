from typing import Iterable, Sequence, TYPE_CHECKING

import click
from click import Context, Parameter

from .common import join_param_labels

if TYPE_CHECKING:
    from ._core import Constraint


def default_constraint_error(params: Iterable[Parameter], desc: str) -> str:
    return (
        'the following constraint on parameters [%s] was not satisfied: %s'
        % (join_param_labels(params), desc)
    )


class ConstraintViolated(click.UsageError):
    def __init__(
        self, message: str,
        ctx: Context,
        constraint: 'Constraint',
        params: Sequence[click.Parameter]
    ):
        super().__init__(message, ctx=ctx)
        self.ctx = ctx
        self.constraint = constraint
        self.params = params

    @classmethod
    def default(
        cls,
        desc: str,
        ctx: Context,
        constraint: 'Constraint',
        params: Sequence[Parameter],
    ) -> 'ConstraintViolated':
        return ConstraintViolated(
            default_constraint_error(params, desc),
            ctx=ctx, constraint=constraint, params=params,
        )


class UnsatisfiableConstraint(Exception):
    """Raised if a constraint cannot be satisfied by a group of parameters
    independently from their values at runtime; e.g. ``mutually_exclusive`` cannot
    be satisfied if multiple of the parameters are required."""

    def __init__(
        self, constraint: 'Constraint', params: Iterable[Parameter], reason: str
    ):
        self.constraint = constraint
        self.params = params
        self.reason = reason
        param_names = join_param_labels(params)
        message = (f"\nthe constraint {constraint}\n"
                   f"defined on parameters [{param_names}]\n"
                   f"cannot be satisfied because {reason}")
        super().__init__(message)
