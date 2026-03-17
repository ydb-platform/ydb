"""
Constraints for parameter groups.

.. versionadded:: v0.5.0
"""

from ._conditional import If
from ._core import (
    AcceptAtMost,
    AcceptBetween,
    And,
    Constraint,
    ErrorFmt,
    ErrorRephraser,
    HelpRephraser,
    Operator,
    Or,
    Rephraser,
    RequireAtLeast,
    RequireExactly,
    WrapperConstraint,
    accept_none,
    all_or_none,
    mutually_exclusive,
    require_all,
    require_any,
    require_one,
)
from ._support import (
    BoundConstraintSpec, ConstraintMixin, constrained_params, constraint
)
from .conditions import AllSet, AnySet, Equal, IsSet, Not
from .exceptions import ConstraintViolated, UnsatisfiableConstraint

__all__ = [
    "AcceptAtMost",
    "AcceptBetween",
    "AllSet",
    "And",
    "AnySet",
    "BoundConstraintSpec",
    "Constraint",
    "ConstraintMixin",
    "ConstraintViolated",
    "Equal",
    "ErrorFmt",
    "ErrorRephraser",
    "HelpRephraser",
    "If",
    "IsSet",
    "Not",
    "Operator",
    "Or",
    "Rephraser",
    "RequireAtLeast",
    "RequireExactly",
    "UnsatisfiableConstraint",
    "WrapperConstraint",
    "accept_none",
    "all_or_none",
    "constrained_params",
    "constraint",
    "mutually_exclusive",
    "require_all",
    "require_any",
    "require_one",
]
