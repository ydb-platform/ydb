# pyright: reportUnusedImport=false
# ruff: noqa: F401
from __future__ import annotations as _annotations

from importlib.util import find_spec
from typing import TYPE_CHECKING

from logfire.variables.abstract import (
    ResolvedVariable,
    SyncMode,
    ValidationReport,
    VariableAlreadyExistsError,
    VariableNotFoundError,
    VariableWriteError,
)

if TYPE_CHECKING:
    # We use a TYPE_CHECKING block here because we need to do these imports lazily to prevent issues due to loading the
    # logfire pydantic plugin.
    # If you change the imports here, you need to update the __getattr__ definition below to match.
    from logfire.variables.config import (
        KeyIsNotPresent,
        KeyIsPresent,
        LabeledValue,
        LabelRef,
        LatestVersion,
        LocalVariablesOptions,
        Rollout,
        RolloutOverride,
        ValueDoesNotEqual,
        ValueDoesNotMatchRegex,
        ValueEquals,
        ValueIsIn,
        ValueIsNotIn,
        ValueMatchesRegex,
        VariableConfig,
        VariablesConfig,
        VariableTypeConfig,
    )
    from logfire.variables.variable import (
        ResolveFunction,
        Variable,
        targeting_context,
    )

__all__ = [
    # Variable classes
    'Variable',
    'ResolvedVariable',
    'ResolveFunction',
    # Configuration classes
    'VariablesConfig',
    'VariableConfig',
    'VariableTypeConfig',
    'LocalVariablesOptions',
    # Label and rollout configuration
    'LabeledValue',
    'LabelRef',
    'LatestVersion',
    'Rollout',
    'RolloutOverride',
    # Targeting conditions
    'KeyIsPresent',
    'KeyIsNotPresent',
    'ValueEquals',
    'ValueDoesNotEqual',
    'ValueIsIn',
    'ValueIsNotIn',
    'ValueMatchesRegex',
    'ValueDoesNotMatchRegex',
    # Context managers and utilities
    'targeting_context',
    # Types
    'SyncMode',
    'ValidationReport',
    # Exceptions
    'VariableAlreadyExistsError',
    'VariableNotFoundError',
    'VariableWriteError',
]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

    if not find_spec('pydantic'):  # pragma: no cover
        raise ImportError(
            'Using managed variables requires the `pydantic` package.\n'
            'You can install this with:\n'
            "    pip install 'logfire[variables]'"
        )

    from logfire.variables.config import (
        KeyIsNotPresent,
        KeyIsPresent,
        LabeledValue,
        LabelRef,
        LatestVersion,
        LocalVariablesOptions,
        Rollout,
        RolloutOverride,
        ValueDoesNotEqual,
        ValueDoesNotMatchRegex,
        ValueEquals,
        ValueIsIn,
        ValueIsNotIn,
        ValueMatchesRegex,
        VariableConfig,
        VariablesConfig,
        VariableTypeConfig,
    )
    from logfire.variables.variable import (
        ResolveFunction,
        Variable,
        targeting_context,
    )

    return locals()[name]
