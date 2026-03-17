import logfire
from _typeshed import Incomplete
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from logfire.variables.abstract import ResolvedVariable
from logfire.variables.config import VariableConfig
from typing import Any, Generic, Protocol, TypeVar
from typing_extensions import TypeIs

__all__ = ['ResolveFunction', 'is_resolve_function', 'Variable', 'targeting_context']

T_co = TypeVar('T_co', covariant=True)

@dataclass
class _TargetingContextData:
    """Internal data structure for targeting context."""
    default: str | None = ...
    by_variable: dict[str, str] = field(default_factory=dict[str, str])

class ResolveFunction(Protocol[T_co]):
    """Protocol for functions that resolve variable values based on context."""
    def __call__(self, targeting_key: str | None, attributes: Mapping[str, Any] | None) -> T_co:
        """Resolve the variable value given a targeting key and attributes."""

def is_resolve_function(f: Any) -> TypeIs[ResolveFunction[Any]]:
    """Check if a callable matches the ResolveFunction signature.

    A resolve function is any callable that can be called with exactly two positional arguments
    (targeting_key and attributes). This includes:
    - Functions/lambdas with exactly 2 parameters
    - Functions with 2+ parameters where all after the first 2 have defaults
    - Functions with *args or **kwargs

    Args:
        f: The object to check.

    Returns:
        True if the callable can be invoked with two positional arguments.
    """

class Variable(Generic[T_co]):
    """A managed variable that can be resolved dynamically based on configuration."""
    name: str
    value_type: type[T_co]
    default: T_co | ResolveFunction[T_co]
    description: str | None
    logfire_instance: logfire.Logfire
    type_adapter: Incomplete
    def __init__(self, name: str, *, type: type[T_co], default: T_co | ResolveFunction[T_co], description: str | None = None, logfire_instance: logfire.Logfire) -> None:
        """Create a new managed variable.

        Args:
            name: Unique name identifying this variable.
            type: The expected type of this variable's values, used for validation.
            default: Default value to use when no configuration is found, or a function
                that computes the default based on targeting_key and attributes.
            description: Optional human-readable description of what this variable controls.
            logfire_instance: The Logfire instance this variable is associated with. Used to determine config, etc.
        """
    @contextmanager
    def override(self, value: T_co | ResolveFunction[T_co]) -> Iterator[None]:
        """Context manager to temporarily override this variable's value.

        Args:
            value: The value to use within this context, or a function that computes
                the value based on targeting_key and attributes.
        """
    async def refresh(self, force: bool = False):
        """Asynchronously refresh the variable."""
    def refresh_sync(self, force: bool = False):
        """Synchronously refresh the variable."""
    def get(self, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None, *, label: str | None = None) -> ResolvedVariable[T_co]:
        """Resolve the variable and return full details including label, version, and any errors.

        Args:
            targeting_key: Optional key for deterministic label selection (e.g., user ID).
                If not provided, falls back to contextvar targeting key (set via targeting_context),
                then to the current trace ID if there is an active trace.
            attributes: Optional attributes for condition-based targeting rules.
            label: Optional explicit label name to select. If provided, bypasses rollout
                weights and targeting, directly selecting the specified label. If the label
                doesn't exist in the configuration, falls back to default resolution.

        Returns:
            A ResolvedVariable object containing the resolved value, selected label,
            version, and any errors that occurred.
        """
    def to_config(self) -> VariableConfig:
        """Create a VariableConfig from this Variable instance.

        This creates a minimal config with just the name, schema, and example.
        No labels or versions are created - use this to generate a template config that can be edited.

        Returns:
            A VariableConfig with minimal configuration.
        """

@contextmanager
def targeting_context(targeting_key: str, variables: Sequence[Variable[Any]] | None = None) -> Iterator[None]:
    '''Set the targeting key for variable resolution within this context.

    The targeting key is used for deterministic label selection - the same targeting key
    will always resolve to the same label for a given variable configuration.

    Args:
        targeting_key: The targeting key to use for deterministic label selection
            (e.g., user ID, organization ID).
        variables: If provided, only apply this targeting key to these specific variables.
            If not provided, this becomes the default targeting key for all variables.

    Variable-specific targeting always takes precedence over the default, regardless
    of nesting order. Call-site explicit targeting_key still wins over everything.

    Example:
        # Set default targeting for all variables
        with targeting_context("user123"):
            value = my_variable.get()  # uses "user123"

        # Set targeting for specific variables
        with targeting_context("org456", variables=[org_variable]):
            org_value = org_variable.get()  # uses "org456"
            other_value = other_variable.get()  # uses default or trace_id

        # Combine default and specific - order doesn\'t matter for precedence
        with targeting_context("user123"):
            with targeting_context("org456", variables=[org_variable]):
                org_value = org_variable.get()  # uses "org456" (specific wins)
                other_value = other_variable.get()  # uses "user123" (default)
    '''
