import logfire
from _typeshed import Incomplete
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from logfire.variables.config import VariableConfig, VariableTypeConfig, VariablesConfig
from logfire.variables.variable import Variable
from typing import Any, Generic, TypeVar

__all__ = ['ResolvedVariable', 'SyncMode', 'ValidationReport', 'VariableProvider', 'NoOpVariableProvider', 'VariableWriteError', 'VariableNotFoundError', 'VariableAlreadyExistsError']

SyncMode: Incomplete
T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)

class VariableWriteError(Exception):
    """Base exception for variable write operation failures."""
class VariableNotFoundError(VariableWriteError):
    """Raised when a variable is not found."""
class VariableAlreadyExistsError(VariableWriteError):
    """Raised when trying to create a variable that already exists."""

@dataclass(kw_only=True)
class ResolvedVariable(Generic[T_co]):
    '''Details about a variable resolution including value, label, version, and any errors.

    This class can be used as a context manager. When used as a context manager, it
    automatically sets baggage with the variable name and label, enabling downstream
    spans and logs to be associated with the variable resolution that was active at the time.

    Example:
        ```python skip="true"
        my_var = logfire.var(name=\'my_var\', type=str, default=\'default\')
        with my_var.get() as details:
            # Inside this context, baggage is set with:
            # logfire.variables.my_var = <label> (or \'<code_default>\' if no label)
            value = details.value
            # Any spans/logs created here will have the baggage attached
        ```
    '''
    name: str
    value: T_co
    label: str | None = ...
    version: int | None = ...
    exception: Exception | None = ...
    def __post_init__(self) -> None: ...
    def __enter__(self): ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None: ...

@dataclass
class LabelCompatibility:
    """Result of checking a label value's compatibility with a schema."""
    label: str
    serialized_value: str
    is_compatible: bool
    error: str | None = ...

@dataclass
class VariableChange:
    """Represents a change to be made to a variable."""
    name: str
    change_type: str
    local_schema: dict[str, Any] | None = ...
    server_schema: dict[str, Any] | None = ...
    initial_value: str | None = ...
    incompatible_labels: list[LabelCompatibility] | None = ...
    server_id: str | None = ...
    local_description: str | None = ...
    server_description: str | None = ...
    description_differs: bool = ...

@dataclass
class VariableDiff:
    """Represents the diff between local and server variables."""
    changes: list[VariableChange]
    orphaned_server_variables: list[str]
    @property
    def has_changes(self) -> bool:
        """Return True if there are any changes to apply."""

@dataclass
class LabelValidationError:
    """Represents a validation error for a specific label value."""
    variable_name: str
    label: str | None
    error: Exception

@dataclass
class DescriptionDifference:
    """Represents a description difference between local and server."""
    variable_name: str
    local_description: str | None
    server_description: str | None

@dataclass
class ValidationReport:
    '''Report of variable validation results.

    This class contains the results of validating variable definitions against
    a provider\'s configuration. It can be used to check for errors programmatically
    or formatted for human-readable output.

    Example:
        ```python skip="true"
        report = provider.validate_variables(variables)
        if not report.is_valid:
            print(report.format())
            sys.exit(1)
        ```
    '''
    errors: list[LabelValidationError]
    variables_checked: int
    variables_not_on_server: list[str]
    description_differences: list[DescriptionDifference]
    @property
    def has_errors(self) -> bool:
        """Return True if there are any validation errors."""
    @property
    def is_valid(self) -> bool:
        """Return False if there are any validation errors or any variables not defined in the (possibly remote) config."""
    def format(self, *, colors: bool = True) -> str:
        """Format the validation report for human-readable output.

        Args:
            colors: If True, include ANSI color codes in output.

        Returns:
            A formatted string representation of the report.
        """

class VariableProvider(ABC):
    """Abstract base class for variable value providers."""
    @abstractmethod
    def get_serialized_value(self, variable_name: str, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None) -> ResolvedVariable[str | None]:
        """Retrieve the serialized value for a variable.

        Args:
            variable_name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection (e.g., user ID).
            attributes: Optional attributes for condition-based targeting rules.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
    def get_serialized_value_for_label(self, variable_name: str, label: str) -> ResolvedVariable[str | None]:
        """Retrieve the serialized value for a specific label of a variable.

        This method bypasses rollout weights and targeting, directly selecting the
        specified label. Used for explicit label selection.

        Args:
            variable_name: The name of the variable to resolve.
            label: The name of the label to select.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).

        Note:
            The default implementation uses get_variable_config to look up the label.
            Subclasses may override this for more efficient implementations.
        """
    def refresh(self, force: bool = False):
        """Refresh the value provider.

        Only relevant to remote providers where initial retrieval may be asynchronous.
        Calling this method is intended to block until an initial retrieval happens, but is not guaranteed
        to eagerly retrieve any updates if the provider implements some kind of caching; the `force` argument
        is provided as a way to ignore any caching.

        Args:
            force: Whether to force refresh. If using a provider with caching, setting this to `True` triggers a refresh
            ignoring the cache.
        """
    def shutdown(self, timeout_millis: float = 5000):
        """Clean up any resources used by the provider.

        Args:
            timeout_millis: The timeout budget in milliseconds for shutdown operations.
        """
    def start(self, logfire_instance: logfire.Logfire | None) -> None:
        """Start any background tasks for this provider.

        This is called after the provider is created and the Logfire instance is available.
        Providers that need to run background tasks (like polling) should override this method
        to start those tasks, using the provided logfire instance for error logging.

        Args:
            logfire_instance: The Logfire instance to use for error logging, or None if
                variable instrumentation is disabled.
        """
    def get_variable_config(self, name: str) -> VariableConfig | None:
        """Retrieve the full configuration for a variable.

        Args:
            name: The name of the variable.

        Returns:
            The VariableConfig if found, or None if the variable doesn't exist.

        Note:
            Subclasses should override this method to provide actual implementations.
            The default implementation returns None.
        """
    def get_all_variables_config(self) -> VariablesConfig:
        """Retrieve all variable configurations.

        This is used by push_variables() to compute diffs.

        Returns:
            A VariablesConfig containing all variable configurations.
            Returns an empty VariablesConfig if no configs are available.
        """
    def create_variable(self, config: VariableConfig) -> VariableConfig:
        """Create a new variable configuration.

        Args:
            config: The configuration for the new variable.

        Returns:
            The created VariableConfig.

        Raises:
            VariableAlreadyExistsError: If a variable with this name already exists.

        Note:
            Subclasses should override this method to provide actual implementations.
            The default implementation emits a warning and returns the config unchanged.
        """
    def update_variable(self, name: str, config: VariableConfig) -> VariableConfig:
        """Update an existing variable configuration.

        Args:
            name: The name of the variable to update.
            config: The new configuration for the variable.

        Returns:
            The updated VariableConfig.

        Raises:
            VariableNotFoundError: If the variable does not exist.

        Note:
            Subclasses should override this method to provide actual implementations.
            The default implementation emits a warning and returns the config unchanged.
        """
    def delete_variable(self, name: str) -> None:
        """Delete a variable configuration.

        Args:
            name: The name of the variable to delete.

        Raises:
            VariableNotFoundError: If the variable does not exist.

        Note:
            Subclasses should override this method to provide actual implementations.
            The default implementation emits a warning.
        """
    def batch_update(self, updates: dict[str, VariableConfig | None]) -> None:
        """Update multiple variables atomically.

        This default implementation processes updates sequentially. Subclasses
        (especially remote providers) may override this to batch operations
        into a single API call for better performance.

        Args:
            updates: A mapping of variable names to their new configurations.
                Unrecognized names will be created.
                A None value means the variable should be deleted.
                All others will be updated.
        """
    def push_config(self, config: VariablesConfig, *, mode: SyncMode = 'merge', dry_run: bool = False, yes: bool = False) -> bool:
        """Push a VariablesConfig to this provider.

        This method pushes a complete VariablesConfig (including labels and rollouts)
        to the provider. It's useful for:
        - Pushing configs generated or modified locally
        - Pushing configs read from files
        - Partial updates (merge mode) or full replacement (replace mode)

        Args:
            config: The VariablesConfig to push.
            mode: 'merge' updates/creates only variables in config (leaves others unchanged).
                  'replace' makes the server match the config exactly (deletes missing variables).
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.
        """
    def pull_config(self) -> VariablesConfig:
        """Pull the current variable configuration from the provider.

        This method fetches the complete configuration from the provider,
        useful for generating local copies of the config that can be modified.

        Returns:
            The current VariablesConfig from the provider.
        """
    def push_variables(self, variables: Sequence[Variable[object]], *, dry_run: bool = False, yes: bool = False, strict: bool = False) -> bool:
        """Push variable definitions to this provider.

        This method syncs local variable definitions (metadata only) with the provider:
        - Creates new variables that don't exist in the provider
        - Updates JSON schemas for existing variables if they've changed
        - Warns about existing label values that are incompatible with new schemas

        Args:
            variables: Variable instances to push.
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.
            strict: If True, fail if any existing label values are incompatible with new schemas.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.
        """
    def validate_variables(self, variables: Sequence[Variable[object]]) -> ValidationReport:
        '''Validate that provider-side variable label values match local type definitions.

        This method fetches the current variable configuration from the provider and
        validates that all label values can be deserialized to the expected types
        defined in the local Variable instances.

        Args:
            variables: Variable instances to validate.

        Returns:
            A ValidationReport containing any errors found. Use `report.is_valid` to check
            if validation passed, and `report.format()` to get a human-readable summary.

        Example:
            ```python skip="true"
            report = provider.validate_variables(variables)
            if not report.is_valid:
                print(report.format())
                sys.exit(1)
            ```
        '''
    def list_variable_types(self) -> dict[str, VariableTypeConfig]:
        """List all variable types from the provider.

        Returns:
            A dictionary mapping type names to their configurations.
        """
    def get_variable_type(self, name: str) -> VariableTypeConfig | None:
        """Get a variable type by name.

        Args:
            name: The name of the type to retrieve.

        Returns:
            The VariableTypeConfig if found, None otherwise.
        """
    def upsert_variable_type(self, config: VariableTypeConfig) -> VariableTypeConfig:
        """Create or update a variable type.

        If a type with the given name exists, it will be updated.
        Otherwise, a new type will be created.

        Args:
            config: The type configuration to upsert.

        Returns:
            The created or updated VariableTypeConfig.
        """
    def push_variable_types(self, types: Sequence[type[Any] | tuple[type[Any], str]], *, dry_run: bool = False, yes: bool = False, strict: bool = False) -> bool:
        '''Push variable type definitions to this provider.

        This method syncs local type definitions with the provider:
        - Creates new types that don\'t exist in the provider
        - Updates JSON schemas for existing types if they\'ve changed
        - Warns about schema changes
        - Checks if existing variable label values are compatible with the new schemas

        Args:
            types: Types to push. Items can be:
                - A type (name defaults to __name__ or str(type))
                - A tuple of (type, name) for explicit naming
            dry_run: If True, only show what would change without applying.
            yes: If True, skip confirmation prompt.
            strict: If True, abort when existing label values are incompatible with
                the new type schema.

        Returns:
            True if changes were applied (or would be applied in dry_run mode), False otherwise.

        Example:
            ```python skip="true"
            from pydantic import BaseModel


            class FeatureConfig(BaseModel):
                enabled: bool
                max_items: int = 10


            # Push using __name__ as type name
            provider.push_variable_types([FeatureConfig])

            # Push with explicit name
            provider.push_variable_types([(FeatureConfig, \'my_feature_config\')])
            ```
        '''

@dataclass
class NoOpVariableProvider(VariableProvider):
    """A variable provider that always returns None, used when no provider is configured."""
    def get_serialized_value(self, variable_name: str, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None) -> ResolvedVariable[str | None]:
        """Return None for all variable lookups.

        Args:
            variable_name: The name of the variable to resolve (ignored).
            targeting_key: Optional key for deterministic label selection (ignored).
            attributes: Optional attributes for condition-based targeting rules (ignored).

        Returns:
            A ResolvedVariable with value=None.
        """
    def get_variable_config(self, name: str) -> VariableConfig | None:
        """Return None for all variable lookups.

        Args:
            name: The name of the variable (ignored).

        Returns:
            Always None since no provider is configured.
        """
    def push_variables(self, variables: Sequence[Variable[Any]], *, dry_run: bool = False, yes: bool = False, strict: bool = False) -> bool:
        """No-op implementation that prints a message about missing provider configuration.

        Returns:
            Always False since no provider is configured.
        """
    def validate_variables(self, variables: Sequence[Variable[Any]]) -> ValidationReport:
        """No-op implementation that returns an empty validation report.

        Returns:
            An empty ValidationReport since there's no provider to validate against.
        """
