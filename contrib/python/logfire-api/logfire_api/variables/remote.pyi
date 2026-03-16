import logfire
from collections.abc import Mapping
from logfire._internal.config import VariablesOptions
from logfire.variables.abstract import ResolvedVariable, VariableProvider
from logfire.variables.config import VariableConfig, VariableTypeConfig, VariablesConfig
from typing import Any

__all__ = ['LogfireRemoteVariableProvider']

class LogfireRemoteVariableProvider(VariableProvider):
    """Variable provider that fetches configuration from a remote Logfire API.

    The threading implementation draws heavily from opentelemetry.sdk._shared_internal.BatchProcessor.
    """
    def __init__(self, base_url: str, token: str, options: VariablesOptions) -> None:
        """Create a new remote variable provider.

        Args:
            base_url: The base URL of the Logfire API.
            token: Authentication token for the Logfire API.
            options: Options for retrieving remote variables.
        """
    def start(self, logfire_instance: logfire.Logfire | None) -> None:
        """Start background polling with the given logfire instance for error logging.

        Args:
            logfire_instance: The Logfire instance to use for error logging, or None if
                variable instrumentation is disabled (errors will be reported via warnings).
        """
    def refresh(self, force: bool = False):
        """Fetch the latest variable configuration from the remote API.

        Args:
            force: If True, fetch configuration even if the polling interval hasn't elapsed.
        """
    def get_serialized_value(self, variable_name: str, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None) -> ResolvedVariable[str | None]:
        """Resolve a variable's serialized value from the remote configuration.

        Args:
            variable_name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection (e.g., user ID).
            attributes: Optional attributes for condition-based targeting rules.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
    def get_serialized_value_for_label(self, variable_name: str, label: str) -> ResolvedVariable[str | None]:
        """Resolve a variable's serialized value for a specific label from the remote configuration.

        Args:
            variable_name: The name of the variable to resolve.
            label: The name of the label to select.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
    def shutdown(self, timeout_millis: float = 5000):
        """Stop the background polling thread and clean up resources.

        Args:
            timeout_millis: The timeout budget in milliseconds for shutdown operations.
        """
    def get_variable_config(self, name: str) -> VariableConfig | None:
        """Retrieve the full configuration for a variable from the cached config.

        This method supports alias-based lookup, so you can pass either the
        variable's current name or any of its configured aliases.

        Args:
            name: The name (or alias) of the variable.

        Returns:
            The VariableConfig if found, or None if the variable doesn't exist.
        """
    def get_all_variables_config(self) -> VariablesConfig:
        """Retrieve all variable configurations from the cached config.

        Returns:
            A VariablesConfig containing all variable configurations.
            Returns an empty VariablesConfig if no config has been fetched yet.
        """
    def create_variable(self, config: VariableConfig) -> VariableConfig:
        """Create a new variable configuration via the remote API.

        Args:
            config: The configuration for the new variable.

        Returns:
            The created VariableConfig.

        Raises:
            VariableAlreadyExistsError: If a variable with this name already exists.
            VariableWriteError: If the API request fails.
        """
    def update_variable(self, name: str, config: VariableConfig) -> VariableConfig:
        """Update an existing variable configuration via the remote API.

        This is a metadata-only update (name, description, schema, example).
        Labels and versions are managed through the UI.

        Args:
            name: The name of the variable to update.
            config: The new configuration for the variable.

        Returns:
            The updated VariableConfig.

        Raises:
            VariableNotFoundError: If the variable does not exist.
            VariableWriteError: If the API request fails.
        """
    def delete_variable(self, name: str) -> None:
        """Delete a variable configuration via the remote API.

        Args:
            name: The name of the variable to delete.

        Raises:
            VariableNotFoundError: If the variable does not exist.
            VariableWriteError: If the API request fails.
        """
    def list_variable_types(self) -> dict[str, VariableTypeConfig]:
        """List all variable types from the remote API.

        Returns:
            A dictionary mapping type names to their configurations.
        """
    def upsert_variable_type(self, config: VariableTypeConfig) -> VariableTypeConfig:
        """Create or update a variable type via the remote API.

        If a type with the given name exists, it will be updated.
        Otherwise, a new type will be created.

        Args:
            config: The type configuration to upsert.

        Returns:
            The created or updated VariableTypeConfig.

        Raises:
            VariableWriteError: If the API request fails.
        """
