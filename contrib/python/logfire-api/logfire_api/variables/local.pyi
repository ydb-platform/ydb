from collections.abc import Mapping
from logfire.variables.abstract import ResolvedVariable, VariableProvider
from logfire.variables.config import VariableConfig, VariablesConfig
from typing import Any

__all__ = ['LocalVariableProvider']

class LocalVariableProvider(VariableProvider):
    """Variable provider that resolves values from a local in-memory configuration.

    This provider stores a mutable `VariablesConfig` and supports both read and write operations.
    """
    def __init__(self, config: VariablesConfig) -> None:
        """Create a new local variable provider.

        Args:
            config: A VariablesConfig instance to use for variable resolution and mutation.
        """
    def get_serialized_value(self, variable_name: str, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None) -> ResolvedVariable[str | None]:
        """Resolve a variable's serialized value from the local configuration.

        Args:
            variable_name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection (e.g., user ID).
                If not provided and there is an active trace, its trace ID is used to ensure
                the same value is used across a trace.
            attributes: Optional attributes for condition-based targeting rules.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
    def get_variable_config(self, name: str) -> VariableConfig | None:
        """Retrieve the full configuration for a variable.

        This method supports alias-based lookup, so you can pass either the
        variable's current name or any of its configured aliases.

        Args:
            name: The name (or alias) of the variable.

        Returns:
            The VariableConfig if found, or None if the variable doesn't exist.
        """
    def get_all_variables_config(self) -> VariablesConfig:
        """Retrieve all variable configurations.

        Returns:
            A VariablesConfig containing all variable configurations.
        """
    def create_variable(self, config: VariableConfig) -> VariableConfig:
        """Create a new variable configuration.

        Args:
            config: The configuration for the new variable.

        Returns:
            The created VariableConfig.

        Raises:
            VariableAlreadyExistsError: If a variable with this name already exists.
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
        """
    def delete_variable(self, name: str) -> None:
        """Delete a variable configuration.

        Args:
            name: The name of the variable to delete.

        Raises:
            VariableNotFoundError: If the variable does not exist.
        """
