import re
from collections.abc import Mapping, Sequence
from logfire._internal.config import LocalVariablesOptions as LocalVariablesOptions
from logfire.variables.abstract import ResolvedVariable
from logfire.variables.variable import Variable
from pydantic import BaseModel
from typing import Any, Literal

__all__ = ['KeyIsNotPresent', 'KeyIsPresent', 'LabeledValue', 'LabelRef', 'LatestVersion', 'LocalVariablesOptions', 'Rollout', 'RolloutOverride', 'ValueDoesNotEqual', 'ValueDoesNotMatchRegex', 'ValueEquals', 'ValueIsIn', 'ValueIsNotIn', 'ValueMatchesRegex', 'VariableConfig', 'VariablesConfig', 'VariableTypeConfig']

class ValueEquals(BaseModel):
    """Condition that matches when an attribute equals a specific value."""
    attribute: str
    value: Any
    kind: Literal['value-equals']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute equals the expected value."""

class ValueDoesNotEqual(BaseModel):
    """Condition that matches when an attribute does not equal a specific value."""
    attribute: str
    value: Any
    kind: Literal['value-does-not-equal']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute does not equal the specified value."""

class ValueIsIn(BaseModel):
    """Condition that matches when an attribute value is in a set of values."""
    attribute: str
    values: Sequence[Any]
    kind: Literal['value-is-in']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value is in the allowed set."""

class ValueIsNotIn(BaseModel):
    """Condition that matches when an attribute value is not in a set of values."""
    attribute: str
    values: Sequence[Any]
    kind: Literal['value-is-not-in']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value is not in the excluded set."""

class ValueMatchesRegex(BaseModel):
    """Condition that matches when an attribute value matches a regex pattern."""
    attribute: str
    pattern: str | re.Pattern[str]
    kind: Literal['value-matches-regex']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value matches the regex pattern."""

class ValueDoesNotMatchRegex(BaseModel):
    """Condition that matches when an attribute value does not match a regex pattern."""
    attribute: str
    pattern: str | re.Pattern[str]
    kind: Literal['value-does-not-match-regex']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value does not match the regex pattern."""

class KeyIsPresent(BaseModel):
    """Condition that matches when an attribute key is present."""
    attribute: str
    kind: Literal['key-is-present']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute key exists in the attributes."""

class KeyIsNotPresent(BaseModel):
    """Condition that matches when an attribute key is not present."""
    attribute: str
    kind: Literal['key-is-not-present']
    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute key does not exist in the attributes."""

class LabeledValue(BaseModel):
    """A label pointing to a version with an inline serialized value."""
    version: int
    serialized_value: str

class LabelRef(BaseModel):
    """A label pointing to a version via a reference to another label, 'latest', or 'code_default'."""
    version: int | None
    ref: str

class LatestVersion(BaseModel):
    """The latest (highest) version of the variable."""
    version: int
    serialized_value: str

class Rollout(BaseModel):
    """Configuration for label selection with weighted probabilities."""
    labels: dict[str, float]
    def select_label(self, seed: str | None) -> str | None:
        """Select a label based on configured weights using optional seeded randomness.

        Args:
            seed: Optional seed for deterministic label selection. If provided, the same seed
                will always select the same label.

        Returns:
            The name of the selected label, or None if no label is selected
            (when labels is empty or weights sum to less than 1.0).
        """

class RolloutOverride(BaseModel):
    """An override of the default rollout when specific conditions are met."""
    conditions: list[Condition]
    rollout: Rollout

class VariableConfig(BaseModel):
    """Configuration for a single managed variable including labels, versions, and rollout rules."""
    name: VariableName
    labels: dict[str, LabeledValue | LabelRef]
    rollout: Rollout
    overrides: list[RolloutOverride]
    latest_version: LatestVersion | None
    description: str | None
    json_schema: dict[str, Any] | None
    type_name: str | None
    aliases: list[VariableName] | None
    example: str | None
    def resolve_label(self, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None) -> str | None:
        """Evaluate rollout rules and return the selected label name.

        The resolution process:
        1. Evaluate overrides in order; the first match takes precedence
        2. Select a label based on the rollout weights (deterministic if targeting_key is provided)

        Args:
            targeting_key: A string identifying the subject of evaluation (e.g., user ID).
                When provided, ensures deterministic label selection for the same key.
            attributes: Additional attributes for condition matching in override rules.

        Returns:
            The name of the selected label, or None if no label is selected (empty rollout
            means 'use code default').
        """
    def resolve_value(self, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None, *, label: str | None = None) -> tuple[str | None, str | None, int | None]:
        """Resolve the serialized value for this variable.

        Resolution order:
        1. If explicit label requested, look up in labels, follow refs
        2. If rollout selects a label, look up, follow refs
        3. If no label selected (empty rollout or remainder probability), use code default

        Following refs: if a LabeledValue has a `ref`, look up that label's value.
        If ref == 'latest', use latest_version.
        If ref == 'code_default', return (None, None, None) to trigger code default fallthrough.

        Args:
            targeting_key: A string identifying the subject of evaluation (e.g., user ID).
            attributes: Additional attributes for condition matching in override rules.
            label: Optional explicit label to select, bypassing rollout.

        Returns:
            A tuple of (serialized_value, label, version) where:
            - serialized_value is the JSON-serialized value or None
            - label is the name of the label that was selected or None
            - version is the version number or None
        """
    def follow_ref(self, labeled_value: LabeledValue | LabelRef, _visited: set[str] | None = None) -> tuple[str | None, int | None]:
        """Follow ref chains to get the actual serialized value.

        Args:
            labeled_value: The LabeledValue or LabelRef to resolve.
            _visited: Set of already-visited ref names to detect cycles.

        Returns:
            A tuple of (serialized_value, version).
        """

class VariablesConfig(BaseModel):
    """Container for all managed variable configurations."""
    variables: dict[VariableName, VariableConfig]
    def resolve_serialized_value(self, name: VariableName, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None, *, label: str | None = None) -> ResolvedVariable[str | None]:
        """Evaluate a managed variable configuration and resolve the selected value.

        Args:
            name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection.
            attributes: Optional attributes for condition matching.
            label: Optional explicit label to select, bypassing rollout.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
    def get_validation_errors(self, variables: list[Variable[Any]]) -> dict[str, dict[str | None, Exception]]:
        """Validate that all variable label values can be deserialized to their expected types.

        Args:
            variables: List of Variable instances to validate against this configuration.

        Returns:
            A dict mapping variable names to dicts of label names (or None for general errors) to exceptions.
        """
    @staticmethod
    def from_variables(variables: list[Variable[Any]]) -> VariablesConfig:
        """Create a VariablesConfig from a list of Variable instances.

        This creates a minimal config with just the name, schema, and example for each variable.
        No labels or versions are created - use this to generate a template config that can be edited.

        Args:
            variables: List of Variable instances to create configs from.

        Returns:
            A VariablesConfig with minimal configs for each variable.
        """
    def merge(self, other: VariablesConfig) -> VariablesConfig:
        """Merge another VariablesConfig into this one.

        Variables in `other` will override variables with the same name in this config.

        Args:
            other: Another VariablesConfig to merge.

        Returns:
            A new VariablesConfig with variables from both configs.
        """

class VariableTypeConfig(BaseModel):
    """Configuration for a variable type (reusable schema definition)."""
    name: str
    json_schema: dict[str, Any]
    description: str | None
    source_hint: str | None
