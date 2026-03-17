from __future__ import annotations as _annotations

import random
import re
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, ValidationError, WithJsonSchema, field_validator, model_validator
from typing_extensions import TypeAliasType

from logfire._internal.config import (
    LocalVariablesOptions as LocalVariablesOptions,
    VariablesOptions as VariablesOptions,
)
from logfire.variables.abstract import ResolvedVariable
from logfire.variables.variable import Variable

try:
    from pydantic import Discriminator
except ImportError:  # pragma: no cover
    # This is only used in an annotation, so if you have Pydantic < 2.5, just treat it as a no-op
    def Discriminator(*args: Any, **kwargs: Any) -> Any:
        pass


__all__ = (
    'KeyIsNotPresent',
    'KeyIsPresent',
    'LabeledValue',
    'LabelRef',
    'LatestVersion',
    'LocalVariablesOptions',
    'Rollout',
    'RolloutOverride',
    'ValueDoesNotEqual',
    'ValueDoesNotMatchRegex',
    'ValueEquals',
    'ValueIsIn',
    'ValueIsNotIn',
    'ValueMatchesRegex',
    'VariableConfig',
    'VariablesConfig',
    'VariableTypeConfig',
)


class ValueEquals(BaseModel):
    """Condition that matches when an attribute equals a specific value."""

    attribute: str
    """The name of the attribute to check."""
    value: Any
    """The value the attribute must equal."""
    kind: Literal['value-equals'] = 'value-equals'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute equals the expected value."""
        return attributes.get(self.attribute, object()) == self.value


class ValueDoesNotEqual(BaseModel):
    """Condition that matches when an attribute does not equal a specific value."""

    attribute: str
    """The name of the attribute to check."""
    value: Any
    """The value the attribute must not equal."""
    kind: Literal['value-does-not-equal'] = 'value-does-not-equal'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute does not equal the specified value."""
        # If attribute is missing, use object() sentinel which won't equal any value
        return attributes.get(self.attribute, object()) != self.value


class ValueIsIn(BaseModel):
    """Condition that matches when an attribute value is in a set of values."""

    attribute: str
    """The name of the attribute to check."""
    values: Sequence[Any]
    """The set of values the attribute must be in."""
    kind: Literal['value-is-in'] = 'value-is-in'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value is in the allowed set."""
        value = attributes.get(self.attribute, object())
        return value in self.values


class ValueIsNotIn(BaseModel):
    """Condition that matches when an attribute value is not in a set of values."""

    attribute: str
    """The name of the attribute to check."""
    values: Sequence[Any]
    """The set of values the attribute must not be in."""
    kind: Literal['value-is-not-in'] = 'value-is-not-in'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value is not in the excluded set."""
        # If attribute is missing, use object() sentinel which won't be in the list
        value = attributes.get(self.attribute, object())
        return value not in self.values


class ValueMatchesRegex(BaseModel):
    """Condition that matches when an attribute value matches a regex pattern."""

    attribute: str
    """The name of the attribute to check."""
    pattern: str | re.Pattern[str]
    """The regex pattern the attribute value must match."""
    kind: Literal['value-matches-regex'] = 'value-matches-regex'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value matches the regex pattern."""
        value = attributes.get(self.attribute)
        if not isinstance(value, str):
            return False
        return bool(re.search(self.pattern, value))


class ValueDoesNotMatchRegex(BaseModel):
    """Condition that matches when an attribute value does not match a regex pattern."""

    attribute: str
    """The name of the attribute to check."""
    pattern: str | re.Pattern[str]
    """The regex pattern the attribute value must not match."""
    kind: Literal['value-does-not-match-regex'] = 'value-does-not-match-regex'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute value does not match the regex pattern."""
        value = attributes.get(self.attribute)
        if not isinstance(value, str):
            # Missing or non-string values cannot match the pattern,
            # so they satisfy the "does not match" condition.
            return True
        return not re.search(self.pattern, value)


class KeyIsPresent(BaseModel):
    """Condition that matches when an attribute key is present."""

    attribute: str
    """The name of the attribute key that must be present."""
    kind: Literal['key-is-present'] = 'key-is-present'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute key exists in the attributes."""
        return self.attribute in attributes


class KeyIsNotPresent(BaseModel):
    """Condition that matches when an attribute key is not present."""

    attribute: str
    """The name of the attribute key that must not be present."""
    kind: Literal['key-is-not-present'] = 'key-is-not-present'
    """Discriminator field for condition type."""

    def matches(self, attributes: Mapping[str, Any]) -> bool:
        """Check if the attribute key does not exist in the attributes."""
        return self.attribute not in attributes


Condition = TypeAliasType(
    'Condition',
    Annotated[
        Union[
            ValueEquals,
            ValueDoesNotEqual,
            ValueIsIn,
            ValueIsNotIn,
            ValueMatchesRegex,
            ValueDoesNotMatchRegex,
            KeyIsPresent,
            KeyIsNotPresent,
        ],
        Discriminator('kind'),
    ],
)


VariableName = Annotated[str, Field(pattern=r'^[a-zA-Z_][a-zA-Z0-9_]*$'), WithJsonSchema({'type': 'string'})]
"""The name of a variable.

At least for now, must be a valid Python identifier."""


class LabeledValue(BaseModel):
    """A label pointing to a version with an inline serialized value."""

    version: int
    """The version number this label points to."""
    serialized_value: str
    """The JSON-serialized value for this version."""


class LabelRef(BaseModel):
    """A label pointing to a version via a reference to another label, 'latest', or 'code_default'."""

    version: int | None = None
    """The version number this label points to. None for label-to-label refs or code_default."""
    ref: str
    """Reference to another label, 'latest', or 'code_default'."""


class LatestVersion(BaseModel):
    """The latest (highest) version of the variable."""

    version: int
    """The version number of the latest version."""
    serialized_value: str
    """The JSON-serialized value of the latest version."""


class Rollout(BaseModel):
    """Configuration for label selection with weighted probabilities."""

    labels: dict[str, float] = {}
    """Mapping of label names to their selection weights (must sum to at most 1.0).
    An empty dict means 'use code default'."""

    @cached_property
    def _population_and_weights(self) -> tuple[list[str | None], list[float]]:
        # Note that the caching means that the `labels` field should be treated as immutable
        population: list[str | None] = []
        weights: list[float] = []
        for k, v in self.labels.items():
            population.append(k)
            weights.append(v)

        p_none = 1 - sum(weights)
        if p_none > 0:
            population.append(None)
            weights.append(p_none)
        return population, weights

    @field_validator('labels')
    @classmethod
    def _validate_label_proportions(cls, v: dict[str, float]):
        # Note: if the values sum to _less_ than 1, the remaining proportion corresponds to the probability of using
        # the code default.
        if any(weight < 0 for weight in v.values()):
            raise ValueError('Label proportions must not be negative.')
        if sum(v.values()) > 1.0 + 1e-9:
            raise ValueError('Label proportions must not sum to more than 1.')
        return v

    def select_label(self, seed: str | None) -> str | None:
        """Select a label based on configured weights using optional seeded randomness.

        Args:
            seed: Optional seed for deterministic label selection. If provided, the same seed
                will always select the same label.

        Returns:
            The name of the selected label, or None if no label is selected
            (when labels is empty or weights sum to less than 1.0).
        """
        if not self.labels:
            return None
        rand = random.Random(seed)
        population, weights = self._population_and_weights
        return rand.choices(population, weights)[0]


class RolloutOverride(BaseModel):
    """An override of the default rollout when specific conditions are met."""

    conditions: list[Condition]
    """List of conditions that must all match for this override to apply."""
    rollout: Rollout
    """The rollout configuration to use when all conditions match."""


class VariableConfig(BaseModel):
    """Configuration for a single managed variable including labels, versions, and rollout rules."""

    # A note on migrations:
    # * To migrate value types, copy the variable using a new name, update the values, and use the new variable name in updated code
    # * To migrate variable names, update the "aliases" field on the VariableConfig
    name: VariableName
    """Unique name identifying this variable."""
    labels: dict[str, LabeledValue | LabelRef] = {}
    """Mapping of label names to their configurations."""
    rollout: Rollout
    """Default rollout configuration for label selection."""
    overrides: list[RolloutOverride]
    """Conditional overrides evaluated in order; first match takes precedence."""
    latest_version: LatestVersion | None = None
    """The latest (highest) version of the variable, if any versions exist."""
    description: str | None = (
        None  # Note: When we drop support for python 3.9, move this field immediately after `name`
    )
    """Description of the variable."""
    json_schema: dict[str, Any] | None = None
    """JSON schema describing the expected type of this variable's values."""
    type_name: str | None = None
    """Name of the variable type this variable references, if any."""
    aliases: list[VariableName] | None = None
    """Alternative names that resolve to this variable; useful for name migrations."""
    example: str | None = None
    """JSON-serialized example value from code; used as a template when creating new values in the UI."""
    # NOTE: Context-based targeting_key can be set via targeting_context() from logfire.variables.
    # TODO(DavidM): Consider adding remotely-managed targeting_key_attribute for automatic attribute-based targeting.

    @model_validator(mode='after')
    def _validate_labels(self):
        # Validate rollout label references
        for k in self.rollout.labels:
            if k not in self.labels:
                raise ValueError(f'Label {k!r} present in `rollout.labels` is not present in `labels`.')

        # Validate rollout override label references
        for i, override in enumerate(self.overrides):  # pragma: no branch
            for k in override.rollout.labels:  # pragma: no branch
                if k not in self.labels:  # pragma: no branch
                    raise ValueError(f'Label {k!r} present in `overrides[{i}].rollout` is not present in `labels`.')

        # Validate ref chains don't reference non-existent labels
        for k, labeled_value in self.labels.items():
            if isinstance(labeled_value, LabelRef) and labeled_value.ref not in ('latest', 'code_default'):
                if labeled_value.ref not in self.labels:
                    raise ValueError(f'Label {k!r} has ref {labeled_value.ref!r} which is not present in `labels`.')

        return self

    def resolve_label(
        self, targeting_key: str | None = None, attributes: Mapping[str, Any] | None = None
    ) -> str | None:
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
        if attributes is None:
            attributes = {}

        # Step 1: Determine the rollout and overrides to use
        base_rollout = self.rollout
        base_overrides = self.overrides

        # Step 2: Find the first matching override, or use the base rollout
        selected_rollout = base_rollout
        for override in base_overrides:
            if _matches_all_conditions(override.conditions, attributes):
                selected_rollout = override.rollout
                break  # First match takes precedence

        seed = None if targeting_key is None else f'{self.name!r}:{targeting_key!r}'
        return selected_rollout.select_label(seed)

    def resolve_value(
        self,
        targeting_key: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        *,
        label: str | None = None,
    ) -> tuple[str | None, str | None, int | None]:
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
        # If explicit label requested, look it up directly
        if label is not None:
            labeled_value = self.labels.get(label)
            if labeled_value is not None:
                serialized, version = self.follow_ref(labeled_value)
                return serialized, label, version
            # Label not found, fall through to rollout-based resolution

        # Use rollout to select a label
        selected_label = self.resolve_label(targeting_key, attributes)

        if selected_label is not None:
            labeled_value = self.labels.get(selected_label)
            if labeled_value is not None:  # pragma: no branch
                serialized, version = self.follow_ref(labeled_value)
                return serialized, selected_label, version

        # No label selected (empty rollout or remainder probability) -> use code default
        return None, None, None

    def follow_ref(
        self, labeled_value: LabeledValue | LabelRef, _visited: set[str] | None = None
    ) -> tuple[str | None, int | None]:
        """Follow ref chains to get the actual serialized value.

        Args:
            labeled_value: The LabeledValue or LabelRef to resolve.
            _visited: Set of already-visited ref names to detect cycles.

        Returns:
            A tuple of (serialized_value, version).
        """
        if isinstance(labeled_value, LabelRef):
            if labeled_value.ref == 'code_default':
                return None, None

            if labeled_value.ref == 'latest':
                # Reference to latest version
                if self.latest_version is not None:
                    return self.latest_version.serialized_value, self.latest_version.version
                return None, labeled_value.version

            # Reference to another label - follow the chain
            if _visited is None:
                _visited = set()
            if labeled_value.ref in _visited:
                # Cycle detected, return None
                return None, labeled_value.version
            _visited.add(labeled_value.ref)

            target = self.labels.get(labeled_value.ref)
            if target is None:
                return None, labeled_value.version
            return self.follow_ref(target, _visited)

        return labeled_value.serialized_value, labeled_value.version


class VariablesConfig(BaseModel):
    """Container for all managed variable configurations."""

    variables: dict[VariableName, VariableConfig]
    """Mapping of variable names to their configurations."""

    @model_validator(mode='after')
    def _validate_variables(self):
        # Validate lookup keys on variables dict
        for k, v in self.variables.items():
            if v.name != k:
                raise ValueError(f'`variables` has invalid lookup key {k!r} for value with name {v.name!r}.')
        return self

    @cached_property
    def _alias_map(self) -> dict[VariableName, str]:
        # Build alias lookup map for efficient lookups
        alias_map: dict[VariableName, VariableName] = {}
        for var_config in self.variables.values():
            if var_config.aliases:
                for alias in var_config.aliases:
                    alias_map[alias] = var_config.name
        return alias_map

    def _invalidate_alias_map(self) -> None:
        """Invalidate the cached alias map so it gets rebuilt on next access."""
        self.__dict__.pop('_alias_map', None)  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    def resolve_serialized_value(
        self,
        name: VariableName,
        targeting_key: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        *,
        label: str | None = None,
    ) -> ResolvedVariable[str | None]:
        """Evaluate a managed variable configuration and resolve the selected value.

        Args:
            name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection.
            attributes: Optional attributes for condition matching.
            label: Optional explicit label to select, bypassing rollout.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
        variable_config = self._get_variable_config(name)
        if variable_config is None:
            return ResolvedVariable(name=name, value=None, _reason='unrecognized_variable')

        serialized_value, selected_label, version = variable_config.resolve_value(
            targeting_key, attributes, label=label
        )
        return ResolvedVariable(
            name=variable_config.name,
            value=serialized_value,
            label=selected_label,
            version=version,
            _reason='resolved',
        )

    def _get_variable_config(self, name: VariableName) -> VariableConfig | None:
        # First try direct lookup
        config = self.variables.get(name)
        if config is not None:
            return config

        # Fall back to alias lookup (aliases are stored on each VariableConfig)
        if name in self._alias_map:
            return self.variables.get(self._alias_map[name])

        return None

    def get_validation_errors(self, variables: list[Variable[Any]]) -> dict[str, dict[str | None, Exception]]:
        """Validate that all variable label values can be deserialized to their expected types.

        Args:
            variables: List of Variable instances to validate against this configuration.

        Returns:
            A dict mapping variable names to dicts of label names (or None for general errors) to exceptions.
        """
        errors: dict[str, dict[str | None, Exception]] = {}
        for variable in variables:
            try:
                config = self._get_variable_config(variable.name)
                if config is None:
                    raise ValueError(f'No config for variable with name {variable.name!r}')
                # Validate inline label values
                for k, labeled_value in config.labels.items():
                    if isinstance(labeled_value, LabeledValue):
                        try:
                            variable.type_adapter.validate_json(labeled_value.serialized_value)
                        except ValidationError as e:
                            errors.setdefault(variable.name, {})[k] = e
                # Validate latest version value
                if config.latest_version is not None:
                    try:
                        variable.type_adapter.validate_json(config.latest_version.serialized_value)
                    except ValidationError as e:
                        errors.setdefault(variable.name, {})['latest'] = e
            except Exception as e:
                errors.setdefault(variable.name, {})[None] = e
        return errors

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
        from logfire.variables.variable import is_resolve_function

        variable_configs: dict[VariableName, VariableConfig] = {}
        for variable in variables:
            # Get JSON schema from the type adapter
            json_schema = variable.type_adapter.json_schema()

            # Get the serialized default value as an example (if not a function)
            example: str | None = None
            if not is_resolve_function(variable.default):
                example = variable.type_adapter.dump_json(variable.default).decode('utf-8')

            config = VariableConfig(
                name=variable.name,
                description=variable.description,
                labels={},
                rollout=Rollout(labels={}),
                overrides=[],
                json_schema=json_schema,
                example=example,
            )
            variable_configs[variable.name] = config

        return VariablesConfig(variables=variable_configs)

    def merge(self, other: VariablesConfig) -> VariablesConfig:
        """Merge another VariablesConfig into this one.

        Variables in `other` will override variables with the same name in this config.

        Args:
            other: Another VariablesConfig to merge.

        Returns:
            A new VariablesConfig with variables from both configs.
        """
        merged_variables = dict(self.variables)
        merged_variables.update(other.variables)
        return VariablesConfig(variables=merged_variables)


def _matches_all_conditions(conditions: list[Condition], attributes: Mapping[str, Any]) -> bool:
    """Check if all conditions match the provided attributes.

    Args:
        conditions: List of conditions to evaluate.
        attributes: Attributes to match against.

    Returns:
        True if all conditions match, False otherwise.
    """
    for condition in conditions:
        if not condition.matches(attributes):
            return False
    return True


class VariableTypeConfig(BaseModel):
    """Configuration for a variable type (reusable schema definition)."""

    name: str
    """Unique name identifying this type. Defaults to __name__ or str(type)."""
    json_schema: dict[str, Any]
    """JSON schema describing the type structure."""
    description: str | None = None
    """Human-readable description of this type."""
    source_hint: str | None = None
    """Optional hint about where this type is defined in code (e.g., 'myapp.config.FeatureConfig')."""


def get_default_type_name(t: Any) -> str:
    """Get the default name for a Python type.

    For regular classes, returns __name__ (e.g., 'FeatureConfig').
    For unions, generics, or other complex types, returns str(type) (e.g., 'FeatureConfig | None').

    Args:
        t: The Python type to get a name for.

    Returns:
        A string name for the type.
    """
    if isinstance(t, type):
        return t.__name__
    return str(t)


def get_source_hint(t: Any) -> str | None:
    """Get a source hint for a type based on its module and qualname.

    Args:
        t: The Python type to get a source hint for.

    Returns:
        A source hint string like 'myapp.config.FeatureConfig', or None if not available.
    """
    if isinstance(t, type):
        module = getattr(t, '__module__', None)
        qualname = getattr(t, '__qualname__', None)
        if module and qualname:
            return f'{module}.{qualname}'
    return None
