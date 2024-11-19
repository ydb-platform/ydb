from __future__ import annotations
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import (
    ScenarioTestHelper,
    TestContext,
)
from abc import abstractmethod

from typing import override, Dict
from dataclasses import dataclass
import json


@dataclass
class ObjectStorageParams:
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    scheme: str = 'HTTP'
    verify_ssl: bool = False

    def to_proto_str(self) -> str:
        return (
            f'Scheme: {self.scheme}\n'
            f'VerifySSL: {str(self.verify_ssl).lower()}\n'
            f'Endpoint: "{self.endpoint}"\n'
            f'Bucket: "{self.bucket}"\n'
            f'AccessKey: "{self.access_key}"\n'
            f'SecretKey: "{self.secret_key}"\n'
        )


@dataclass
class TieringRule:
    tier_name: str
    duration_for_evict: str

    def to_dict(self):
        return {
            'tierName': self.tier_name,
            'durationForEvict': self.duration_for_evict,
        }


@dataclass
class TieringPolicy:
    rules: list[TieringRule]

    def __init__(self):
        self.rules = []

    def with_rule(self, rule: TieringRule):
        self.rules.append(rule)
        return self

    def to_json(self) -> str:
        return json.dumps({'rules': list(map(lambda x: x.to_dict(), self.rules))})


@dataclass
class TierConfig:
    name: str
    s3_params: ObjectStorageParams

    def to_proto_str(self) -> str:
        return (
            f'Name: "{self.name}"\n'
            f'ObjectStorage: {{\n{self.s3_params.to_proto_str()}\n}}'
        )


class AlterTieringRule(ScenarioTestHelper.IYqlble):
    """Alter a tiering rule.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str, default_column: str, config: TieringPolicy) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the altered object.
            default_column: Default column used for tiering.
            config: Tiering rules to apply."""

        super().__init__(name)
        self._default_column: str = default_column
        self._config: TieringPolicy = config

    @override
    def params(self) -> Dict[str, str]:
        return {'tiering_rule': self._name, 'config': self._config.to_json()}

    @override
    def title(self):
        return 'Alter tiering rule'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'ALTER OBJECT `{self._name}` (TYPE TIERING_RULE)' \
               f' SET (defaultColumn = {self._default_column}, description = `{self._config.to_json()}`)'


class CreateTieringRule(AlterTieringRule):
    """Create a tiering rule.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def title(self):
        return 'Create tiering rule'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'CREATE OBJECT `{self._name}` (TYPE TIERING_RULE)' \
               f' WITH (defaultColumn = {self._default_column}, description = `{self._config.to_json()}`)'


class CreateTieringRuleIfNotExists(AlterTieringRule):
    """Create a tiering rule. If it exists, do nothing.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def title(self):
        return 'Create tiering rule'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'CREATE OBJECT IF NOT EXISTS `{self._name}` (TYPE TIERING_RULE)' \
               f' WITH (defaultColumn = {self._default_column}, description = `{self._config.to_json()}`)'


class AlterTier(ScenarioTestHelper.IYqlble):
    """Alter a tier.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str, config: TierConfig) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the altered object.
            config: Tier configuration."""

        super().__init__(name)
        self._config: TierConfig = config

    @override
    def params(self) -> Dict[str, str]:
        return {'tier': self._name, 'config': self._config.to_proto_str()}

    @override
    def title(self):
        return 'Alter tier'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'ALTER OBJECT `{self._name}` (TYPE TIER) SET (tierConfig = `{self._config.to_proto_str()}`)'


class CreateTier(AlterTier):
    """Create a tier.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def title(self):
        return 'Create tier'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'CREATE OBJECT `{self._name}` (TYPE TIER) WITH (tierConfig = `{self._config.to_proto_str()}`)'


class CreateTierIfNotExists(AlterTier):
    """Create a tier. If it exists, do nothing.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def title(self):
        return 'Create tier'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'CREATE OBJECT IF NOT EXISTS `{self._name}` (TYPE TIER)' \
               f' WITH (tierConfig = `{self._config.to_proto_str()}`)'


class DropObjectBase(ScenarioTestHelper.IYqlble):
    """Drop a tier.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str) -> None:
        """Constructor.

        Args:
            name: Name (relative path) of the altered object."""

        super().__init__(name)

    @override
    def params(self) -> Dict[str, str]:
        return {'object_type': self._object_type()}

    @override
    def title(self):
        return f'Drop {self._object_type().lower()}'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'DROP OBJECT `{self._name}` (TYPE {self._object_type()})'

    @abstractmethod
    def _object_type(self) -> str:
        pass


class DropTier(DropObjectBase):
    """Drop a tier.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def _object_type(self):
        return 'TIER'


class DropTieringRule(DropObjectBase):
    """Drop a tier.

     See {ScenarioTestHelper.IYqlble}.
    """

    @override
    def _object_type(self):
        return 'TIERING_RULE'
