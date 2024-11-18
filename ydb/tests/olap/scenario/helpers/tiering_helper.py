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
    access_key_secret: str
    secret_key_secret: str


class UpsertSecret(ScenarioTestHelper.IYqlble):
    """Override value of a secret.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str, value: str):
        self._name = name
        self._value = value

    @override
    def title(self):
        return 'Create secret'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        return f'UPSERT OBJECT `{self._name}` (TYPE SECRET) WITH (value="{self._value})'


class CreateExternalDataSource(ScenarioTestHelper.IYqlble):
    """Create an external data source.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, path: str, config: ObjectStorageParams, allow_existing: bool = False):
        self._path = path
        self._config = config
        self._allow_existing = allow_existing

    @override
    def title(self):
        return 'Create tiering rule'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        verb = "CREATE IF NOT EXISTS" if self._allow_existing else "CREATE"
        return f'{verb} EXTERNAL DATA SOURCE `{ctx.get_full_path(self._path)}` WITH (' \
               f'    SOURCE_TYPE="ObjectStorage",' \
               f'    LOCATION="http://{self._config.endpoint}/{self._config.bucket}",' \
                '    AUTH_METHOD="AWS"' \
                '    AWS_SECRET_ACCESS_KEY_SECRET_NAME="{self._config.access_key}"' \
                '    AWS_ACCESS_KEY_ID_SECRET_NAME="{self._config.secret_key}"' \
                ')'


class DropObjectBase(ScenarioTestHelper.IYqlble):
    """Base class for DROP queries.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, path: str, missing_ok: bool = False):
        self._path = path
        self._missing_ok = missing_ok

    @override
    def title(self):
        return f'Drop {self._get_object_type().lower()}'
    
    @abstractmethod
    def _get_object_type(self):
        pass

    @override
    def to_yql(self, ctx: TestContext) -> str:
        verb = "DROP IF EXISTS" if self._allow_existing else "DROP"
        return f'{verb} {self._get_object_type()} {ctx.get_full_path(self._path)}'


class DropExternalDataSource(DropObjectBase):
    @override
    def _get_object_type(self):
        return 'EXTERNAL DATA SOURCE'


class DropSecret(ScenarioTestHelper.IYqlble):
    """Drop secret.

     See {ScenarioTestHelper.IYqlble}.
    """

    def __init__(self, name: str, missing_ok: bool = False):
        self._name = name
        self._missing_ok = missing_ok

    @override
    def title(self):
        return 'Drop secret'

    @override
    def to_yql(self, ctx: TestContext) -> str:
        verb = "DROP IF EXISTS" if self._allow_existing else "DROP"
        return f'DROP OBJECT {self.name} (TYPE SECRET)'
