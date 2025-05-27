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
    access_key: str
    secret_key: str


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
        return f'UPSERT OBJECT `{self._name}` (TYPE SECRET) WITH (value="{self._value}")'

    @override
    def params(self) -> Dict[str, str]:
        return {"name": self._name}


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
        return f'CREATE EXTERNAL DATA SOURCE{' IF NOT EXISTS' if self._allow_existing else ''}' \
               f'`{ScenarioTestHelper(ctx).get_full_path(self._path)}` WITH (' \
                '    SOURCE_TYPE="ObjectStorage",' \
               f'    LOCATION="{self._config.endpoint}/{self._config.bucket}",' \
                '    AUTH_METHOD="AWS",' \
               f'    AWS_ACCESS_KEY_ID_SECRET_NAME="{self._config.access_key_secret}",' \
               f'    AWS_SECRET_ACCESS_KEY_SECRET_NAME="{self._config.secret_key_secret}",' \
                '    AWS_REGION="ru-central1"' \
                ')'

    @override
    def params(self) -> Dict[str, str]:
        return {"path": self._path}


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
        return f'DROP {self._get_object_type()}' \
               f'{" IF EXISTS" if self._missing_ok else ""}' \
               f' `{ScenarioTestHelper(ctx).get_full_path(self._path)}`'

    @override
    def params(self) -> Dict[str, str]:
        return {"path": self._path}


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
        return f'DROP OBJECT {self._name}{' IF EXISTS' if self._missing_ok else ''} (TYPE SECRET)'

    @override
    def params(self) -> Dict[str, str]:
        return {"name": self._name}
