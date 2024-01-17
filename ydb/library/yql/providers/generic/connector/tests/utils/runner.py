from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings, GenericSettings
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema, YsonList


@dataclass
class Result:
    data_out: Optional[YsonList]
    data_out_with_types: Optional[List]
    schema: Optional[Schema]
    stdout: str
    stderr: str
    returncode: int


class Runner(ABC):
    @abstractmethod
    def __init__(self, runner_path: Path, settings: Settings):
        pass

    @abstractmethod
    def run(self, test_dir: Path, script: str, generic_settings: GenericSettings) -> Result:
        pass
