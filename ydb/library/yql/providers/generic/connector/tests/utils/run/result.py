from dataclasses import dataclass
from typing import Optional, List

from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema


@dataclass
class Result:
    data_out: Optional[List]
    data_out_with_types: Optional[List]
    schema: Optional[Schema]
    output: str
    returncode: int
