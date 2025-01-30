from dataclasses import dataclass
from typing import Optional, List

from yt import yson
from ydb.library.yql.providers.generic.connector.tests.utils.schema import Schema


@dataclass
class Result:
    data_out: Optional[yson.yson_types.YsonList]
    data_out_with_types: Optional[List]
    schema: Optional[Schema]
    output: str
    returncode: int
