from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Granularity(str, Enum):
    """
    Time Series Granuality
    """

    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"


class TimeSeriesConfig(BaseModel):
    """
    Time Series Collection config
    """

    time_field: str
    meta_field: Optional[str] = None
    granularity: Optional[Granularity] = None
    bucket_max_span_seconds: Optional[int] = None
    bucket_rounding_second: Optional[int] = None
    expire_after_seconds: Optional[int] = None

    def build_query(self, collection_name: str) -> Dict[str, Any]:
        res: Dict[str, Any] = {"name": collection_name}
        timeseries: Dict[str, Any] = {"timeField": self.time_field}
        if self.meta_field is not None:
            timeseries["metaField"] = self.meta_field
        if self.granularity is not None:
            timeseries["granularity"] = self.granularity
        if self.bucket_max_span_seconds is not None:
            timeseries["bucketMaxSpanSeconds"] = self.bucket_max_span_seconds
        if self.bucket_rounding_second is not None:
            timeseries["bucketRoundingSeconds"] = self.bucket_rounding_second
        res["timeseries"] = timeseries
        if self.expire_after_seconds is not None:
            res["expireAfterSeconds"] = self.expire_after_seconds
        return res
