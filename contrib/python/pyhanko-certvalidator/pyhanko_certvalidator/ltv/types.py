import abc
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Optional

__all__ = [
    'ValidationTimingInfo',
    'ValidationTimingParams',
    'IssuedItemContainer',
]

# TODO potentially re-home these at some point


@dataclass(frozen=True)
class ValidationTimingInfo:
    validation_time: datetime
    best_signature_time: datetime
    point_in_time_validation: bool

    @classmethod
    def now(cls, tz: Optional[tzinfo] = None) -> 'ValidationTimingInfo':
        now = datetime.now(tz=tz or timezone.utc)
        return ValidationTimingInfo(
            validation_time=now,
            best_signature_time=now,
            point_in_time_validation=False,
        )


@dataclass(frozen=True)
class ValidationTimingParams:
    timing_info: ValidationTimingInfo
    time_tolerance: timedelta

    @property
    def validation_time(self):
        return self.timing_info.validation_time

    @property
    def best_signature_time(self):
        return self.timing_info.best_signature_time

    @property
    def point_in_time_validation(self):
        return self.timing_info.point_in_time_validation


class IssuedItemContainer(abc.ABC):
    """
    A container for some data object issued by an entity (e.g. a certificate).
    """

    @property
    def issuance_date(self) -> Optional[datetime]:
        """
        The issuance date of the item.
        """

        raise NotImplementedError
