from abc import ABC, abstractmethod


class TimeZoneNotFoundError(Exception):
    pass


class TimeZoneBackend(ABC):
    utc_tzobj = None
    all_tzstrs = None
    base_tzstrs = None

    @abstractmethod
    def is_tzobj(self, value):
        pass

    @abstractmethod
    def to_tzobj(self, tzstr):
        pass
