from dataclasses import dataclass

from ..common import Dumper, Loader
from ..definitions import DebugTrail
from ..provider.located_request import LocatedRequest


@dataclass(frozen=True)
class LoaderRequest(LocatedRequest[Loader]):
    pass


@dataclass(frozen=True)
class DumperRequest(LocatedRequest[Dumper]):
    pass


class StrictCoercionRequest(LocatedRequest[bool]):
    pass


class DebugTrailRequest(LocatedRequest[DebugTrail]):
    pass
