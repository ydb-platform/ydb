from typing import NamedTuple

from pyproj.aoi import AreaOfInterest, AreaOfUse
from pyproj.enums import PJType

class Unit(NamedTuple):
    auth_name: str
    code: str
    name: str
    category: str
    conv_factor: float
    proj_short_name: str | None
    deprecated: bool

def get_units_map(
    auth_name: str | None = None,
    category: str | None = None,
    allow_deprecated: bool = False,
) -> dict[str, Unit]: ...
def get_authorities() -> list[str]: ...
def get_codes(
    auth_name: str, pj_type: PJType | str, allow_deprecated: bool = False
) -> list[str]: ...

class CRSInfo(NamedTuple):
    auth_name: str
    code: str
    name: str
    type: PJType
    deprecated: bool
    area_of_use: AreaOfUse | None
    projection_method_name: str | None

def query_crs_info(
    auth_name: str | None = None,
    pj_types: PJType | list[PJType] | None = None,
    area_of_interest: AreaOfInterest | None = None,
    contains: bool = False,
    allow_deprecated: bool = False,
) -> list[CRSInfo]: ...
def query_utm_crs_info(
    datum_name: str | None = None,
    area_of_interest: AreaOfInterest | None = None,
    contains: bool = False,
) -> list[CRSInfo]: ...
def get_database_metadata(key: str) -> str | None: ...
