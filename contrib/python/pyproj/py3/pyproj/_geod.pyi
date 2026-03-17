from typing import Any, NamedTuple

geodesic_version_str: str

class GeodIntermediateReturn(NamedTuple):
    npts: int
    del_s: float
    dist: float
    lons: Any
    lats: Any
    azis: Any

class Geod:
    initstring: str
    a: float
    b: float
    f: float
    es: float
    sphere: bool
    def __init__(
        self, a: float, f: float, sphere: bool, b: float, es: float
    ) -> None: ...
    def __reduce__(self) -> tuple[type[Geod], str]: ...
    def _fwd(
        self,
        lons: Any,
        lats: Any,
        az: Any,
        dist: Any,
        radians: bool = False,
        return_back_azimuth: bool = True,
    ) -> None: ...
    def _fwd_point(
        self,
        lons: float,
        lats: float,
        az: float,
        dist: float,
        radians: bool = False,
        return_back_azimuth: bool = True,
    ) -> tuple[float, float, float]: ...
    def _inv(
        self,
        lons1: Any,
        lats1: Any,
        lons2: Any,
        lats2: Any,
        radians: bool = False,
        return_back_azimuth: bool = False,
    ) -> None: ...
    def _inv_point(
        self,
        lons1: float,
        lats1: float,
        lons2: float,
        lats2: float,
        radians: bool = False,
        return_back_azimuth: bool = False,
    ) -> tuple[float, float, float]: ...
    def _inv_or_fwd_intermediate(
        self,
        lon1: float,
        lat1: float,
        lon2_or_azi1: float,
        lat2: float,
        npts: int,
        del_s: float,
        radians: bool,
        initial_idx: int,
        terminus_idx: int,
        flags: int,
        out_lons: Any,
        out_lats: Any,
        out_azis: Any,
        return_back_azimuth: bool,
        is_fwd: bool,
    ) -> GeodIntermediateReturn: ...
    def _line_length(self, lons: Any, lats: Any, radians: bool = False) -> float: ...
    def _polygon_area_perimeter(
        self, lons: Any, lats: Any, radians: bool = False
    ) -> tuple[float, float]: ...

def reverse_azimuth(azi: Any, radians: bool = False) -> None: ...
