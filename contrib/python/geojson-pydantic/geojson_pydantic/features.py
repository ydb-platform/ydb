"""pydantic models for GeoJSON Feature objects."""

from typing import Any, Dict, Generic, Iterator, List, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field, StrictInt, StrictStr, field_validator

from geojson_pydantic.base import _GeoJsonBase
from geojson_pydantic.geometries import Geometry

Props = TypeVar("Props", bound=Union[Dict[str, Any], BaseModel])
Geom = TypeVar("Geom", bound=Geometry)


class Feature(_GeoJsonBase, Generic[Geom, Props]):
    """Feature Model"""

    type: Literal["Feature"]
    geometry: Union[Geom, None] = Field(...)
    properties: Union[Props, None] = Field(...)
    id: Optional[Union[StrictInt, StrictStr]] = None

    __geojson_exclude_if_none__ = {"bbox", "id"}

    @field_validator("geometry", mode="before")
    def set_geometry(cls, geometry: Any) -> Any:
        """set geometry from geo interface or input"""
        if hasattr(geometry, "__geo_interface__"):
            return geometry.__geo_interface__

        return geometry


Feat = TypeVar("Feat", bound=Feature)


class FeatureCollection(_GeoJsonBase, Generic[Feat]):
    """FeatureCollection Model"""

    type: Literal["FeatureCollection"]
    features: List[Feat]

    def iter(self) -> Iterator[Feat]:
        """iterate over features"""
        return iter(self.features)

    @property
    def length(self) -> int:
        """return features length"""
        return len(self.features)
