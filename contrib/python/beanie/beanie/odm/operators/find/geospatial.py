from abc import ABC
from enum import Enum
from typing import List, Optional

from beanie.odm.operators.find import BaseFindOperator


class BaseFindGeospatialOperator(BaseFindOperator, ABC): ...


class GeoIntersects(BaseFindGeospatialOperator):
    """
    `$geoIntersects` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    GeoIntersects(Place.geo, "Polygon", [[0,0], [1,1], [3,3]])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": [[0,0], [1,1], [3,3]],
                }
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/geoIntersects/>
    """

    def __init__(self, field, geo_type: str, coordinates: List[List[float]]):
        self.field = field
        self.geo_type = geo_type
        self.coordinates = coordinates

    @property
    def query(self):
        return {
            self.field: {
                "$geoIntersects": {
                    "$geometry": {
                        "type": self.geo_type,
                        "coordinates": self.coordinates,
                    }
                }
            }
        }


class GeoWithinTypes(str, Enum):
    Polygon = "Polygon"
    MultiPolygon = "MultiPolygon"


class GeoWithin(BaseFindGeospatialOperator):
    """
    `$geoWithin` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    GeoWithin(Place.geo, "Polygon", [[0,0], [1,1], [3,3]])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoWithin": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": [[0,0], [1,1], [3,3]],
                }
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/geoWithin/>
    """

    def __init__(
        self, field, geo_type: GeoWithinTypes, coordinates: List[List[float]]
    ):
        self.field = field
        self.geo_type = geo_type
        self.coordinates = coordinates

    @property
    def query(self):
        return {
            self.field: {
                "$geoWithin": {
                    "$geometry": {
                        "type": self.geo_type,
                        "coordinates": self.coordinates,
                    }
                }
            }
        }


class Box(BaseFindGeospatialOperator):
    """
    `$box` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    Box(Place.geo, lower_left=[10,12], upper_right=[15,20])
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$geoWithin": {
                "$box": [[10, 12], [15, 20]]
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/box/>
    """

    def __init__(
        self, field, lower_left: List[float], upper_right: List[float]
    ):
        self.field = field
        self.coordinates = [lower_left, upper_right]

    @property
    def query(self):
        return {self.field: {"$geoWithin": {"$box": self.coordinates}}}


class Near(BaseFindGeospatialOperator):
    """
    `$near` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    Near(Place.geo, 1.2345, 2.3456, min_distance=500)
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [1.2345, 2.3456],
                },
                "$maxDistance": 500,
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/near/>
    """

    operator = "$near"

    def __init__(
        self,
        field,
        longitude: float,
        latitude: float,
        max_distance: Optional[float] = None,
        min_distance: Optional[float] = None,
    ):
        self.field = field
        self.longitude = longitude
        self.latitude = latitude
        self.max_distance = max_distance
        self.min_distance = min_distance

    @property
    def query(self):
        expression = {
            self.field: {
                self.operator: {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [self.longitude, self.latitude],
                    },
                }
            }
        }
        if self.max_distance:
            expression[self.field][self.operator]["$maxDistance"] = (
                self.max_distance
            )  # type: ignore
        if self.min_distance:
            expression[self.field][self.operator]["$minDistance"] = (
                self.min_distance
            )  # type: ignore
        return expression


class NearSphere(Near):
    """
    `$nearSphere` query operator

    Example:

    ```python
    class GeoObject(BaseModel):
        type: str = "Point"
        coordinates: Tuple[float, float]

    class Place(Document):
        geo: GeoObject

        class Collection:
            name = "places"
            indexes = [
                [("geo", pymongo.GEOSPHERE)],  # GEO index
            ]

    NearSphere(Place.geo, 1.2345, 2.3456, min_distance=500)
    ```

    Will return query object like

    ```python
    {
        "geo": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [1.2345, 2.3456],
                },
                "$maxDistance": 500,
            }
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nearSphere/>
    """

    operator = "$nearSphere"
