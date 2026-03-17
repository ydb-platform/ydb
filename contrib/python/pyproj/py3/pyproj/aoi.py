"""
This module contains the structures related to areas of interest.
"""

from dataclasses import dataclass
from typing import NamedTuple, Union

from pyproj.utils import is_null


@dataclass(frozen=True)
class AreaOfInterest:
    """
    .. versionadded:: 2.3.0

    This is the area of interest for:

    - Transformations
    - Querying for CRS data.
    """

    #: The west bound in degrees of the area of interest.
    west_lon_degree: float
    #: The south bound in degrees of the area of interest.
    south_lat_degree: float
    #: The east bound in degrees of the area of interest.
    east_lon_degree: float
    #: The north bound in degrees of the area of interest.
    north_lat_degree: float

    def __post_init__(self):
        if (
            is_null(self.west_lon_degree)
            or is_null(self.south_lat_degree)
            or is_null(self.east_lon_degree)
            or is_null(self.north_lat_degree)
        ):
            raise ValueError("NaN or None values are not allowed.")


class AreaOfUse(NamedTuple):
    """
    .. versionadded:: 2.0.0

    Area of Use for CRS, CoordinateOperation, or a Transformer.
    """

    #: West bound of area of use.
    west: float
    #: South bound of area of use.
    south: float
    #: East bound of area of use.
    east: float
    #: North bound of area of use.
    north: float
    #: Name of area of use.
    name: str | None = None

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """
        The bounds of the area of use.

        Returns
        -------
        tuple[float, float, float, float]
            west, south, east, and north bounds.
        """
        return self.west, self.south, self.east, self.north

    def __str__(self) -> str:
        return f"- name: {self.name}\n- bounds: {self.bounds}"


@dataclass
class BBox:
    """
    Bounding box to check if data intersects/contains other
    bounding boxes.

    .. versionadded:: 3.0.0

    """

    #: West bound of bounding box.
    west: float
    #: South bound of bounding box.
    south: float
    #: East bound of bounding box.
    east: float
    #: North bound of bounding box.
    north: float

    def __post_init__(self):
        if (
            is_null(self.west)
            or is_null(self.south)
            or is_null(self.east)
            or is_null(self.north)
        ):
            raise ValueError("NaN or None values are not allowed.")

    def intersects(self, other: Union["BBox", AreaOfUse]) -> bool:
        """
        Parameters
        ----------
        other: BBox
            The other BBox to use to check.

        Returns
        -------
        bool:
            True if this BBox intersects the other bbox.
        """
        return (
            self.west < other.east
            and other.west < self.east
            and self.south < other.north
            and other.south < self.north
        )

    def contains(self, other: Union["BBox", AreaOfUse]) -> bool:
        """
        Parameters
        ----------
        other: Union["BBox", AreaOfUse]
            The other BBox to use to check.

        Returns
        -------
        bool:
            True if this BBox contains the other bbox.
        """
        return (
            other.west >= self.west
            and other.east <= self.east
            and other.south >= self.south
            and other.north <= self.north
        )
