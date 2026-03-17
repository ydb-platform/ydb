# Copyright 2011 Tomo Krajina
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
GPX related stuff
"""

import logging as mod_logging
import math as mod_math
import collections as mod_collections
import copy as mod_copy
import datetime as mod_datetime

from . import utils as mod_utils
from . import geo as mod_geo
from . import gpxfield as mod_gpxfield

from typing import *

log = mod_logging.getLogger(__name__)

IGNORE_TOP_SPEED_PERCENTILES = 0.05

# GPX date format to be used when writing the GPX output:
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# GPX date format(s) used for parsing. The T between date and time and Z after
# time are allowed, too:
DATE_FORMATS = [
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
]

# Used in smoothing, sum must be 1:
SMOOTHING_RATIO = (0.4, 0.2, 0.4)

# When computing stopped time -- this is the minimum speed between two points,
# if speed is less than this value -- we'll assume it is zero
DEFAULT_STOPPED_SPEED_THRESHOLD = 1

# Fields used for all point elements (route point, track point, waypoint):
GPX_10_POINT_FIELDS = [
        mod_gpxfield.GPXField('latitude', attribute='lat', type=mod_gpxfield.FLOAT_TYPE, mandatory=True),
        mod_gpxfield.GPXField('longitude', attribute='lon', type=mod_gpxfield.FLOAT_TYPE, mandatory=True),
        mod_gpxfield.GPXField('elevation', 'ele', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('time', type=mod_gpxfield.TIME_TYPE),
        mod_gpxfield.GPXField('magnetic_variation', 'magvar', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('geoid_height', 'geoidheight', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('name'),
        mod_gpxfield.GPXField('comment', 'cmt'),
        mod_gpxfield.GPXField('description', 'desc'),
        mod_gpxfield.GPXField('source', 'src'),
        mod_gpxfield.GPXField('link', 'url'),
        mod_gpxfield.GPXField('link_text', 'urlname'),
        mod_gpxfield.GPXField('symbol', 'sym'),
        mod_gpxfield.GPXField('type'),
        mod_gpxfield.GPXField('type_of_gpx_fix', 'fix', possible=('none', '2d', '3d', 'dgps', 'pps', '3',)),
        mod_gpxfield.GPXField('satellites', 'sat', type=mod_gpxfield.INT_TYPE),
        mod_gpxfield.GPXField('horizontal_dilution', 'hdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('vertical_dilution', 'vdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('position_dilution', 'pdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('age_of_dgps_data', 'ageofdgpsdata', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('dgps_id', 'dgpsid', type=mod_gpxfield.INT_TYPE),
]
GPX_11_POINT_FIELDS = [
        # See GPX for description of text fields
        mod_gpxfield.GPXField('latitude', attribute='lat', type=mod_gpxfield.FLOAT_TYPE, mandatory=True),
        mod_gpxfield.GPXField('longitude', attribute='lon', type=mod_gpxfield.FLOAT_TYPE, mandatory=True),
        mod_gpxfield.GPXField('elevation', 'ele', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('time', type=mod_gpxfield.TIME_TYPE),
        mod_gpxfield.GPXField('magnetic_variation', 'magvar', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('geoid_height', 'geoidheight', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('name'),
        mod_gpxfield.GPXField('comment', 'cmt'),
        mod_gpxfield.GPXField('description', 'desc'),
        mod_gpxfield.GPXField('source', 'src'),
        'link:@link',
            mod_gpxfield.GPXField('link', attribute='href'),
            mod_gpxfield.GPXField('link_text', tag='text'),
            mod_gpxfield.GPXField('link_type', tag='type'),
        '/link',
        mod_gpxfield.GPXField('symbol', 'sym'),
        mod_gpxfield.GPXField('type'),
        mod_gpxfield.GPXField('type_of_gpx_fix', 'fix', possible=('none', '2d', '3d', 'dgps', 'pps', '3',)),
        mod_gpxfield.GPXField('satellites', 'sat', type=mod_gpxfield.INT_TYPE),
        mod_gpxfield.GPXField('horizontal_dilution', 'hdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('vertical_dilution', 'vdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('position_dilution', 'pdop', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('age_of_dgps_data', 'ageofdgpsdata', type=mod_gpxfield.FLOAT_TYPE),
        mod_gpxfield.GPXField('dgps_id', 'dgpsid', type=mod_gpxfield.INT_TYPE),
        mod_gpxfield.GPXExtensionsField('extensions', is_list=True),
]

# GPX1.0 track points have two more fields after time
# Note that this is not true for GPX1.1
GPX_TRACK_POINT_FIELDS = GPX_10_POINT_FIELDS[:4] \
        + [ \
                mod_gpxfield.GPXField('course', type=mod_gpxfield.FLOAT_TYPE), \
                mod_gpxfield.GPXField('speed', type=mod_gpxfield.FLOAT_TYPE) \
          ] \
        + GPX_10_POINT_FIELDS[4:]

# When possible, the result of various methods are named tuples defined here:
class TimeBounds(NamedTuple):
    start_time: Optional[mod_datetime.datetime]
    end_time: Optional[mod_datetime.datetime]
class MovingData(NamedTuple):
    moving_time: float
    stopped_time: float
    moving_distance: float
    stopped_distance: float
    max_speed: float
class UphillDownhill(NamedTuple):
    uphill: float
    downhill: float
class MinimumMaximum(NamedTuple):
    minimum: Optional[float]
    maximum: Optional[float]
class NearestLocationData(NamedTuple):  # this is also what walk() returns/iterates over
    location: "GPXTrackPoint"
    track_no: int
    segment_no: int
    point_no: int
class PointData(NamedTuple):
    point: "GPXTrackPoint"
    distance_from_start: float
    track_no: int
    segment_no: int
    point_no: int


class GPXException(Exception):
    """
    Exception used for invalid GPX files. It is used when the XML file is
    valid but something is wrong with the GPX data.
    """
    pass


class GPXBounds:
    gpx_10_fields = gpx_11_fields = [
            mod_gpxfield.GPXField('min_latitude', attribute='minlat', type=mod_gpxfield.FLOAT_TYPE),
            mod_gpxfield.GPXField('max_latitude', attribute='maxlat', type=mod_gpxfield.FLOAT_TYPE),
            mod_gpxfield.GPXField('min_longitude', attribute='minlon', type=mod_gpxfield.FLOAT_TYPE),
            mod_gpxfield.GPXField('max_longitude', attribute='maxlon', type=mod_gpxfield.FLOAT_TYPE),
    ]

    __slots__ = ('min_latitude', 'max_latitude', 'min_longitude', 'max_longitude')

    def __init__(self, min_latitude: Optional[float] = None, max_latitude: Optional[float] = None,
                 min_longitude: Optional[float] = None, max_longitude: Optional[float] = None) -> None:
        self.min_latitude = min_latitude
        self.max_latitude = max_latitude
        self.min_longitude = min_longitude
        self.max_longitude = max_longitude

    def __iter__(self) -> Iterator[Any]:
        return (self.min_latitude, self.max_latitude, self.min_longitude, self.max_longitude,).__iter__()

    def _min(self, a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is not None and b is not None:
            return min(a, b)
        return None

    def _max(self, a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is not None and b is not None:
            return max(a, b)
        return None

    def max_bounds(self, bounds: "GPXBounds") -> "GPXBounds":
        return GPXBounds(self._min(self.min_latitude, bounds.min_latitude),
            self._max(self.max_latitude, bounds.max_latitude),
            self._min(self.min_longitude, bounds.min_longitude),
            self._max(self.max_longitude, bounds.max_longitude))

class GPXXMLSyntaxException(GPXException):
    """
    Exception used when the XML syntax is invalid.

    The __cause__ can be a minidom or lxml exception (See http://www.python.org/dev/peps/pep-3134/).
    """
    def __init__(self, message: str, original_exception: BaseException) -> None:
        GPXException.__init__(self, message)
        self.__cause__ = original_exception


class GPXWaypoint(mod_geo.Location):
    gpx_10_fields = GPX_10_POINT_FIELDS
    gpx_11_fields = GPX_11_POINT_FIELDS

    __slots__ = ('latitude', 'longitude', 'elevation', 'time',
                 'magnetic_variation', 'geoid_height', 'name', 'comment',
                 'description', 'source', 'link', 'link_text', 'symbol',
                 'type', 'type_of_gpx_fix', 'satellites',
                 'horizontal_dilution', 'vertical_dilution',
                 'position_dilution', 'age_of_dgps_data', 'dgps_id',
                 'link_type', 'extensions')

    def __init__(self, latitude: Optional[float]=None, longitude: Optional[float]=None, elevation: Optional[float]=None,
                 time: Optional[mod_datetime.datetime]=None, name: Optional[str]=None, description: Optional[str]=None,
                 symbol: Optional[str]=None, type: Optional[str]=None, comment: Optional[str]=None,
                 horizontal_dilution: Optional[float]=None, vertical_dilution: Optional[float]=None,
                 position_dilution: Optional[float]=None) -> None:
        mod_geo.Location.__init__(self, latitude or 0, longitude or 0, elevation)
        self.time = time
        self.magnetic_variation: Optional[float] = None
        self.geoid_height: Optional[float] = None
        self.name = name
        self.comment = comment
        self.description = description
        self.source: Optional[str] = None
        self.link = None
        self.link_text = None
        self.link_type: Optional[str] = None
        self.symbol = symbol
        self.type = type
        self.type_of_gpx_fix: Optional[str] = None
        self.satellites: Optional[int] = None
        self.horizontal_dilution = horizontal_dilution
        self.vertical_dilution = vertical_dilution
        self.position_dilution = position_dilution
        self.age_of_dgps_data: Optional[float] = None
        self.dgps_id: Optional[int] = None
        self.extensions: List[Any] = [] # TODO

    def __str__(self) -> str:
        return f'[wpt{{{self.name}}}:{self.latitude},{self.longitude}@{self.elevation}]'

    def __repr__(self) -> str:
        parts = [f'{self.latitude}, {self.longitude}']
        for attribute in 'elevation', 'time', 'name', 'description', 'symbol', 'type', 'comment', \
                'horizontal_dilution', 'vertical_dilution', 'position_dilution':
            value = getattr(self, attribute)
            if value is not None:
                parts.append(f'{attribute}={value!r}')
        return f'GPXWaypoint({", ".join(parts)})'

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of the point by the specified delta

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust time into the future
            Negative time delta will adjust time into the past
        """
        if self.time:
            self.time += delta

    def remove_time(self) -> None:
        """ Will remove time metadata. """
        self.time = None

    def get_max_dilution_of_precision(self) -> Optional[float]:
        """
        Only care about the max dop for filtering, no need to go into too much detail
        """
        if self.horizontal_dilution is None:
            return None
        if self.vertical_dilution is None:
            return None
        if self.position_dilution is None:
            return None
        return max(self.horizontal_dilution, self.vertical_dilution, self.position_dilution)


class GPXRoutePoint(mod_geo.Location):
    gpx_10_fields = GPX_10_POINT_FIELDS
    gpx_11_fields = GPX_11_POINT_FIELDS

    __slots__ = ('latitude', 'longitude', 'elevation', 'time',
                 'magnetic_variation', 'geoid_height', 'name', 'comment',
                 'description', 'source', 'link', 'link_text', 'symbol',
                 'type', 'type_of_gpx_fix', 'satellites',
                 'horizontal_dilution', 'vertical_dilution',
                 'position_dilution', 'age_of_dgps_data', 'dgps_id',
                 'link_type', 'extensions')

    def __init__(self, latitude: Optional[float]=None, longitude: Optional[float]=None, elevation: Optional[float]=None, time: Optional[mod_datetime.datetime]=None, name: Optional[str]=None,
                 description: Optional[str]=None, symbol: Optional[str]=None, type: Optional[str]=None, comment: Optional[str]=None,
                 horizontal_dilution: Optional[float]=None, vertical_dilution: Optional[float]=None,
                 position_dilution: Optional[float]=None) -> None:

        mod_geo.Location.__init__(self, latitude or 0, longitude or 0, elevation)
        self.time = time
        self.magnetic_variation: Optional[float] = None
        self.geoid_height: Optional[float] = None
        self.name = name
        self.comment = comment
        self.description = description
        self.source: Optional[str] = None
        self.link: Optional[str] = None
        self.link_text: Optional[str] = None
        self.symbol = symbol
        self.type: Optional[str] = type
        self.type_of_gpx_fix: Optional[str] = None
        self.satellites: Optional[int] = None
        self.horizontal_dilution = horizontal_dilution
        self.vertical_dilution = vertical_dilution
        self.position_dilution = position_dilution
        self.age_of_dgps_data: Optional[float] = None
        self.dgps_id: Optional[int] = None
        self.link_type: Optional[str] = None
        self.extensions: List[Any] = [] # TODO

    def __str__(self) -> str:
        return f'[rtept{{{self.name}}}:{self.latitude},{self.longitude}@{self.elevation}]'

    def __repr__(self) -> str:
        parts = [f'{self.latitude}, {self.longitude}']
        for attribute in 'elevation', 'time', 'name', 'description', 'symbol', 'type', 'comment', \
                'horizontal_dilution', 'vertical_dilution', 'position_dilution':
            value = getattr(self, attribute)
            if value is not None:
                parts.append(f'{attribute}={value!r}')
        return f'GPXRoutePoint({", ".join(parts)})'

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of the point by the specified delta

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust time into the future
            Negative time delta will adjust time into the past
        """
        if self.time:
            self.time += delta

    def remove_time(self) -> None:
        """ Will remove time metadata. """
        self.time = None


class GPXRoute:
    gpx_10_fields = [
            mod_gpxfield.GPXField('name'),
            mod_gpxfield.GPXField('comment', 'cmt'),
            mod_gpxfield.GPXField('description', 'desc'),
            mod_gpxfield.GPXField('source', 'src'),
            mod_gpxfield.GPXField('link', 'url'),
            mod_gpxfield.GPXField('link_text', 'urlname'),
            mod_gpxfield.GPXField('number', type=mod_gpxfield.INT_TYPE),
            mod_gpxfield.GPXComplexField('points', tag='rtept', classs=GPXRoutePoint, is_list=True),
    ]
    gpx_11_fields = [
            # See GPX for description of text fields
            mod_gpxfield.GPXField('name'),
            mod_gpxfield.GPXField('comment', 'cmt'),
            mod_gpxfield.GPXField('description', 'desc'),
            mod_gpxfield.GPXField('source', 'src'),
            'link:@link',
                mod_gpxfield.GPXField('link', attribute='href'),
                mod_gpxfield.GPXField('link_text', tag='text'),
                mod_gpxfield.GPXField('link_type', tag='type'),
            '/link',
            mod_gpxfield.GPXField('number', type=mod_gpxfield.INT_TYPE),
            mod_gpxfield.GPXField('type'),
            mod_gpxfield.GPXExtensionsField('extensions', is_list=True),
            mod_gpxfield.GPXComplexField('points', tag='rtept', classs=GPXRoutePoint, is_list=True),
    ]

    __slots__ = ('name', 'comment', 'description', 'source', 'link',
                 'link_text', 'number', 'points', 'link_type', 'type',
                 'extensions')

    def __init__(self, name: Optional[str]=None, description: Optional[str]=None, number: Optional[int]=None) -> None:
        self.name = name
        self.comment: Optional[str] = None
        self.description = description
        self.source: Optional[str] = None
        self.link: Optional[str] = None
        self.link_text: Optional[str] = None
        self.number = number
        self.points: List[GPXRoutePoint] = []
        self.link_type: Optional[str] = None
        self.type: Optional[str] = None
        self.extensions: List[Any] = []

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of the all the points in the route by the specified delta.

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust time into the future
            Negative time delta will adjust time into the past
        """
        for point in self.points:
            point.adjust_time(delta)

    def remove_time(self) -> None:
        """ Removes time meta data from route. """
        for point in self.points:
            point.remove_time()

    def remove_elevation(self) -> None:
        """ Removes elevation data from route """
        for point in self.points:
            point.remove_elevation()

    def length(self) -> float:
        """
        Computes length (2-dimensional) of route.

        Returns:
        -----------
        length: float
            Length returned in meters
        """
        return mod_geo.length_2d(cast(List[mod_geo.Location], self.points))

    def get_center(self) -> Optional[mod_geo.Location]:
        """
        Get the center of the route.

        Returns
        -------
        center: Location
            latitude: latitude of center in degrees
            longitude: longitude of center in degrees
            elevation: not calculated here
        """
        if not self.points:
            return None

        if not self.points:
            return None

        sum_lat = 0.
        sum_lon = 0.
        n = 0.

        for point in self.points:
            n += 1.
            sum_lat += point.latitude
            sum_lon += point.longitude

        if not n:
            return mod_geo.Location(0, 0)

        return mod_geo.Location(latitude=sum_lat / n, longitude=sum_lon / n)

    def walk(self, only_points: bool=False) -> Iterator[Any]:
        """
        Generator for iterating over route points

        Parameters
        ----------
        only_points: boolean
            Only yield points (no index yielded)

        Yields
        ------
        point: GPXRoutePoint
            A point in the GPXRoute
        point_no: int
            Not included in yield if only_points is true
        """
        for point_no, point in enumerate(self.points):
            if only_points:
                yield point
            else:
                yield point, point_no

    def get_points_no(self) -> int:
        """
        Get the number of points in route.

        Returns
        ----------
        num_points : integer
            Number of points in route
        """
        return len(self.points)

    def move(self, location_delta: mod_geo.LocationDelta) -> None:
        """
        Moves each point in the route.

        Parameters
        ----------
        location_delta: LocationDelta
            LocationDelta to move each point
        """
        for route_point in self.points:
            route_point.move(location_delta)

    def __repr__(self) -> str:
        parts = []
        for attribute in 'name', 'description', 'number':
            value = getattr(self, attribute)
            if value is not None:
                parts.append(f'{attribute}={value!r}')
        parts.append(f'points=[{"..." if self.points else ""}]')
        return f'GPXRoute({", ".join(parts)})'


class GPXTrackPoint(mod_geo.Location):
    gpx_10_fields = GPX_TRACK_POINT_FIELDS
    gpx_11_fields = GPX_11_POINT_FIELDS

    __slots__ = ('latitude', 'longitude', 'elevation', 'time', 'course',
                 'speed', 'magnetic_variation', 'geoid_height', 'name',
                 'comment', 'description', 'source', 'link', 'link_text',
                 'symbol', 'type', 'type_of_gpx_fix', 'satellites',
                 'horizontal_dilution', 'vertical_dilution',
                 'position_dilution', 'age_of_dgps_data', 'dgps_id',
                 'link_type', 'extensions')

    def __init__(self, latitude: Optional[float]=None, longitude: Optional[float]=None, elevation: Optional[float]=None,
                 time: Optional[mod_datetime.datetime]=None, symbol: Optional[str]=None, comment: Optional[str]=None,
                 horizontal_dilution: Optional[float]=None, vertical_dilution: Optional[float]=None,
                 position_dilution: Optional[float]=None, speed: Optional[float]=None,
                 name: Optional[str]=None) -> None:
        mod_geo.Location.__init__(self, latitude or 0, longitude or 0, elevation)
        self.time = time
        self.course = None
        self.speed = speed
        self.magnetic_variation: Optional[float] = None
        self.geoid_height: Optional[float] = None
        self.name = name
        self.comment = comment
        self.description: Optional[str] = None
        self.source: Optional[str] = None
        self.link: Optional[str] = None
        self.link_text: Optional[str] = None
        self.link_type: Optional[str] = None
        self.symbol: Optional[str] = symbol
        self.type: Optional[str] = None
        self.type_of_gpx_fix: Optional[str] = None
        self.satellites: Optional[float] = None
        self.horizontal_dilution = horizontal_dilution
        self.vertical_dilution = vertical_dilution
        self.position_dilution = position_dilution
        self.age_of_dgps_data: Optional[float] = None
        self.dgps_id: Optional[int] = None
        self.extensions: List[Any] = []

    def __repr__(self) -> str:
        parts = [f'{self.latitude}, {self.longitude}']
        for attribute in 'elevation', 'time', 'symbol', 'comment', 'horizontal_dilution', \
                'vertical_dilution', 'position_dilution', 'speed', 'name':
            value = getattr(self, attribute)
            if value is not None:
                parts.append(f'{attribute}={value!r}')
        return f'GPXTrackPoint({", ".join(parts)})'

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of the point by the specified delta

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust time into the future
            Negative time delta will adjust time into the past
        """
        if self.time:
            self.time += delta

    def remove_time(self) -> None:
        """ Will remove time metadata. """
        self.time = None

    def time_difference(self, track_point: "GPXTrackPoint") -> Optional[float]:
        """
        Get time difference between specified point and this point.

        Parameters
        ----------
        track_point : GPXTrackPoint

        Returns
        ----------
        time_difference : float
            Time difference returned in seconds
        """
        if not self.time or not track_point or not track_point.time:
            return None

        time_1 = self.time
        time_2 = track_point.time

        if time_1 == time_2:
            return 0

        if time_1 > time_2:
            delta = time_1 - time_2
        else:
            delta = time_2 - time_1

        return mod_utils.total_seconds(delta)

    def speed_between(self, track_point: "GPXTrackPoint") -> Optional[float]:
        """
        Compute the speed between specified point and this point.

        NOTE: This is a computed speed, not the GPXTrackPoint speed that comes
              the GPX file.

        Parameters
        ----------
        track_point : GPXTrackPoint

        Returns
        ----------
        speed : float
            Speed returned in meters/second
        """
        if not track_point:
            return None

        seconds = self.time_difference(track_point)
        length = self.distance_3d(track_point)
        if not length:
            length = self.distance_2d(track_point)

        if not seconds or length is None:
            return None

        return length / seconds

    def course_between(self, track_point: "GPXTrackPoint", loxodromic: bool=True) -> Optional[float]:
        """
        Compute the instantaneous course from one point to another.

        Both loxodromic (Rhumb line) and orthodromic (Great circle) navigation
        models are available.

        The default navigation model is loxodromic.

        There is no difference between these models in course computation
        when points are relatively close to each other (less than ≈150 km)

        In most cases the default model is OK.

        However, the orthodromic navigation model can be important for the
        long-distance (> ≈1000 km) logs acquired from maritime transport
        or aeroplanes

        Generally, the choice between these two models depends on a vehicle type,
        distance and navigation equipment used.

        More information on these two navigation models:
        https://www.movable-type.co.uk/scripts/latlong.html

        NOTE: This is a computed course, not the GPXTrackPoint course that comes in
              the GPX file.

        Parameters
        ----------
        track_point : GPXTrackPoint
        loxodromic : True
                     Set to False to use the orthodromic navigation model

        Returns
        ----------
        course : float
                Course returned in decimal degrees, true (not magnetic)
                (0.0 <= value < 360.0)
        """

        if not track_point:
            return None

        course = mod_geo.get_course(self.latitude, self.longitude,
                                    track_point.latitude, track_point.longitude,
                                    loxodromic)
        return course

    def __str__(self) -> str:
        return f'[trkpt:{self.latitude},{self.longitude}@{self.elevation}@{self.time}]'

class GPXTrackSegment:
    gpx_10_fields = [
            mod_gpxfield.GPXComplexField('points', tag='trkpt', classs=GPXTrackPoint, is_list=True),
    ]
    gpx_11_fields = [
            mod_gpxfield.GPXComplexField('points', tag='trkpt', classs=GPXTrackPoint, is_list=True),
            mod_gpxfield.GPXExtensionsField('extensions', is_list=True),
    ]

    __slots__ = ('points', 'extensions', )

    def __init__(self, points: Optional[List[GPXTrackPoint]]=None) -> None:
        self.points: List[GPXTrackPoint] = points if points else []
        self.extensions: List[Any] = []

    def simplify(self, max_distance: Optional[float]=None) -> None:
        """
        Simplify using the Ramer-Douglas-Peucker algorithm: http://en.wikipedia.org/wiki/Ramer-Douglas-Peucker_algorithm
        """
        self.points = mod_geo.simplify_polyline(self.points, max_distance) # type: ignore

    def reduce_points(self, min_distance: float) -> None:
        """
        Reduces the number of points in the track segment. Segment points will
        be updated in place.

        Parameters
        ----------
        min_distance : float
            The minimum separation in meters between points
        """
        reduced_points: List[GPXTrackPoint] = []
        for point in self.points:
            if reduced_points:
                distance = reduced_points[-1].distance_3d(point)
                if min_distance is not None and distance is not None and distance >= min_distance:
                    reduced_points.append(point)
            else:
                # Leave first point:
                reduced_points.append(point)

        self.points = reduced_points

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of all points in the segment by the specified delta

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust point times into the future
            Negative time delta will adjust point times into the past
        """
        for track_point in self.points:
            track_point.adjust_time(delta)

    def remove_time(self) -> None:
        """ Removes time data for all points in the segment. """
        for track_point in self.points:
            track_point.remove_time()

    def remove_elevation(self) -> None:
        """ Removes elevation data for all points in the segment. """
        for track_point in self.points:
            track_point.remove_elevation()

    def length_2d(self) -> Optional[float]:
        """
        Computes 2-dimensional length (meters) of segment (only latitude and
        longitude, no elevation).

        Returns
        ----------
        length : float
            Length returned in meters
        """
        return mod_geo.length_2d(self.points) # type: ignore

    def length_3d(self) -> float:
        """
        Computes 3-dimensional length of segment (latitude, longitude, and
        elevation).

        Returns
        ----------
        length : float
            Length returned in meters
        """
        return mod_geo.length_3d(self.points) # type: ignore

    def move(self, location_delta: mod_geo.LocationDelta) -> None:
        """
        Moves each point in the segment.

        Parameters
        ----------
        location_delta: LocationDelta object
            Delta (distance/angle or lat/lon offset to apply each point in the
            segment
        """
        for track_point in self.points:
            track_point.move(location_delta)

    def walk(self, only_points: bool=False) -> Iterator[Any]: # Union[GPXTrackPoint, Tuple[GPXTrackPoint, int]]]:
        """
        Generator for iterating over segment points

        Parameters
        ----------
        only_points: boolean
            Only yield points (no index yielded)

        Yields
        ------
        point: GPXTrackPoint
            A point in the sement
        point_no: int
            Not included in yield if only_points is true
        """
        for point_no, point in enumerate(self.points if self.points else []):
            if only_points:
                yield point
            else:
                yield point, point_no

    def get_points_no(self) -> int:
        """
        Gets the number of points in segment.

        Returns
        ----------
        num_points : integer
            Number of points in segment
        """
        if not self.points:
            return 0
        return len(self.points)

    def split(self, point_no: int) -> Tuple["GPXTrackSegment", "GPXTrackSegment"]:
        """
        Splits the segment into two parts.

        Parameters
        ----------
        point_no : integer
            The index of the track point in the segment to split
        """
        part_1 = self.points[:point_no + 1]
        part_2 = self.points[point_no + 1:]
        return GPXTrackSegment(part_1), GPXTrackSegment(part_2)

    def join(self, track_segment: "GPXTrackSegment") -> None:
        """ Joins with another segment """
        self.points += track_segment.points

    def remove_point(self, point_no: int) -> None:
        """ Removes a point specified by index from the segment """
        if point_no < 0 or point_no >= len(self.points):
            return

        part_1 = self.points[:point_no]
        part_2 = self.points[point_no + 1:]

        self.points = part_1 + part_2

    def get_moving_data(self, stopped_speed_threshold: Optional[float]=None, raw: bool=False, speed_extreemes_percentiles: float=IGNORE_TOP_SPEED_PERCENTILES, ignore_nonstandard_distances: bool = True) -> Optional[MovingData]:
        """
        Return a tuple of (moving_time, stopped_time, moving_distance,
        stopped_distance, max_speed) that may be used for detecting the time
        stopped, and max speed. Not that those values are not absolutely true,
        because the "stopped" or "moving" information aren't saved in the segment.

        Because of errors in the GPS recording, it may be good to calculate
        them on a reduced and smoothed version of the track.

        Parameters
        ----------
        stopped_speed_threshold : float
            speeds (km/h) below this threshold are treated as if having no
            movement. Default is 1 km/h.

        Returns
        ----------
        moving_data : MovingData : named tuple
            moving_time : float
                time (seconds) of segment in which movement was occurring
            stopped_time : float
                time (seconds) of segment in which no movement was occurring
            stopped_distance : float
                distance (meters) travelled during stopped times
            moving_distance : float
                distance (meters) travelled during moving times
            max_speed : float
                Maximum speed (m/s) during the segment.
        """
        if not stopped_speed_threshold:
            stopped_speed_threshold = DEFAULT_STOPPED_SPEED_THRESHOLD

        if raw:
            speed_extreemes_percentiles=0
            ignore_nonstandard_distances=False

        moving_time = 0.
        stopped_time = 0.

        moving_distance = 0.
        stopped_distance = 0.

        speeds_and_distances = []

        for previous, point in zip(self.points, self.points[1:]):

            # Won't compute max_speed for first and last because of common GPS
            # recording errors, and because smoothing don't work well for those
            # points:
            if point.time and previous.time:
                timedelta = point.time - previous.time

                if point.elevation and previous.elevation:
                    distance = point.distance_3d(previous)
                else:
                    distance = point.distance_2d(previous)

                seconds = mod_utils.total_seconds(timedelta)
                speed_kmh: float = 0
                if seconds > 0 and distance is not None:
                    # TODO: compute threshold in m/s instead this to kmh every time:
                    speed_kmh = (distance / 1000) / (seconds / 60 ** 2)
                    if distance:
                        if speed_kmh <= stopped_speed_threshold:
                            stopped_time += seconds
                            stopped_distance += distance
                        else:
                            moving_time += seconds
                            moving_distance += distance
                        if moving_time:
                            speeds_and_distances.append((distance / seconds, distance, ))

        max_speed = None
        if speeds_and_distances:
            max_speed = mod_geo.calculate_max_speed(speeds_and_distances, speed_extreemes_percentiles, ignore_nonstandard_distances)

        return MovingData(moving_time, stopped_time, moving_distance, stopped_distance, max_speed or 0.0)

    def get_time_bounds(self) -> TimeBounds:
        """
        Gets the time bound (start and end) of the segment.

        returns
        ----------
        time_bounds : TimeBounds named tuple
            start_time : datetime
                Start time of the first segment in track
            end time : datetime
                End time of the last segment in track
        """
        start_time = None
        end_time = None

        for point in self.points:
            if point.time:
                if not start_time:
                    start_time = point.time
                if point.time:
                    end_time = point.time

        return TimeBounds(start_time, end_time)

    def get_bounds(self) -> Optional[GPXBounds]:
        """
        Gets the latitude and longitude bounds of the segment.

        Returns
        ----------
        bounds : Bounds named tuple
            min_latitude : float
                Minimum latitude of segment in decimal degrees [-90, 90]
            max_latitude : float
                Maximum latitude of segment in decimal degrees [-90, 90]
            min_longitude : float
                Minimum longitude of segment in decimal degrees [-180, 180]
            max_longitude : float
                Maximum longitude of segment in decimal degrees [-180, 180]
        """
        min_lat = None
        max_lat = None
        min_lon = None
        max_lon = None

        for point in self.points:
            if min_lat is None or point.latitude < min_lat:
                min_lat = point.latitude
            if max_lat is None or point.latitude > max_lat:
                max_lat = point.latitude
            if min_lon is None or point.longitude < min_lon:
                min_lon = point.longitude
            if max_lon is None or point.longitude > max_lon:
                max_lon = point.longitude

        if min_lat and max_lat and min_lon and max_lon:
            return GPXBounds(min_lat, max_lat, min_lon, max_lon)
        return None

    def get_speed(self, point_no: int) -> Optional[float]:
        """
        Computes the speed at the specified point index.

        Parameters
        ----------
        point_no : integer
            index of the point used to compute speed

        Returns
        ----------
        speed : float
            Speed returned in m/s
        """
        point = self.points[point_no]

        previous_point = None
        next_point = None

        if 0 < point_no < len(self.points):
            previous_point = self.points[point_no - 1]
        if 0 <= point_no < len(self.points) - 1:
            next_point = self.points[point_no + 1]

        #log.debug('previous: %s' % previous_point)
        #log.debug('next: %s' % next_point)

        speed_1 = point.speed_between(previous_point) if previous_point else None
        speed_2 = point.speed_between(next_point) if next_point else None

        if speed_1:
            speed_1 = abs(speed_1)
        if speed_2:
            speed_2 = abs(speed_2)

        if speed_1 and speed_2:
            return (speed_1 + speed_2) / 2

        if speed_1:
            return speed_1

        return speed_2

    def add_elevation(self, delta: float) -> None:
        """
        Adjusts elevation data for segment.

        Parameters
        ----------
        delta : float
            Elevation delta in meters to apply to track
        """
        log.debug('delta = %s', delta)

        if not delta:
            return

        for track_point in self.points:
            if track_point.elevation is not None:
                track_point.elevation += delta

    def add_missing_data(self,
                         get_data_function: Callable[[GPXTrackPoint], Any],
                         add_missing_function: Callable[[List[GPXTrackPoint], GPXTrackPoint, GPXTrackPoint, List[float]], None]) -> None:
        """
        Calculate missing data.

        Parameters
        ----------
        get_data_function : object
            Returns the data from point
        add_missing_function : void
            Function with the following arguments: array with points with missing data, the point before them (with data),
            the point after them (with data), and distance ratios between points in the interval (the sum of distances ratios
            will be 1)
        """
        if not get_data_function:
            raise GPXException(f'Invalid get_data_function: {get_data_function}')
        if not add_missing_function:
            raise GPXException(f'Invalid add_missing_function: {add_missing_function}')

        # Points (*without* data) between two points (*with* data):
        interval: List[GPXTrackPoint] = []
        # Point (*with* data) before and after the interval:
        start_point = None

        previous_point = None
        for track_point in self.points:
            data = get_data_function(track_point)
            if data is None and previous_point:
                if not start_point:
                    start_point = previous_point
                interval.append(track_point)
            else:
                if interval and start_point:
                    distances_ratios = self._get_interval_distances_ratios(interval, start_point, track_point)
                    add_missing_function(interval, start_point, track_point, distances_ratios)
                    start_point = None
                    interval = []
            previous_point = track_point

    def _get_interval_distances_ratios(self, interval: List[GPXTrackPoint], start: GPXTrackPoint, end: GPXTrackPoint) -> List[float]:
        assert start, start
        assert end, end
        assert interval, interval
        assert len(interval) > 0, interval

        distances = []
        distance_from_start: float = 0
        previous_point = start
        for point in interval:
            dist = point.distance_3d(previous_point)
            if dist is not None:
                distance_from_start += dist
                distances.append(distance_from_start)
                previous_point = point

        dist = interval[-1].distance_3d(end)
        from_start_to_end = None
        if dist:
            from_start_to_end = distances[-1] + dist

        assert len(interval) == len(distances)

        return [(distance / from_start_to_end) if from_start_to_end else 0
                for distance in distances]

    def get_duration(self) -> Optional[float]:
        """
        Calculates duration or track segment

        Returns
        -------
        duration: float
            Duration in seconds
        """
        if not self.points or len(self.points) < 2:
            return 0.0

        # Search for start:
        first = self.points[0]
        if not first.time:
            first = self.points[1]

        last = self.points[-1]
        if not last.time:
            last = self.points[-2]

        if not last.time or not first.time:
            log.debug('Can\'t find time')
            return None

        if last.time < first.time:
            log.debug('Not enough time data')
            return None

        return mod_utils.total_seconds(last.time - first.time)

    def get_uphill_downhill(self) -> UphillDownhill:
        """
        Calculates the uphill and downhill elevation climbs for the track
        segment. If elevation for some points is not found those are simply
        ignored.

        Returns
        -------
        uphill_downhill: UphillDownhill named tuple
            uphill: float
                Uphill elevation climbs in meters
            downhill: float
                Downhill elevation descent in meters
        """
        if not self.points:
            return UphillDownhill(0, 0)

        elevations = [point.elevation for point in self.points]
        uphill, downhill = mod_geo.calculate_uphill_downhill(elevations)

        return UphillDownhill(uphill, downhill)

    def get_elevation_extremes(self) -> MinimumMaximum:
        """
        Calculate elevation extremes of track segment

        Returns
        -------
        min_max_elevation: MinimumMaximum named tuple
            minimum: float
                Minimum elevation in meters
            maximum: float
                Maximum elevation in meters
        """
        if not self.points:
            return MinimumMaximum(None, None)

        elevations = [x.elevation for x in self.points if x.elevation is not None]
        if not elevations:
            return MinimumMaximum(None, None)
        return MinimumMaximum(min(elevations), max(elevations))

    def get_location_at(self, time: mod_datetime.datetime) -> Optional[GPXTrackPoint]:
        """
        Gets approx. location at given time. Note that, at the moment this
        method returns an instance of GPXTrackPoint in the future -- this may
        be a mod_geo.Location instance with approximated latitude, longitude
        and elevation!
        """
        if not self.points:
            return None

        if not time:
            return None

        first_time = self.points[0].time
        last_time = self.points[-1].time

        if not first_time and not last_time:
            log.debug('No times for track segment')
            return None

        if first_time and time and last_time and not first_time <= time <= last_time:
            log.debug(f'Not in track (search for:{time}, start:{first_time}, end:{last_time})')
            return None

        for point in self.points:
            if point.time and time <= point.time:
                # TODO: If between two points -- approx position!
                # return mod_geo.Location(point.latitude, point.longitude)
                return point
        
        return None

    def get_nearest_location(self, location: mod_geo.Location) -> Optional[NearestLocationData]:
        """ Return the (location, track_point_no) on this track segment """
        return min((NearestLocationData(pt, -1, -1, pt_no) for (pt, pt_no) in self.walk()) # type: ignore
                   ,key=lambda x: x.location.distance_2d(location) if x is not None else mod_math.inf
                   ,default=None)

    def smooth(self, vertical: bool=True, horizontal: bool=False, remove_extremes: bool=False) -> None:
        """ "Smooths" the elevation graph. Can be called multiple times. """
        if len(self.points) <= 3:
            return

        elevations: List[float] = []
        latitudes: List[float] = []
        longitudes: List[float] = []

        for point in self.points:
            elevations.append(point.elevation or 0)
            latitudes.append(point.latitude or 0)
            longitudes.append(point.longitude or 0)

        avg_distance: float = 0
        avg_elevation_delta: float = 1
        if remove_extremes:
            # compute the average distance between two points:
            distances: List[float] = []
            elevations_delta = []
            for prev, cur in zip(self.points, self.points[1:]):
                dist = prev.distance_2d(cur)
                if dist:
                    distances.append(dist)
                if cur.elevation is not None and prev.elevation is not None:
                    elevations_delta.append(abs(cur.elevation - prev.elevation))
            if distances:
                avg_distance = 1.0 * sum(distances) / len(distances)
            if elevations_delta:
                avg_elevation_delta = 1.0 * sum(elevations_delta) / len(elevations_delta)

        # If The point moved more than this number * the average distance between two
        # points -- then is a candidate for deletion:
        # TODO: Make this a method parameter
        remove_2d_extremes_threshold = 1.75 * avg_distance
        remove_elevation_extremes_threshold = avg_elevation_delta * 5  # TODO: Param

        new_track_points = [self.points[0]]

        for i in range(len(self.points))[1:-1]:
            new_point = None
            point_removed = False
            if vertical and elevations[i - 1] and elevations[i] and elevations[i + 1]:
                old_elevation = self.points[i].elevation
                new_elevation = SMOOTHING_RATIO[0] * elevations[i - 1] + \
                    SMOOTHING_RATIO[1] * elevations[i] + \
                    SMOOTHING_RATIO[2] * elevations[i + 1]

                if not remove_extremes:
                    self.points[i].elevation = new_elevation

                if remove_extremes:
                    # The point must be enough distant to *both* neighbours:
                    d1 = abs((old_elevation or 0) - elevations[i - 1])
                    d2 = abs((old_elevation or 0) - elevations[i + 1])
                    #print d1, d2, remove_2d_extremes_threshold

                    # TODO: Remove extremes threshold is meant only for 2D, elevation must be
                    # computed in different way!
                    if min(d1, d2) < remove_elevation_extremes_threshold and abs((old_elevation or 0) - (new_elevation or 0)) < remove_2d_extremes_threshold:
                        new_point = self.points[i]
                    else:
                        #print 'removed elevation'
                        point_removed = True
                else:
                    new_point = self.points[i]
            else:
                new_point = self.points[i]

            if horizontal:
                old_latitude = self.points[i].latitude
                new_latitude = SMOOTHING_RATIO[0] * latitudes[i - 1] + \
                    SMOOTHING_RATIO[1] * latitudes[i] + \
                    SMOOTHING_RATIO[2] * latitudes[i + 1]
                old_longitude = self.points[i].longitude
                new_longitude = SMOOTHING_RATIO[0] * longitudes[i - 1] + \
                    SMOOTHING_RATIO[1] * longitudes[i] + \
                    SMOOTHING_RATIO[2] * longitudes[i + 1]

                if not remove_extremes:
                    self.points[i].latitude = new_latitude
                    self.points[i].longitude = new_longitude

                # TODO: This is not ideal.. Because if there are points A, B and C on the same
                # line but B is very close to C... This would remove B (and possibly) A even though
                # it is not an extreme. This is the reason for this algorithm:
                d1 = mod_geo.distance(latitudes[i - 1], longitudes[i - 1], None, latitudes[i], longitudes[i], None)
                d2 = mod_geo.distance(latitudes[i + 1], longitudes[i + 1], None, latitudes[i], longitudes[i], None)
                dist = mod_geo.distance(latitudes[i - 1], longitudes[i - 1], None, latitudes[i + 1], longitudes[i + 1], None)

                #print d1, d2, d, remove_extremes

                if d1 + d2 > dist * 1.5 and remove_extremes:
                    dist = mod_geo.distance(old_latitude, old_longitude, None, new_latitude, new_longitude, None)
                    #print "d, threshold = ", d, remove_2d_extremes_threshold
                    if dist < remove_2d_extremes_threshold:
                        new_point = self.points[i]
                    else:
                        #print 'removed 2d'
                        point_removed = True
                else:
                    new_point = self.points[i]

            if new_point and not point_removed:
                new_track_points.append(new_point)

        new_track_points.append(self.points[- 1])

        #print 'len=', len(new_track_points)

        self.points = new_track_points

    def has_times(self) -> bool:
        """
        Returns if points in this segment contains timestamps.

        The first point, the last point, and 75% of the points must have times
        for this method to return true.
        """
        if not self.points:
            return True
            # ... or otherwise one empty track segment would change the entire
            # track's "has_times" status!

        found = 0
        for track_point in self.points:
            if track_point.time:
                found += 1

        return len(self.points) > 2 and found / len(self.points) > .75

    def has_elevations(self) -> bool:
        """
        Returns if points in this segment contains elevation.

        The first point, the last point, and at least 75% of the points must
        have elevation for this method to return true.
        """
        if not self.points:
            return True
            # ... or otherwise one empty track segment would change the entire
            # track's "has_times" status!

        found = 0
        for track_point in self.points:
            if track_point.elevation:
                found += 1

        return len(self.points) > 2 and found / len(self.points) > .75


    def __repr__(self) -> str:
        return f'GPXTrackSegment(points=[{"..." if self.points else ""}])'

    def clone(self) -> "GPXTrackSegment":
        return mod_copy.deepcopy(self)


class GPXTrack:
    gpx_10_fields = [
            mod_gpxfield.GPXField('name'),
            mod_gpxfield.GPXField('comment', 'cmt'),
            mod_gpxfield.GPXField('description', 'desc'),
            mod_gpxfield.GPXField('source', 'src'),
            mod_gpxfield.GPXField('link', 'url'),
            mod_gpxfield.GPXField('link_text', 'urlname'),
            mod_gpxfield.GPXField('number', type=mod_gpxfield.INT_TYPE),
            mod_gpxfield.GPXComplexField('segments', tag='trkseg', classs=GPXTrackSegment, is_list=True),
    ]
    gpx_11_fields = [
            # See GPX for text field description
            mod_gpxfield.GPXField('name'),
            mod_gpxfield.GPXField('comment', 'cmt'),
            mod_gpxfield.GPXField('description', 'desc'),
            mod_gpxfield.GPXField('source', 'src'),
            'link:@link',
                mod_gpxfield.GPXField('link', attribute='href'),
                mod_gpxfield.GPXField('link_text', tag='text'),
                mod_gpxfield.GPXField('link_type', tag='type'),
            '/link',
            mod_gpxfield.GPXField('number', type=mod_gpxfield.INT_TYPE),
            mod_gpxfield.GPXField('type'),
            mod_gpxfield.GPXExtensionsField('extensions', is_list=True),
            mod_gpxfield.GPXComplexField('segments', tag='trkseg', classs=GPXTrackSegment, is_list=True),
    ]

    __slots__ = ('name', 'comment', 'description', 'source', 'link',
                 'link_text', 'number', 'segments', 'link_type', 'type',
                 'extensions')

    def __init__(self, name: Optional[str]=None, description: Optional[str]=None, number: Optional[int]=None) -> None:
        self.name = name
        self.comment: Optional[str] = None
        self.description = description
        self.source: Optional[str] = None
        self.link: Optional[str] = None
        self.link_text: Optional[str] = None
        self.number = number
        self.segments: List[GPXTrackSegment] = []
        self.link_type = None
        self.type = None
        self.extensions: List[Any] = [] # TODO

    def simplify(self, max_distance: Optional[float]=None) -> None:
        """
        Simplify using the Ramer-Douglas-Peucker algorithm: http://en.wikipedia.org/wiki/Ramer-Douglas-Peucker_algorithm
        """
        for segment in self.segments:
            segment.simplify(max_distance=max_distance)

    def reduce_points(self, min_distance: float) -> None:
        """
        Reduces the number of points in the track. Segment points will be
        updated in place.

        Parameters
        ----------
        min_distance : float
            The minimum separation in meters between points
        """
        for segment in self.segments:
            segment.reduce_points(min_distance)

    def adjust_time(self, delta: mod_datetime.timedelta) -> None:
        """
        Adjusts the time of all segments in the track by the specified delta

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust time into the future
            Negative time delta will adjust time into the past
        """
        for segment in self.segments:
            segment.adjust_time(delta)

    def remove_time(self) -> None:
        """ Removes time data for all points in all segments of track. """
        for segment in self.segments:
            segment.remove_time()

    def remove_elevation(self) -> None:
        """ Removes elevation data for all points in all segments of track. """
        for segment in self.segments:
            segment.remove_elevation()

    def remove_empty(self) -> None:
        """ Removes empty segments in track """
        result = []

        for segment in self.segments:
            if len(segment.points) > 0:
                result.append(segment)

        self.segments = result

    def length_2d(self) -> float:
        """
        Computes 2-dimensional length (meters) of track (only latitude and
        longitude, no elevation). This is the sum of the 2D length of all
        segments.

        Returns
        ----------
        length : float
            Length returned in meters
        """
        length: float = 0
        for track_segment in self.segments:
            d = track_segment.length_2d()
            if d:
                length += d
        return length

    def get_time_bounds(self) -> TimeBounds:
        """
        Gets the time bound (start and end) of the track.

        Returns
        ----------
        time_bounds : TimeBounds named tuple
            start_time : datetime
                Start time of the first segment in track
            end time : datetime
                End time of the last segment in track
        """
        start_time = None
        end_time = None

        for track_segment in self.segments:
            point_start_time, point_end_time = track_segment.get_time_bounds()
            if not start_time and point_start_time:
                start_time = point_start_time
            if point_end_time:
                end_time = point_end_time

        return TimeBounds(start_time, end_time)

    def get_bounds(self) -> Optional[GPXBounds]:
        """
        Gets the latitude and longitude bounds of the track.

        Returns
        ----------
        bounds : Bounds named tuple
            min_latitude : float
                Minimum latitude of track in decimal degrees [-90, 90]
            max_latitude : float
                Maximum latitude of track in decimal degrees [-90, 90]
            min_longitude : float
                Minimum longitude of track in decimal degrees [-180, 180]
            max_longitude : float
                Maximum longitude of track in decimal degrees [-180, 180]
        """
        bounds: Optional[GPXBounds] = None
        for track_segment in self.segments:
            segment_bounds = track_segment.get_bounds()
            if bounds is None:
                bounds = segment_bounds
            elif segment_bounds:
                bounds = bounds.max_bounds(segment_bounds)

        return bounds

    def walk(self, only_points: bool=False) -> Iterator[Any]: #Union[GPXTrackPoint, Tuple[GPXTrackPoint, int, int]]]:
        """
        Generator used to iterates through track

        Parameters
        ----------
        only_point s: boolean
            Only yield points while walking

        Yields
        ----------
        point : GPXTrackPoint
            Point in the track
        segment_no : integer
            Index of segment containing point. This is suppressed if only_points
            is True.
        point_no : integer
            Index of point. This is suppressed if only_points is True.
        """
        for segment_no, segment in enumerate(self.segments if self.segments else []):
            for point_no, point in enumerate(segment.points if segment.points else []):
                if only_points:
                    yield point
                else:
                    yield point, segment_no, point_no

    def get_points_no(self) -> int:
        """
        Get the number of points in all segments in the track.

        Returns
        ----------
        num_points : integer
            Number of points in track
        """
        result = 0

        for track_segment in self.segments:
            result += track_segment.get_points_no()

        return result

    def length_3d(self) -> float:
        """
        Computes 3-dimensional length of track (latitude, longitude, and
        elevation). This is the sum of the 3D length of all segments.

        Returns
        ----------
        length : float
            Length returned in meters
        """
        length: float = 0
        for track_segment in self.segments:
            d = track_segment.length_3d()
            if d:
                length += d
        return length

    def split(self, track_segment_no: int, track_point_no: int) -> None:
        """
        Splits one of the segments in the track in two parts. If one of the
        split segments is empty it will not be added in the result. The
        segments will be split in place.

        Parameters
        ----------
        track_segment_no : integer
            The index of the segment to split
        track_point_no : integer
            The index of the track point in the segment to split
        """
        new_segments = []
        for i in range(len(self.segments)):
            segment = self.segments[i]
            if i == track_segment_no:
                segment_1, segment_2 = segment.split(track_point_no)
                if segment_1:
                    new_segments.append(segment_1)
                if segment_2:
                    new_segments.append(segment_2)
            else:
                new_segments.append(segment)
        self.segments = new_segments

    def join(self, track_segment_no: int, track_segment_no_2: Optional[int]=None) -> None:
        """
        Joins two segments of this track. The segments will be split in place.

        Parameters
        ----------
        track_segment_no : integer
            The index of the first segment to join
        track_segment_no_2 : integer
            The index of second segment to join. If track_segment_no_2 is not
            provided,the join will be with the next segment after
            track_segment_no.
        """
        if not track_segment_no_2:
            track_segment_no_2 = track_segment_no + 1

        if track_segment_no_2 >= len(self.segments):
            return

        new_segments = []
        for i in range(len(self.segments)):
            segment = self.segments[i]
            if i == track_segment_no:
                second_segment = self.segments[track_segment_no_2]
                segment.join(second_segment)

                new_segments.append(segment)
            elif i == track_segment_no_2:
                # Nothing, it is already joined
                pass
            else:
                new_segments.append(segment)
        self.segments = new_segments

    def get_moving_data(self, stopped_speed_threshold: Optional[float]=None, raw: bool=False, speed_extreemes_percentiles: float=IGNORE_TOP_SPEED_PERCENTILES, ignore_nonstandard_distances: bool = True) -> MovingData:
        """
        Return a tuple of (moving_time, stopped_time, moving_distance,
        stopped_distance, max_speed) that may be used for detecting the time
        stopped, and max speed. Not that those values are not absolutely true,
        because the "stopped" or "moving" information aren't saved in the track.

        Because of errors in the GPS recording, it may be good to calculate
        them on a reduced and smoothed version of the track.

        Parameters
        ----------
        stopped_speed_threshold : float
            speeds (km/h) below this threshold are treated as if having no
            movement. Default is 1 km/h.

        Returns
        ----------
        moving_data : MovingData : named tuple
            moving_time : float
                time (seconds) of track in which movement was occurring
            stopped_time : float
                time (seconds) of track in which no movement was occurring
            stopped_distance : float
                distance (meters) travelled during stopped times
            moving_distance : float
                distance (meters) travelled during moving times
            max_speed : float
                Maximum speed (m/s) during the track.
        """
        moving_time = 0.
        stopped_time = 0.

        moving_distance = 0.
        stopped_distance = 0.

        max_speed = 0.

        for segment in self.segments:
            moving_data = segment.get_moving_data(stopped_speed_threshold, raw, speed_extreemes_percentiles, ignore_nonstandard_distances)
            if moving_data:
                moving_time += moving_data.moving_time
                stopped_time += moving_data.stopped_time
                moving_distance += moving_data.moving_distance
                stopped_distance += moving_data.stopped_distance
                if moving_data.max_speed is not None and moving_data.max_speed > max_speed:
                    max_speed = moving_data.max_speed

        return MovingData(moving_time, stopped_time, moving_distance, stopped_distance, max_speed)

    def add_elevation(self, delta: float) -> None:
        """
        Adjusts elevation data for track.

        Parameters
        ----------
        delta : float
            Elevation delta in meters to apply to track
        """
        for track_segment in self.segments:
            track_segment.add_elevation(delta)

    def add_missing_data(self,
                         get_data_function: Callable[[GPXTrackPoint], Any],
                         add_missing_function: Callable[[List[GPXTrackPoint], GPXTrackPoint, GPXTrackPoint, List[float]], None]) -> None:
        for track_segment in self.segments:
            track_segment.add_missing_data(get_data_function, add_missing_function)

    def move(self, location_delta: mod_geo.LocationDelta) -> None:
        """
        Moves each point in the track.

        Parameters
        ----------
        location_delta: LocationDelta object
            Delta (distance/angle or lat/lon offset to apply each point in each
            segment of the track
        """
        for track_segment in self.segments:
            track_segment.move(location_delta)

    def get_duration(self) -> Optional[float]:
        """
        Calculates duration or track

        Returns
        -------
        duration: float
            Duration in seconds or None if any time data is missing
        """
        if not self.segments:
            return 0.0

        result = 0.
        for track_segment in self.segments:
            duration = track_segment.get_duration()
            if duration or duration == 0:
                result += duration
            elif duration is None:
                return None

        return result

    def get_uphill_downhill(self) -> UphillDownhill:
        """
        Calculates the uphill and downhill elevation climbs for the track.
        If elevation for some points is not found those are simply ignored.

        Returns
        -------
        uphill_downhill: UphillDownhill named tuple
            uphill: float
                Uphill elevation climbs in meters
            downhill: float
                Downhill elevation descent in meters
        """
        if not self.segments:
            return UphillDownhill(0, 0)

        uphill: float = 0
        downhill: float = 0

        for track_segment in self.segments:
            current_uphill, current_downhill = track_segment.get_uphill_downhill()

            uphill += current_uphill or .0
            downhill += current_downhill or .0

        return UphillDownhill(uphill, downhill)

    def get_location_at(self, time: mod_datetime.datetime) -> List[GPXTrackPoint]:
        """
        Gets approx. location at given time. Note that, at the moment this
        method returns an instance of GPXTrackPoint in the future -- this may
        be a mod_geo.Location instance with approximated latitude, longitude
        and elevation!
        """
        result = []
        for track_segment in self.segments:
            location = track_segment.get_location_at(time)
            if location:
                result.append(location)

        return result

    def get_elevation_extremes(self) -> MinimumMaximum:
        """
        Calculate elevation extremes of track

        Returns
        -------
        min_max_elevation: MinimumMaximum named tuple
            minimum: float
                Minimum elevation in meters
            maximum: float
                Maximum elevation in meters
        """
        if not self.segments:
            return MinimumMaximum(None, None)

        elevations = []

        for track_segment in self.segments:
            (_min, _max) = track_segment.get_elevation_extremes()
            if _min is not None:
                elevations.append(_min)
            if _max is not None:
                elevations.append(_max)

        if len(elevations) == 0:
            return MinimumMaximum(None, None)

        return MinimumMaximum(min(elevations), max(elevations))

    def get_center(self) -> Optional[mod_geo.Location]:
        """
        Get the center of the route.

        Returns
        -------
        center: Location
            latitude: latitude of center in degrees
            longitude: longitude of center in degrees
            elevation: not calculated here
        """
        if not self.segments:
            return None
        sum_lat: float = 0
        sum_lon: float = 0
        n = 0
        for track_segment in self.segments:
            for point in track_segment.points:
                n += 1
                sum_lat += point.latitude
                sum_lon += point.longitude

        if not n:
            return mod_geo.Location(0, 0)

        return mod_geo.Location(latitude=sum_lat / n, longitude=sum_lon / n)

    def smooth(self, vertical: bool=True, horizontal: bool=False, remove_extremes: bool=False) -> None:
        """ See: GPXTrackSegment.smooth() """
        for track_segment in self.segments:
            track_segment.smooth(vertical, horizontal, remove_extremes)

    def has_times(self) -> bool:
        """ See GPXTrackSegment.has_times() """
        if not self.segments:
            return False

        for track_segment in self.segments:
            if track_segment.has_times():
                return True

        return False

    def has_elevations(self) -> bool:
        """ Returns true if track data has elevation for all segments """
        if not self.segments:
            return False

        result = True
        for track_segment in self.segments:
            result = result and track_segment.has_elevations()

        return result

    def get_nearest_location(self, location: mod_geo.Location) -> Optional[NearestLocationData]:
        """ Returns (location, track_segment_no, track_point_no) for nearest location on track """
        return min((NearestLocationData(pt, -1, seg, pt_no) for (pt, seg, pt_no) in self.walk()) # type: ignore
                   ,key=lambda x: x.location.distance_2d(location) if x is not None else mod_math.inf
                   ,default=None)
        
    def clone(self) -> "GPXTrack":
        return mod_copy.deepcopy(self)


    def __repr__(self) -> str:
        parts = []
        for attribute in 'name', 'description', 'number':
            value = getattr(self, attribute)
            if value is not None:
                parts.append(f'{attribute}={value!r}')
        parts.append(f'segments={self.segments!r}')
        return f'GPXTrack({", ".join(parts)})'


class GPX:
    gpx_10_fields = [
            mod_gpxfield.GPXField('version', attribute='version'),
            mod_gpxfield.GPXField('creator', attribute='creator'),
            mod_gpxfield.GPXField('name'),
            mod_gpxfield.GPXField('description', 'desc'),
            mod_gpxfield.GPXField('author_name', 'author'),
            mod_gpxfield.GPXField('author_email', 'email'),
            mod_gpxfield.GPXField('link', 'url'),
            mod_gpxfield.GPXField('link_text', 'urlname'),
            mod_gpxfield.GPXField('time', type=mod_gpxfield.TIME_TYPE),
            mod_gpxfield.GPXField('keywords'),
            mod_gpxfield.GPXComplexField('bounds', classs=GPXBounds, empty_body=True),
            mod_gpxfield.GPXComplexField('waypoints', classs=GPXWaypoint, tag='wpt', is_list=True),
            mod_gpxfield.GPXComplexField('routes', classs=GPXRoute, tag='rte', is_list=True),
            mod_gpxfield.GPXComplexField('tracks', classs=GPXTrack, tag='trk', is_list=True),
    ]
    # Text fields serialize as empty container tags, dependents are
    # are listed after as 'tag:dep1:dep2:dep3'. If no dependents are
    # listed, it will always serialize. The container is closed with
    # '/tag'. Required dependents are preceded by an @. If a required
    # dependent is empty, nothing in the container will serialize. The
    # format is 'tag:@dep2'. No optional dependents need to be listed.
    # Extensions not yet supported
    gpx_11_fields = [
            mod_gpxfield.GPXField('version', attribute='version'),
            mod_gpxfield.GPXField('creator', attribute='creator'),
            'metadata:name:description:author_name:author_email:author_link:copyright_author:copyright_year:copyright_license:link:time:keywords:bounds',
                mod_gpxfield.GPXField('name', 'name'),
                mod_gpxfield.GPXField('description', 'desc'),
                'author:author_name:author_email:author_link',
                    mod_gpxfield.GPXField('author_name', 'name'),
                    mod_gpxfield.GPXEmailField('author_email', 'email'),
                    'link:@author_link',
                        mod_gpxfield.GPXField('author_link', attribute='href'),
                        mod_gpxfield.GPXField('author_link_text', tag='text'),
                        mod_gpxfield.GPXField('author_link_type', tag='type'),
                    '/link',
                '/author',
                'copyright:copyright_author:copyright_year:copyright_license',
                    mod_gpxfield.GPXField('copyright_author', attribute='author'),
                    mod_gpxfield.GPXField('copyright_year', tag='year'),
                    mod_gpxfield.GPXField('copyright_license', tag='license'),
                '/copyright',
                'link:@link',
                    mod_gpxfield.GPXField('link', attribute='href'),
                    mod_gpxfield.GPXField('link_text', tag='text'),
                    mod_gpxfield.GPXField('link_type', tag='type'),
                '/link',
                mod_gpxfield.GPXField('time', type=mod_gpxfield.TIME_TYPE),
                mod_gpxfield.GPXField('keywords'),
                mod_gpxfield.GPXComplexField('bounds', classs=GPXBounds, empty_body=True),
                mod_gpxfield.GPXExtensionsField('metadata_extensions', tag='extensions'),
            '/metadata',
            mod_gpxfield.GPXComplexField('waypoints', classs=GPXWaypoint, tag='wpt', is_list=True),
            mod_gpxfield.GPXComplexField('routes', classs=GPXRoute, tag='rte', is_list=True),
            mod_gpxfield.GPXComplexField('tracks', classs=GPXTrack, tag='trk', is_list=True),
            mod_gpxfield.GPXExtensionsField('extensions', is_list=True),
    ]

    __slots__ = ('version', 'creator', 'name', 'description', 'author_name',
                 'author_email', 'link', 'link_text', 'time', 'keywords',
                 'bounds', 'waypoints', 'routes', 'tracks', 'author_link',
                 'author_link_text', 'author_link_type', 'copyright_author',
                 'copyright_year', 'copyright_license', 'link_type',
                 'metadata_extensions', 'extensions', 'nsmap',
                 'schema_locations')

    def __init__(self) -> None:
        self.version: Optional[str] = None
        self.creator: Optional[str] = None
        self.name: Optional[str] = None
        self.description: Optional[str] = None
        self.link: Optional[str] = None
        self.link_text: Optional[str] = None
        self.link_type: Optional[str] = None
        self.time: Optional[mod_datetime.datetime] = None
        self.keywords: Optional[str] = None
        self.bounds: Optional[GPXBounds] = None
        self.author_name: Optional[str] = None
        self.author_email: Optional[str] = None
        self.author_link: Optional[str] = None
        self.author_link_text: Optional[str] = None
        self.author_link_type: Optional[str] = None
        self.copyright_author: Optional[str] = None
        self.copyright_year: Optional[str] = None
        self.copyright_license: Optional[str] = None
        self.metadata_extensions: List[Any] = [] # TODO
        self.extensions: List[Any] = [] # TODO
        self.waypoints: List[GPXWaypoint] = []
        self.routes: List[GPXRoute] = []
        self.tracks: List[GPXTrack] = []
        self.nsmap: Dict[str, str] = {}
        self.schema_locations: List[str] = []

    def simplify(self, max_distance: Optional[float]=None) -> None:
        """
        Simplify using the Ramer-Douglas-Peucker algorithm: http://en.wikipedia.org/wiki/Ramer-Douglas-Peucker_algorithm
        """
        for track in self.tracks:
            track.simplify(max_distance=max_distance)

    def reduce_points(self, max_points_no: Optional[float]=None, min_distance: Optional[float]=None) -> None:
        """
        Reduces the number of points. Points will be updated in place.

        Parameters
        ----------

        max_points : int
            The maximum number of points to include in the GPX
        min_distance : float
            The minimum separation in meters between points
        """
        if max_points_no is None and min_distance is None:
            raise ValueError("Either max_point_no or min_distance must be supplied")

        if max_points_no is not None and max_points_no < 2:
            raise ValueError("max_points_no must be greater than or equal to 2")

        points_no = len(list(self.walk()))
        if max_points_no is not None and points_no <= max_points_no:
            # No need to reduce points only if no min_distance is specified:
            if not min_distance:
                return

        length = self.length_3d()

        min_distance = min_distance or 0
        max_points_no = max_points_no or 1000000000

        min_distance = max(min_distance, mod_math.ceil(length / max_points_no))

        for track in self.tracks:
            track.reduce_points(min_distance)

        # TODO
        log.debug('Track reduced to %s points', self.get_track_points_no())

    def adjust_time(self, delta: mod_datetime.timedelta, all: bool=False) -> None:
        """
        Adjusts the time of all points in all of the segments of all tracks by
        the specified delta.

        If all=True, waypoints and routes will also be adjusted by the specified delta.

        Parameters
        ----------
        delta : datetime.timedelta
            Positive time delta will adjust times into the future
            Negative time delta will adjust times into the past
        all : bool
            When true, also adjusts time for waypoints and routes.
        """
        if self.time:
            self.time += delta
        for track in self.tracks:
            track.adjust_time(delta)

        if all:
            for waypoint in self.waypoints:
                waypoint.adjust_time(delta)
            for route in self.routes:
                route.adjust_time(delta)

    def remove_time(self, all: bool=False) -> None:
        """
        Removes time data of all points in all of the segments of all tracks.

        If all=True, time date will also be removed from waypoints and routes.

        Parameters
        ----------
        all : bool
            When true, also removes time data for waypoints and routes.
        """
        for track in self.tracks:
            track.remove_time()

        if all:
            for waypoint in self.waypoints:
                waypoint.remove_time()
            for route in self.routes:
                route.remove_time()

    def remove_elevation(self, tracks: bool=True, routes: bool=False, waypoints: bool=False) -> None:
        """ Removes elevation data. """
        if tracks:
            for track in self.tracks:
                track.remove_elevation()
        if routes:
            for route in self.routes:
                route.remove_elevation()
        if waypoints:
            for waypoint in self.waypoints:
                waypoint.remove_elevation()

    def get_time_bounds(self) -> TimeBounds:
        """
        Gets the time bounds (start and end) of the GPX file.

        Returns
        ----------
        time_bounds : TimeBounds named tuple
            start_time : datetime
                Start time of the first segment in track
            end time : datetime
                End time of the last segment in track
        """
        start_time = None
        end_time = None

        for track in self.tracks:
            track_start_time, track_end_time = track.get_time_bounds()
            if not start_time:
                start_time = track_start_time
            if track_end_time:
                end_time = track_end_time

        return TimeBounds(start_time, end_time)

    def get_bounds(self) -> Optional[GPXBounds]:
        """
        Gets the latitude and longitude bounds of the GPX file.

        Returns
        ----------
        bounds : Bounds named tuple
            min_latitude : float
                Minimum latitude of track in decimal degrees [-90, 90]
            max_latitude : float
                Maximum latitude of track in decimal degrees [-90, 90]
            min_longitude : float
                Minimum longitude of track in decimal degrees [-180, 180]
            max_longitude : float
                Maximum longitude of track in decimal degrees [-180, 180]
        """
        result: Optional[GPXBounds] = None 
        for track in self.tracks:
            track_bounds = track.get_bounds()
            if not result:
                result = track_bounds
            elif track_bounds:
                result = track_bounds.max_bounds(result)
        return result

    def get_points_no(self) -> int:
        """
        Get the number of points in all segments of all track.

        Returns
        ----------
        num_points : integer
            Number of points in GPX
        """
        result = 0
        for track in self.tracks:
            result += track.get_points_no()
        return result

    def refresh_bounds(self) -> None:
        """
        Compute bounds and reload min_latitude, max_latitude, min_longitude
        and max_longitude properties of this object
        """
        self.bounds = self.get_bounds()

    def smooth(self, vertical: bool=True, horizontal: bool=False, remove_extremes: bool=False) -> None:
        """ See GPXTrackSegment.smooth(...) """
        for track in self.tracks:
            track.smooth(vertical=vertical, horizontal=horizontal, remove_extremes=remove_extremes)

    def remove_empty(self) -> None:
        """ Removes segments, routes """

        routes = []

        for route in self.routes:
            if len(route.points) > 0:
                routes.append(route)

        self.routes = routes

        for track in self.tracks:
            track.remove_empty()

    def get_moving_data(self, stopped_speed_threshold: Optional[float]=None, raw: bool=False, speed_extreemes_percentiles: float=IGNORE_TOP_SPEED_PERCENTILES, ignore_nonstandard_distances: bool=True) -> MovingData:
        """
        Return a tuple of (moving_time, stopped_time, moving_distance, stopped_distance, max_speed)
        that may be used for detecting the time stopped, and max speed. Not that those values are not
        absolutely true, because the "stopped" or "moving" information aren't saved in the track.

        Because of errors in the GPS recording, it may be good to calculate them on a reduced and
        smoothed version of the track. Something like this:

        cloned_gpx = gpx.clone()
        cloned_gpx.reduce_points(2000, min_distance=10)
        cloned_gpx.smooth(vertical=True, horizontal=True)
        cloned_gpx.smooth(vertical=True, horizontal=False)
        moving_time, stopped_time, moving_distance, stopped_distance, max_speed_ms = cloned_gpx.get_moving_data
        max_speed_kmh = max_speed_ms * 60. ** 2 / 1000.

        Experiment with your own variations to get the values you expect.

        Max speed is in m/s.
        """
        moving_time = 0.
        stopped_time = 0.

        moving_distance = 0.
        stopped_distance = 0.

        max_speed = 0.

        for track in self.tracks:
            track_moving_time, track_stopped_time, track_moving_distance, track_stopped_distance, track_max_speed = track.get_moving_data(stopped_speed_threshold, raw=raw, speed_extreemes_percentiles=speed_extreemes_percentiles, ignore_nonstandard_distances=ignore_nonstandard_distances)
            moving_time += track_moving_time
            stopped_time += track_stopped_time
            moving_distance += track_moving_distance
            stopped_distance += track_stopped_distance

            if track_max_speed > max_speed:
                max_speed = track_max_speed

        return MovingData(moving_time, stopped_time, moving_distance, stopped_distance, max_speed)

    def split(self, track_no: int, track_segment_no: int, track_point_no: int) -> None:
        """
        Splits one of the segments of a track in two parts. If one of the
        split segments is empty it will not be added in the result. The
        segments will be split in place.

        Parameters
        ----------
        track_no : integer
            The index of the track to split
        track_segment_no : integer
            The index of the segment to split
        track_point_no : integer
            The index of the track point in the segment to split
        """
        track = self.tracks[track_no]

        track.split(track_segment_no=track_segment_no, track_point_no=track_point_no)

    def length_2d(self) -> float:
        """
        Computes 2-dimensional length of the GPX file (only latitude and
        longitude, no elevation). This is the sum of 2D length of all segments
        in all tracks.

        Returns
        ----------
        length : float
            Length returned in meters
        """
        result: float = 0
        for track in self.tracks:
            length = track.length_2d()
            if length:
                result += length
        return result

    def length_3d(self) -> float:
        """
        Computes 3-dimensional length of the GPX file (latitude, longitude, and
        elevation). This is the sum of 3D length of all segments in all tracks.

        Returns
        ----------
        length : float
            Length returned in meters
        """
        result: float = 0
        for track in self.tracks:
            length = track.length_3d()
            if length:
                result += length
        return result

    def walk(self, only_points: bool=False) -> Iterator[Any]:
        """
        Generator used to iterates through points in GPX file

        Parameters
        ----------
        only_point s: boolean
            Only yield points while walking

        Yields
        ----------
        point : GPXTrackPoint
            Point in the track
        track_no : integer
            Index of track containing point. This is suppressed if only_points
            is True.
        segment_no : integer
            Index of segment containing point. This is suppressed if only_points
            is True.
        point_no : integer
            Index of point. This is suppressed if only_points is True.
        """
        for track_no, track in enumerate(self.tracks if self.tracks else [] ):
            for segment_no, segment in enumerate(track.segments if track.segments else []):
                for point_no, point in enumerate(segment.points if segment.points else []):
                    if only_points:
                        yield point
                    else:
                        yield point, track_no, segment_no, point_no

    def get_track_points_no(self) -> int:
        """ Number of track points, *without* route and waypoints """
        result = 0

        for track in self.tracks:
            for segment in track.segments:
                result += len(segment.points)

        return result

    def get_duration(self) -> Optional[float]:
        """
        Calculates duration of GPX file

        Returns
        -------
        duration: float
            Duration in seconds or None if time data is not fully populated.
        """
        if not self.tracks:
            return 0.0

        result: float = 0.0
        for track in self.tracks:
            duration = track.get_duration()
            if duration or duration == 0:
                result += duration
            elif duration is None:
                return None

        return result

    def get_uphill_downhill(self) -> UphillDownhill:
        """
        Calculates the uphill and downhill elevation climbs for the gpx file.
        If elevation for some points is not found those are simply ignored.

        Returns
        -------
        uphill_downhill: UphillDownhill named tuple
            uphill: float
                Uphill elevation climbs in meters
            downhill: float
                Downhill elevation descent in meters
        """
        if not self.tracks:
            return UphillDownhill(0, 0)

        uphill: float = 0
        downhill: float = 0

        for track in self.tracks:
            current_uphill, current_downhill = track.get_uphill_downhill()

            uphill += current_uphill or 0
            downhill += current_downhill or 0

        return UphillDownhill(uphill, downhill)

    def get_location_at(self, time: mod_datetime.datetime) -> List[mod_geo.Location]:
        """
        Gets approx. location at given time. Note that, at the moment this
        method returns an instance of GPXTrackPoint in the future -- this may
        be a mod_geo.Location instance with approximated latitude, longitude
        and elevation!
        """
        result: List[mod_geo.Location] = []
        for track in self.tracks:
            locations = track.get_location_at(time)
            for location in locations:
                result.append(location)

        return result

    def get_elevation_extremes(self) -> MinimumMaximum:
        """
        Calculate elevation extremes of GPX file

        Returns
        -------
        min_max_elevation: MinimumMaximum named tuple
            minimum: float
                Minimum elevation in meters
            maximum: float
                Maximum elevation in meters
        """
        if not self.tracks:
            return MinimumMaximum(None, None)

        elevations = []

        for track in self.tracks:
            (_min, _max) = track.get_elevation_extremes()
            if _min is not None:
                elevations.append(_min)
            if _max is not None:
                elevations.append(_max)

        if len(elevations) == 0:
            return MinimumMaximum(None, None)

        return MinimumMaximum(min(elevations), max(elevations))

    def get_points_data(self, distance_2d: bool=False) -> List[PointData]:
        """
        Returns a list of tuples containing the actual point, its distance from the start,
        track_no, segment_no, and segment_point_no
        """
        distance_from_start = 0
        previous_point = None

        # (point, distance_from_start) pairs:
        points = []

        for track_no, track in enumerate(self.tracks):
            for segment_no, segment in enumerate(track.segments):
                for point_no, point in enumerate(segment.points):
                    if previous_point and point_no > 0:
                        if distance_2d:
                            distance = point.distance_2d(previous_point)
                        else:
                            distance = point.distance_3d(previous_point)

                        distance_from_start += distance

                    points.append(PointData(point, distance_from_start, track_no, segment_no, point_no))

                    previous_point = point

        return points

    def get_nearest_locations(self, location: mod_geo.Location, threshold_distance: float=0.01) -> List[NearestLocationData]:
        """
        Returns a list of locations of elements like
        consisting of points where the location may be on the track

        threshold_distance is the minimum distance from the track
        so that the point *may* be counted as to be "on the track".
        For example 0.01 means 1% of the track distance.
        """

        assert location
        assert threshold_distance

        result: List[NearestLocationData] = []

        points = self.get_points_data()

        if not points:
            return result

        distance: Optional[float] = points[- 1][1]

        threshold = (distance or 0.0) * threshold_distance

        min_distance_candidate = None
        distance_from_start_candidate = None
        track_no_candidate: Optional[int] = None
        segment_no_candidate: Optional[int] = None
        point_no_candidate: Optional[int] = None

        for point, distance_from_start, track_no, segment_no, point_no in points:
            distance = location.distance_3d(point)
            if (distance or 0.0) < threshold:
                if min_distance_candidate is None or distance < min_distance_candidate:
                    min_distance_candidate = distance
                    distance_from_start_candidate = distance_from_start
                    track_no_candidate = track_no
                    segment_no_candidate = segment_no
                    point_no_candidate = point_no
            else:
                if distance_from_start_candidate is not None and point and track_no_candidate is not None and segment_no_candidate is not None and point_no_candidate is not None:
                    result.append(NearestLocationData(point, track_no_candidate, segment_no_candidate, point_no_candidate))
                min_distance_candidate = None
                distance_from_start_candidate = None
                track_no_candidate = None
                segment_no_candidate = None
                point_no_candidate = None

        if distance_from_start_candidate is not None and point and track_no_candidate is not None and segment_no_candidate is not None and point_no_candidate is not None:
            result.append(NearestLocationData(point, track_no_candidate, segment_no_candidate, point_no_candidate))

        return result


    def get_nearest_location(self, location: mod_geo.Location) -> Optional[NearestLocationData]:
        """ Returns (location, track_no, track_segment_no, track_point_no) for the
        nearest location on map """
        return min((NearestLocationData(pt, tr, seg, pt_no) for (pt,tr, seg, pt_no) in self.walk()) # type:ignore
                   ,key=lambda x: x.location.distance_2d(location) if x is not None else mod_math.inf
                   ,default=None)

    def add_elevation(self, delta: float) -> None:
        """
        Adjusts elevation data of GPX data.

        Parameters
        ----------
        delta : float
            Elevation delta in meters to apply to GPX data
        """
        for track in self.tracks:
            track.add_elevation(delta)

    def add_missing_data(self,
                         get_data_function: Callable[[GPXTrackPoint], Any],
                         add_missing_function: Callable[[List[GPXTrackPoint], GPXTrackPoint, GPXTrackPoint, List[float]], None]) -> None:
        for track in self.tracks:
            track.add_missing_data(get_data_function, add_missing_function)

    def add_missing_elevations(self) -> None:
        def _add(interval: List[GPXTrackPoint], start: GPXTrackPoint, end: GPXTrackPoint, distances_ratios: List[float]) -> None:
            if (start.elevation is None) or (end.elevation is None):
                return
            assert start
            assert end
            assert interval
            assert len(interval) == len(distances_ratios)
            for point, ratio in zip(interval, distances_ratios):
                point.elevation = start.elevation + ratio * (end.elevation - start.elevation)

        self.add_missing_data(get_data_function=lambda point: point.elevation,
                              add_missing_function=_add)

    def add_missing_times(self) -> None:
        def _add(interval: List[GPXTrackPoint], start: GPXTrackPoint, end: GPXTrackPoint, distances_ratios: List[float]) -> None:
            if (not start) or (not end) or (not start.time) or (not end.time):
                return
            assert interval
            assert len(interval) == len(distances_ratios)

            if end.time and start.time:
                seconds_between = float(mod_utils.total_seconds(end.time - start.time))
                for point, ratio in zip(interval, distances_ratios):
                    point.time = start.time + mod_datetime.timedelta(seconds=ratio * seconds_between)

        self.add_missing_data(get_data_function=lambda point: point.time,
                              add_missing_function=_add)

    def add_missing_speeds(self) -> None:
        """
        The missing speeds are added to a segment.

        The weighted harmonic mean is used to approximate the speed at
        a :obj:'~.GPXTrackPoint'.
        For this to work the speed of the first and last track point in a
        segment needs to be known.
        """
        def _add(interval: List[GPXTrackPoint], start: GPXTrackPoint, end: GPXTrackPoint, distances_ratios: List[float]) -> None:
            if (not start) or (not end) or (not start.time) or (not end.time):
                return
            assert interval
            assert len(interval) == len(distances_ratios)

            time_dist_before = (interval[0].time_difference(start),
                                interval[0].distance_3d(start))
            time_dist_after = (interval[-1].time_difference(end),
                               interval[-1].distance_3d(end))

            # Assemble list of times and distance to neighbour points
            times_dists: List[Tuple[Optional[float], Optional[float]]] = [
                (point.time_difference(nextpoint), point.distance_3d(nextpoint))
                for point, nextpoint in zip(interval, interval[1:])
            ]
            times_dists.insert(0, time_dist_before)
            times_dists.append(time_dist_after)

            for i, point in enumerate(interval):
                time_left, dist_left = times_dists[i]
                time_right, dist_right = times_dists[i+1]
                if time_left and time_right and dist_left and dist_right:
                    point.speed = (dist_left + dist_right) / (time_left + time_right)

        self.add_missing_data(get_data_function=lambda point: point.speed,
                              add_missing_function=_add)

    def fill_time_data_with_regular_intervals(self, start_time: Optional[mod_datetime.datetime]=None, time_delta: Optional[mod_datetime.timedelta]=None, end_time: Optional[mod_datetime.datetime]=None, force: bool=True) -> None:
        """
        Fills the time data for all points in the GPX file. At least two of the parameters start_time, time_delta, and
        end_time have to be provided. If the three are provided, time_delta will be ignored and will be recalculated
        using start_time and end_time.

        The first GPX point will have a time equal to start_time. Then points are assumed to be recorded at regular
        intervals time_delta.

        If the GPX file currently contains time data, it will be overwritten, unless the force flag is set to False, in
        which case the function will return a GPXException error.

        Parameters
        ----------
        start_time: datetime.datetime object
            Start time of the GPX file (corresponds to the time of the first point)
        time_delta: datetime.timedelta object
            Time interval between two points in the GPX file
        end_time: datetime.datetime object
            End time of the GPX file (corresponds to the time of the last point)
        force: bool
            Overwrite current data if the GPX file currently contains time data
        """
        if not (start_time and end_time) and not (start_time and time_delta) and not (time_delta and end_time):
            raise GPXException('You must provide at least two parameters among start_time, time_step, and end_time')

        if self.has_times() and not force:
            raise GPXException('GPX file currently contains time data. Use force=True to overwrite.')

        point_no = self.get_points_no()

        if start_time and end_time:
            if start_time > end_time:
                raise GPXException('Invalid parameters: end_time must occur after start_time')
            time_delta = (end_time - start_time) / (point_no - 1)
        elif end_time and not start_time and time_delta:
            start_time = end_time - (point_no - 1) * time_delta

        if start_time:
            self.time = start_time

        i = 0
        for point in self.walk(only_points=True):
            if i == 0:
                point.time = start_time
            elif time_delta is not None and start_time is not None:
                point.time = start_time + i * time_delta
            i += 1

    def move(self, location_delta: mod_geo.LocationDelta) -> None:
        """
        Moves each point in the gpx file (routes, waypoints, tracks).

        Parameters
        ----------
        location_delta: LocationDelta
            LocationDelta to move each point
        """
        for route in self.routes:
            route.move(location_delta)

        for waypoint in self.waypoints:
            waypoint.move(location_delta)

        for track in self.tracks:
            track.move(location_delta)

    def to_xml(self, version: Optional[str]=None, prettyprint: bool=True) -> str:
        """
        FIXME: Note, this method will change self.version
        """
        if not version:
            if self.version:
                version = self.version
            else:
                version = '1.1'

        if version != '1.0' and version != '1.1':
            raise GPXException(f'Invalid version {version}')

        self.version = version
        if not self.creator:
            self.creator = 'gpx.py -- https://github.com/tkrajina/gpxpy'

        self.nsmap['xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'

        version_path = version.replace('.', '/')

        self.nsmap['defaultns'] = f'http://www.topografix.com/GPX/{version_path}'

        if not self.schema_locations:
            self.schema_locations = [
                f'http://www.topografix.com/GPX/{version_path}',
                f'http://www.topografix.com/GPX/{version_path}/gpx.xsd',
            ]

        content = mod_gpxfield.gpx_fields_to_xml(
            self, 'gpx', version,
            custom_attributes={
                'xsi:schemaLocation': ' '.join(self.schema_locations)
            },
            nsmap=self.nsmap,
            prettyprint=prettyprint
        )

        return f'<?xml version="1.0" encoding="UTF-8"?>\n{content.strip()}'

    def has_times(self) -> bool:
        """ See GPXTrackSegment.has_times() """
        if not self.tracks:
            return False

        result = True
        for track in self.tracks:
            result = result and track.has_times()

        return result

    def has_elevations(self) -> bool:
        """ See GPXTrackSegment.has_elevations()) """
        if not self.tracks:
            return False

        result = True
        for track in self.tracks:
            result = result and track.has_elevations()

        return result

    def __repr__(self) -> str:
        parts = []
        for attribute in 'waypoints', 'routes', 'tracks':
            value = getattr(self, attribute)
            if value:
                parts.append(f'{attribute}={value!r}')
        return f'GPX({", ".join(parts)})'

    def clone(self) -> "GPX":
        return mod_copy.deepcopy(self)

# Add attributes and fill default values (lists or None) for all GPX elements:
for var_name in dir():
    var_value = vars()[var_name]
    if hasattr(var_value, 'gpx_10_fields') or hasattr(var_value, 'gpx_11_fields'):
        #print('Check/fill %s' % var_value)
        mod_gpxfield.gpx_check_slots_and_default_values(var_value)
