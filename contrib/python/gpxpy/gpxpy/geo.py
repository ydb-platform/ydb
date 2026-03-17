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

import logging as mod_logging
import math as mod_math

from . import utils as mod_utils

from typing import *

log = mod_logging.getLogger(__name__)

# Generic geo related function and class(es)

# latitude/longitude in GPX files is always in WGS84 datum
# WGS84 defined the Earth semi-major axis with 6378.137 km
EARTH_RADIUS = 6378.137 * 1000

# One degree in meters:
ONE_DEGREE = (2*mod_math.pi*EARTH_RADIUS) / 360  # ==> 111.319 km


def haversine_distance(latitude_1: float, longitude_1: float, latitude_2: float, longitude_2: float) -> float:
    """
    Haversine distance between two points, expressed in meters.

    Implemented from http://www.movable-type.co.uk/scripts/latlong.html
    """
    d_lon = mod_math.radians(longitude_1 - longitude_2)
    lat1 = mod_math.radians(latitude_1)
    lat2 = mod_math.radians(latitude_2)
    d_lat = lat1 - lat2

    a = mod_math.pow(mod_math.sin(d_lat/2),2) + \
        mod_math.pow(mod_math.sin(d_lon/2),2) * mod_math.cos(lat1) * mod_math.cos(lat2)
    c = 2 * mod_math.asin(mod_math.sqrt(a))
    d = EARTH_RADIUS * c

    return d


def get_course(latitude_1: float, longitude_1: float, latitude_2: float, longitude_2: float,
               loxodromic: bool=True) -> float:
    """
    The initial course from one point to another,
    expressed in decimal degrees clockwise from true North
    (not magnetic)
    (0.0 <= value < 360.0)

    Use the default loxodromic model in most cases
    (except when visualizing the long routes of maritime transport and aeroplanes)

    Implemented from http://www.movable-type.co.uk/scripts/latlong.html
    (sections 'Bearing' and 'Rhumb lines')
    """

    d_lon = mod_math.radians(longitude_2 - longitude_1)
    lat1 = mod_math.radians(latitude_1)
    lat2 = mod_math.radians(latitude_2)

    if not loxodromic:
        y = mod_math.sin(d_lon) * mod_math.cos(lat2)
        x = mod_math.cos(lat1) * mod_math.sin(lat2) - \
            mod_math.sin(lat1) * mod_math.cos(lat2) * mod_math.cos(d_lon)
    else:
        radian_circle = 2*mod_math.pi

        if abs(d_lon) > mod_math.pi:
            if d_lon > 0:
                d_lon = - (radian_circle - d_lon)
            else:
                d_lon = radian_circle + d_lon

        y = d_lon

        delta = mod_math.pi/4
        x = mod_math.log(mod_math.tan(delta + 0.5*lat2)
                         / mod_math.tan(delta + 0.5*lat1))

    course = mod_math.degrees(mod_math.atan2(y, x))
    return course % 360


def length(locations: List["Location"]=[], _3d: bool=False) -> float:
    if not locations:
        return 0
    length: float = 0
    for previous_location, location in zip(locations, locations[1:]):
        if _3d:
            d = location.distance_3d(previous_location)
        else:
            d = location.distance_2d(previous_location)
        if d:
            length += d
    return length


def length_2d(locations: List["Location"]=[]) -> float:
    """ 2-dimensional length (meters) of locations (only latitude and longitude, no elevation). """
    return length(locations, False)


def length_3d(locations: List["Location"]=[]) -> float:
    """ 3-dimensional length (meters) of locations (it uses latitude, longitude, and elevation). """
    return length(locations, True)


def calculate_max_speed(speeds_and_distances: List[Tuple[float, float]], extreemes_percentile: float, ignore_nonstandard_distances: bool) -> Optional[float]:
    """
    Compute average distance and standard deviation for distance. Extremes
    in distances are usually extremes in speeds, so we will ignore them,
    here.

    speeds_and_distances must be a list containing pairs of (speed, distance)
    for every point in a track segment.

    In many cases the top speeds are measurement errors. For that reason extreme speeds can be removed
    with the extreemes_percentile (for example, a value of 0.05 will remove top 5%).
    """
    assert speeds_and_distances
    if len(speeds_and_distances) > 0:
        assert len(speeds_and_distances[0]) == 2
        # ...
        assert len(speeds_and_distances[-1]) == 2

    if not ignore_nonstandard_distances:
        return max(x[0] or 0 for x in speeds_and_distances)

    size = len(speeds_and_distances)

    if size < 2:
        # log.debug('Segment too small to compute speed, size=%s', size)
        return None

    distances = [x[1] for x in speeds_and_distances]
    average_distance = sum(distances) / size
    standard_distance_deviation = mod_math.sqrt(sum((distance - average_distance) ** 2 for distance in distances) / size)

    # Ignore items where the distance is too big:
    filtered_speeds_and_distances = [x for x in speeds_and_distances if abs(x[1] - average_distance) <= standard_distance_deviation * 1.5]

    # sort by speed:
    speeds = [x[0] for x in filtered_speeds_and_distances]
    if not speeds:
        return None
    speeds.sort()

    # Even here there may be some extremes => ignore the last 5%:
    index = int(len(speeds) * (1-extreemes_percentile))
    if index >= len(speeds):
        index = -1

    return speeds[index]


def calculate_uphill_downhill(elevations: List[Optional[float]]) -> Tuple[float, float]:
    if not elevations:
        return 0, 0

    elevations = list(filter(lambda e: e is not None, elevations))
    size = len(elevations)
    def __filter(n: int) -> float:
        current_ele = elevations[n]
        if current_ele is None:
            return False
        if 0 < n < size - 1:
            previous_ele = elevations[n-1]
            next_ele = elevations[n+1]
            if previous_ele is not None and current_ele is not None and next_ele is not None:
                return previous_ele*.3 + current_ele*.4 + next_ele*.3
        return current_ele

    smoothed_elevations = list(map(__filter, range(size)))

    uphill, downhill = 0., 0.
    for prev, cur in zip(smoothed_elevations, smoothed_elevations[1:]):
        if prev is not None and cur is not None:
            d = cur - prev
            if d > 0:
                uphill += d
            else:
                downhill -= d
    return uphill, downhill


def distance(latitude_1: float, longitude_1: float, elevation_1: Optional[float],
            latitude_2: float, longitude_2: float, elevation_2: Optional[float],
            haversine: bool=False) -> float:
    """
    Distance between two points. If elevation is None compute a 2d distance

    if haversine==True -- haversine will be used for every computations,
    otherwise...

    Haversine distance will be used for distant points where elevation makes a
    small difference, so it is ignored. That's because haversine is 5-6 times
    slower than the dummy distance algorithm (which is OK for most GPS tracks).
    """

    # If points too distant -- compute haversine distance:
    if haversine or (abs(latitude_1 - latitude_2) > .2 or abs(longitude_1 - longitude_2) > .2):
        return haversine_distance(latitude_1, longitude_1, latitude_2, longitude_2)

    coef = mod_math.cos(mod_math.radians(latitude_1))
    x = latitude_1 - latitude_2
    y = (longitude_1 - longitude_2) * coef

    distance_2d = mod_math.sqrt(x * x + y * y) * ONE_DEGREE

    if elevation_1 is None or elevation_2 is None or elevation_1 == elevation_2:
        return distance_2d

    return mod_math.sqrt(distance_2d ** 2 + (elevation_1 - elevation_2) ** 2)


def elevation_angle(location1: "Location", location2: "Location", radians: float=False) -> Optional[float]:
    """ Uphill/downhill angle between two locations. """
    if location1.elevation is None or location2.elevation is None:
        return None

    b = location2.elevation - location1.elevation
    a = location2.distance_2d(location1)

    if not a:
        return 0

    angle = mod_math.atan(b / a)

    if radians:
        return angle

    return mod_math.degrees(angle)


def distance_from_line(point: "Location", line_point_1: "Location", line_point_2: "Location") -> Optional[float]:
    """ Distance of point from a line given with two points. """
    assert point, point
    assert line_point_1, line_point_1
    assert line_point_2, line_point_2

    a = line_point_1.distance_2d(line_point_2)

    if not a:
        return line_point_1.distance_2d(point)

    b = line_point_1.distance_2d(point)
    c = line_point_2.distance_2d(point)

    if a is not None and b is not None and c is not None:
        s = (a + b + c) / 2
        return 2 * mod_math.sqrt(abs(s * (s - a) * (s - b) * (s - c))) / a
    return None


def get_line_equation_coefficients(location1: "Location", location2: "Location") -> Iterable[float]:
    """
    Get line equation coefficients for:
        latitude * a + longitude * b + c = 0

    This is a normal cartesian line (not spherical!)
    """
    if location1.longitude == location2.longitude:
        # Vertical line:
        return 0, 1, -location1.longitude
    else:
        a = (location1.latitude - location2.latitude) / (location1.longitude - location2.longitude)
        b = location1.latitude - location1.longitude * a
        return 1, -a, -b


def simplify_polyline(points: List["Location"], max_distance: Optional[float]) -> List["Location"]:
    """Does Ramer-Douglas-Peucker algorithm for simplification of polyline """

    _max_distance = max_distance if max_distance is not None else 10

    if len(points) < 3:
        return points

    begin, end = points[0], points[-1]

    # Use a "normal" line just to detect the most distant point (not its real distance)
    # this is because this is faster to compute than calling distance_from_line() for
    # every point.
    #
    # This is an approximation and may have some errors near the poles and if
    # the points are too distant, but it should be good enough for most use
    # cases...
    a, b, c = get_line_equation_coefficients(begin, end)

    # Initialize to safe values
    tmp_max_distance: float = 0
    tmp_max_distance_position = 1
    
    # Check distance of all points between begin and end, exclusive
    for point_no, point in enumerate(points[1:-1], 1):
        d = abs(a * point.latitude + b * point.longitude + c)
        if d > tmp_max_distance:
            tmp_max_distance = d
            tmp_max_distance_position = point_no

    # Now that we have the most distance point, compute its real distance:
    real_max_distance = distance_from_line(points[tmp_max_distance_position], begin, end)

    # If furthest point is less than max_distance, remove all points between begin and end
    if real_max_distance is not None and real_max_distance < _max_distance:
        return [begin, end]
    
    # If furthest point is more than max_distance, use it as anchor and run
    # function again using (begin to anchor) and (anchor to end), remove extra anchor
    return (simplify_polyline(points[:tmp_max_distance_position + 1], _max_distance) +
            simplify_polyline(points[tmp_max_distance_position:], _max_distance)[1:])


class Location:
    """ Generic geographical location """

    def __init__(self, latitude: float, longitude: float, elevation: Optional[float]=None) -> None:
        self.latitude = latitude
        self.longitude = longitude
        self.elevation = elevation

    def has_elevation(self) -> bool:
        return self.elevation is not None

    def remove_elevation(self) -> None:
        self.elevation = None

    def distance_2d(self, location: "Location") -> Optional[float]:
        if not location:
            return None

        return distance(self.latitude, self.longitude, None, location.latitude, location.longitude, None)

    def distance_3d(self, location: "Location") -> Optional[float]:
        if not location:
            return None

        return distance(self.latitude, self.longitude, self.elevation, location.latitude, location.longitude, location.elevation)

    def elevation_angle(self, location: "Location", radians: bool=False) -> Optional[float]:
        return elevation_angle(self, location, radians)

    def move(self, location_delta: "LocationDelta") -> None:
        self.latitude, self.longitude = location_delta.move(self)

    def __add__(self, location_delta: "LocationDelta") -> "Location":
        latitude, longitude = location_delta.move(self)
        return Location(latitude, longitude)

    def __str__(self) -> str:
        return f'[loc:{self.latitude},{self.longitude}@{self.elevation}]'

    def __repr__(self) -> str:
        if self.elevation is None:
            return f'Location({self.latitude}, {self.longitude})'
        else:
            return f'Location({self.latitude}, {self.longitude}, {self.elevation})'


class LocationDelta:
    """
    Intended to use similar to timestamp.timedelta, but for Locations.
    """

    NORTH = 0
    EAST = 90
    SOUTH = 180
    WEST = 270

    def __init__(self, distance: Optional[float]=None, angle: Optional[float]=None, latitude_diff: Optional[float]=None,
                 longitude_diff: Optional[float]=None) -> None:
        """
        Version 1:
            Distance (in meters).
            angle_from_north *clockwise*.
            ...must be given
        Version 2:
            latitude_diff and longitude_diff
            ...must be given
        """
        if (distance is not None) and (angle is not None):
            if (latitude_diff is not None) or (longitude_diff is not None):
                raise Exception('No lat/lon diff if using distance and angle!')
            self.distance = distance
            self.angle_from_north = angle
            self.move_function = self.move_by_angle_and_distance
        elif (latitude_diff is not None) and (longitude_diff is not None):
            if (distance is not None) or (angle is not None):
                raise Exception('No distance/angle if using lat/lon diff!')
            self.latitude_diff  = latitude_diff
            self.longitude_diff = longitude_diff
            self.move_function = self.move_by_lat_lon_diff

    def move(self, location: Location) -> Tuple[float, float]:
        """
        Move location by this timedelta.
        """
        return self.move_function(location)

    def move_by_angle_and_distance(self, location: Location) -> Tuple[float, float]:
        coef = mod_math.cos(mod_math.radians(location.latitude))
        vertical_distance_diff   = mod_math.sin(mod_math.radians(90 - self.angle_from_north)) / ONE_DEGREE
        horizontal_distance_diff = mod_math.cos(mod_math.radians(90 - self.angle_from_north)) / ONE_DEGREE
        lat_diff = self.distance * vertical_distance_diff
        lon_diff = self.distance * horizontal_distance_diff / coef
        return location.latitude + lat_diff, location.longitude + lon_diff

    def move_by_lat_lon_diff(self, location: "Location") -> Tuple[float, float]:
        return location.latitude + self.latitude_diff, location.longitude + self.longitude_diff
