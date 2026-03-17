from itertools import chain
import geohash
from . import utils, GeoPoint

# dependence between hashtag's precision and distance accurate calculating
# in fact it's sizes of grids in km
GEO_HASH_GRID_SIZE = {
    1: 5000.0,
    2: 1260.0,
    3: 156.0,
    4: 40.0,
    5: 4.8,
    6: 1.22,
    7: 0.152,
    8: 0.038
}


class GeoGridIndex(object):
    """
    Class for store index based on geohash of points for quick-and-dry
    neighbors search
    """
    def __init__(self, precision=4):
        """
        :param precision:
        """
        self.precision = precision
        self.data = {}

    def get_point_hash(self, point):
        """
        return geohash for given point with self.precision
        :param point: GeoPoint instance
        :return: string
        """
        return geohash.encode(point.latitude, point.longitude, self.precision)

    def add_point(self, point):
        """
        add point to index, point must be a GeoPoint instance
        :param point:
        :return:
        """
        assert isinstance(point, GeoPoint), \
            'point should be GeoPoint instance'
        point_hash = self.get_point_hash(point)
        points = self.data.setdefault(point_hash, [])
        points.append(point)

    def get_nearest_points_dirty(self, center_point, radius, unit='km'):
        """
        return approx list of point from circle with given center and radius
        it uses geohash and return with some error (see GEO_HASH_ERRORS)
        :param center_point: center of search circle
        :param radius: radius of search circle
        :return: list of GeoPoints from given area
        """
        if unit == 'mi':
            radius = utils.mi_to_km(radius)
        grid_size = GEO_HASH_GRID_SIZE[self.precision]
        if radius > grid_size / 2:
            # radius is too big for current grid, we cannot use 9 neighbors
            # to cover all possible points
            suggested_precision = 0
            for precision, max_size in GEO_HASH_GRID_SIZE.items():
                if radius > max_size / 2:
                    suggested_precision = precision - 1
                    break
            raise ValueError(
                'Too large radius, please rebuild GeoHashGrid with '
                'precision={0}'.format(suggested_precision)
            )
        me_and_neighbors = geohash.expand(self.get_point_hash(center_point))
        return chain(*(self.data.get(key, []) for key in me_and_neighbors))

    def get_nearest_points(self, center_point, radius, unit='km'):
        """
        return list of geo points from circle with given center and radius
        :param center_point: GeoPoint with center of search circle
        :param radius: radius of search circle
        :return: generator with tuple with GeoPoints and distance
        """
        assert isinstance(center_point, GeoPoint), \
            'point should be GeoPoint instance'
        for point in self.get_nearest_points_dirty(center_point, radius):
            distance = point.distance_to(center_point, unit)
            if distance <= radius:
                yield point, distance
