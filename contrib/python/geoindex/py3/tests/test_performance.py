from operator import itemgetter
import os
import random
import sys

if __name__ == '__main__':
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from geoindex import GeoGridIndex, GeoPoint


def get_random_point():
    lat = random.random()*180 - 90
    lng = random.random()*360 - 180
    return GeoPoint(lat, lng)


def test_performance(count=10):
    index = GeoGridIndex()
    points = [get_random_point() for _ in range(count)]
    list(map(index.add_point, points))
    for point in points:
        ls = list(index.get_nearest_points(point, 20))
        assert len(ls) > 0
        assert point in list(map(itemgetter(0), ls))


if __name__ == '__main__':
    test_performance(int(sys.argv[1]) if len(sys.argv)>1 else 10)
