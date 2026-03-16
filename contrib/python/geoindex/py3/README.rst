========
GeoIndex
========

Simple library for perform quick nearby search for geo spatial data.

Requirements
------------

* python-geohash

It was written and tested on Python 2.7.

Installation
------------

Get it from `pypi <http://pypi.python.org/pypi/geoindex>`_::

    pip install geoindex

or `github <http://github.com/gusdan/geoindex>`_::

    pip install -e git://github.com/gusdan/geoindex.git#egg=geoindex


Simple use
----------

If we have 100000 geo coordinates and we have to find some nearby location
to given point and with given radius we can do something like this::

    from geoindex import GeoGridIndex, GeoPoint

    geo_index = GeoGridIndex()
    for _ in range(10000):
        lat = random.random()*180 - 90
        lng = random.random()*360 - 180
        index.add_point(GeoPoint(lat, lng))

    center_point = GeoPoint(37.7772448, -122.3955118)
    for distance, point in index.get_nearest_points(center_point, 10, 'km'):
        print("We found {0} in {1} km".format(point, distance))


Search with associated data
---------------------------
When we fill index we can pass ref to GeoPoint as reference to some object
and use it after::

    from geoindex import GeoGridIndex, GeoPoint

    index = GeoGridIndex()
    for airport in get_all_airports():
        index.add_point(GeoPoint(lat, lng, ref=airport))

    center_point = GeoPoint(37.7772448, -122.3955118)
    for distance, point in index.get_nearest_points(center_point, 10, 'km'):
        print("We airport {0} in {1} km".format(point.ref, distance))


Performance
-----------

Creating index with 10000 random points and nearby search for each point took
about 400ms. See tests/test_performance.py for more details.


How does it work
----------------

For perform quick search GeoGridIndex uses
`Geohash <http://en.wikipedia.org/wiki/Geohash>`_ for each point and store it
in internal dictionary. When we initialize GeoGridIndex we pass precision to
constructor, based on it we divide all spaces with grid and store each point
inside that grid.
When we search nearest points we define cell with center point and 8 neighbors
and check all points from these cells for distance to center.

Each cell has size dependent on precision, bellow you can find grid's cell size
and precision.


+-----------+------------+
| Precision | Cell size  |
+===========+============+
| 1         | 5000       |
+-----------+------------+
| 2         | 1260       |
+-----------+------------+
| 3         | 156        |
+-----------+------------+
| 4         | 40         |
+-----------+------------+
| 5         | 4.8        |
+-----------+------------+
| 6         | 1.22       |
+-----------+------------+
| 7         | 0.152      |
+-----------+------------+
| 8         | 0.038      |
+-----------+------------+


If you have created GeoGridIndex with precision = 4 it means what all points
will be assign to grid with cell size = 40 km. And we can run search with
radius less than 40/2km. If we'd like to make bigger radius we should create
index with less precision 3.

But in other side if we create index with smallest precision (1) and will run
search (get_nearest_points) with small radius (1km for example) it will works
fine, but it will check all points inside 9 grid cells with size 5000km and it
can be not so fast.