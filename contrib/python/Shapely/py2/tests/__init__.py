import sys
import unittest

from shapely.geos import geos_version_string, lgeos, WKTWriter
from shapely import speedups


test_int_types = [int]

try:
    import numpy
    numpy_version = numpy.version.version
    test_int_types.extend([int, numpy.int16, numpy.int32, numpy.int64])
except ImportError:
    numpy = False
    numpy_version = 'not available'

# Show some diagnostic information; handy for Travis CI
print('Python version: ' + sys.version.replace('\n', ' '))
print('GEOS version: ' + geos_version_string)
print('Numpy version: ' + numpy_version)
print('Cython speedups: ' + str(speedups.available))

if lgeos.geos_version >= (3, 3, 0):
    # Remove any WKT writer defaults to pass tests for all versions of GEOS
    WKTWriter.defaults = {}
