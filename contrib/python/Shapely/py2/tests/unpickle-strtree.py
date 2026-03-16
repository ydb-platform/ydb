# See test_strtree.py::test_pickle_persistence

import sys
import os
sys.path.append(os.getcwd())

import pickle
from shapely.geometry import Point
from shapely.geos import geos_version


if __name__ == "__main__":
    try:
        pickled_strtree = sys.stdin.buffer.read()
    except AttributeError:
        # Python 2.7
        pickled_strtree = sys.stdin.read()

    print("received pickled strtree:", repr(pickled_strtree))
    strtree = pickle.loads(pickled_strtree)
    # Exercise API.
    print("calling \"query()\"...")
    strtree.query(Point(0, 0))
    if geos_version >= (3, 6, 0):
        print("calling \"nearest()\"...")
        strtree.nearest(Point(0, 0))
    else:
        print("skipping \"nearest()\"")
    print("done")
