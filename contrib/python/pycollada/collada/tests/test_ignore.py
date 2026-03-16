import os
import unittest

import collada
from collada.common import (DaeError,
                            DaeBrokenRefError,
                            DaeIncompleteError,
                            DaeMalformedError)


class TestColladaIgnore(unittest.TestCase):
    """
    Check the `ignore` functionality for ignoring
    parsing errors.
    """

    def setUp(self):
        self.dummy = collada.Collada(validate_output=True)
        self.datadir = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), "data")

    def test_incomplete(self):
        # this has an incomplete error
        f = os.path.join(self.datadir, "cube_tristrips.dae")
        raised = False
        try:
            collada.Collada(f)
        except DaeIncompleteError:
            # this should have raised
            raised = True
        if not raised:
            raise ValueError('should have raised an error!')

        raised = False
        try:
            collada.Collada(
                f, ignore=[DaeMalformedError])
        except DaeIncompleteError:
            raised = True
        if not raised:
            raise ValueError('should have raised an error')

        # should have loaded if we ignore the specific error
        m = collada.Collada(f, ignore=[DaeIncompleteError])
        # should have one primitive
        assert len(m.geometries) == 1
        assert len(m.geometries[0].primitives) == 1
        # should be an (n, 3) numpy array
        assert m.geometries[0].primitives[0].normal.shape[1] == 3

        # should have loaded if we ignore the parent error
        m = collada.Collada(f, ignore=[DaeError])
        # should have one primitive
        assert len(m.geometries) == 1
        assert len(m.geometries[0].primitives) == 1
        # should be an (n, 3) numpy array
        assert m.geometries[0].primitives[0].normal.shape[1] == 3

    def test_malformed(self):
        # this has a malformed component
        f = os.path.join(self.datadir, "earthCylindrical.DAE")
        raised = False
        try:
            collada.Collada(f)
        except DaeMalformedError:
            # this should have raised
            raised = True
        if not raised:
            raise ValueError('should have raised an error!')

        raised = False
        try:
            collada.Collada(f, ignore=[DaeMalformedError])
        except DaeError:
            # this should have raised
            raised = True
        if not raised:
            raise ValueError('should have raised an error!')

        # should't have crashed
        collada.Collada(f, ignore=[DaeMalformedError,
                                   DaeBrokenRefError])
        # should have also loaded using the parent class
        collada.Collada(f, ignore=[DaeError])


if __name__ == '__main__':
    unittest.main()
