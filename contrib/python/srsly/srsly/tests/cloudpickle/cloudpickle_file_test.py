import os
import shutil
import sys
import tempfile
import unittest

import pytest

import srsly.cloudpickle as cloudpickle
from srsly.cloudpickle.compat import pickle


class CloudPickleFileTests(unittest.TestCase):
    """In Cloudpickle, expected behaviour when pickling an opened file
    is to send its contents over the wire and seek to the same position."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tmpfilepath = os.path.join(self.tmpdir, 'testfile')
        self.teststring = 'Hello world!'

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_empty_file(self):
        # Empty file
        open(self.tmpfilepath, 'w').close()
        with open(self.tmpfilepath, 'r') as f:
            self.assertEqual('', pickle.loads(cloudpickle.dumps(f)).read())
        os.remove(self.tmpfilepath)

    def test_closed_file(self):
        # Write & close
        with open(self.tmpfilepath, 'w') as f:
            f.write(self.teststring)
        with pytest.raises(pickle.PicklingError) as excinfo:
            cloudpickle.dumps(f)
        assert "Cannot pickle closed files" in str(excinfo.value)
        os.remove(self.tmpfilepath)

    def test_r_mode(self):
        # Write & close
        with open(self.tmpfilepath, 'w') as f:
            f.write(self.teststring)
        # Open for reading
        with open(self.tmpfilepath, 'r') as f:
            new_f = pickle.loads(cloudpickle.dumps(f))
            self.assertEqual(self.teststring, new_f.read())
        os.remove(self.tmpfilepath)

    def test_w_mode(self):
        with open(self.tmpfilepath, 'w') as f:
            f.write(self.teststring)
            f.seek(0)
            self.assertRaises(pickle.PicklingError,
                              lambda: cloudpickle.dumps(f))
        os.remove(self.tmpfilepath)

    def test_plus_mode(self):
        # Write, then seek to 0
        with open(self.tmpfilepath, 'w+') as f:
            f.write(self.teststring)
            f.seek(0)
            new_f = pickle.loads(cloudpickle.dumps(f))
            self.assertEqual(self.teststring, new_f.read())
        os.remove(self.tmpfilepath)

    def test_seek(self):
        # Write, then seek to arbitrary position
        with open(self.tmpfilepath, 'w+') as f:
            f.write(self.teststring)
            f.seek(4)
            unpickled = pickle.loads(cloudpickle.dumps(f))
            # unpickled StringIO is at position 4
            self.assertEqual(4, unpickled.tell())
            self.assertEqual(self.teststring[4:], unpickled.read())
            # but unpickled StringIO also contained the start
            unpickled.seek(0)
            self.assertEqual(self.teststring, unpickled.read())
        os.remove(self.tmpfilepath)

    @pytest.mark.skip(reason="Requires pytest -s to pass")
    def test_pickling_special_file_handles(self):
        # Warning: if you want to run your tests with nose, add -s option
        for out in sys.stdout, sys.stderr:  # Regression test for SPARK-3415
            self.assertEqual(out, pickle.loads(cloudpickle.dumps(out)))
        self.assertRaises(pickle.PicklingError,
                          lambda: cloudpickle.dumps(sys.stdin))


if __name__ == '__main__':
    unittest.main()
