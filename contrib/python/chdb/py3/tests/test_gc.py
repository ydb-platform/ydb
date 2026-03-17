#!python3

import unittest
import gc
import chdb

class TestGC(unittest.TestCase):
    def test_gc(self):
        print("query started")
        gc.set_debug(gc.DEBUG_STATS)

        ret = chdb.query("SELECT 123,'adbcd'", 'CSV')
        # print("ret:", ret)
        # print("ret type:", type(ret))
        self.assertEqual(str(ret), '123,"adbcd"\n')
        gc.collect()

        mv = ret.get_memview()
        self.assertIsNotNone(mv)
        gc.collect()

        self.assertEqual(len(mv), 12)

        out = mv.tobytes()
        self.assertEqual(out, b'123,"adbcd"\n')

        ret2 = chdb.query("SELECT 123,'adbcdefg'", 'CSV').get_memview().tobytes()
        self.assertEqual(ret2, b'123,"adbcdefg"\n')

        mv2 = chdb.query("SELECT 123,'adbcdefg'", 'CSV').get_memview()
        gc.collect()

        self.assertEqual(mv2.tobytes(), b'123,"adbcdefg"\n')

        mv3 = mv2.view()
        gc.collect()
        self.assertEqual(mv3.tobytes(), b'123,"adbcdefg"\n')
        self.assertEqual(len(mv3), 15)


if __name__ == '__main__':
    unittest.main()
