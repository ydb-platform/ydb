import unittest

class TestEditDistance(unittest.TestCase):
    def test_editdistance(self):
        import editdistance
        self.assertEqual(1, editdistance.eval('abc', 'aec'))
    

if __name__ == '__main__':
    unittest.main()