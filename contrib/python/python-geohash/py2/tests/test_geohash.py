import unittest
import geohash

class TestEncode(unittest.TestCase):
	def test_cycle(self):
		for code in ["000000000000","zzzzzzzzzzzz","bgr96qxvpd46",]:
			self.assertEqual(code, geohash.encode(*geohash.decode(code)))

class TestDecode(unittest.TestCase):
	def test_empty(self):
		self.assertEqual(
			geohash.bbox(''),
			{'s':-90.0, 'n':90.0, 'w':-180.0, 'e':180.0})
	
	def test_one(self):
		seq = '0123456789bcdefghjkmnpqrstuvwxyz'
		sws = [
			(-90.0, -180.0),
			(-90.0, -135.0),
			(-45.0, -180.0),
			(-45.0, -135.0),
			(-90.0, -90.0),
			(-90.0, -45.0),
			(-45.0, -90.0),
			(-45.0, -45.0),
			(0.0, -180.0),
			(0.0, -135.0),
			(45.0, -180.0),
			(45.0, -135.0),
			(0.0, -90.0),
			(0.0, -45.0),
			(45.0, -90.0),
			(45.0, -45.0),
			(-90.0, 0.0),
			(-90.0, 45.0),
			(-45.0, 0.0),
			(-45.0, 45.0),
			(-90.0, 90.0),
			(-90.0, 135.0),
			(-45.0, 90.0),
			(-45.0, 135.0),
			(0.0, 0.0),
			(0.0, 45.0),
			(45.0, 0.0),
			(45.0, 45.0),
			(0.0, 90.0),
			(0.0, 135.0),
			(45.0, 90.0),
			(45.0, 135.0)
			]
		for i in zip(seq, sws):
			x = geohash.bbox(i[0])
			self.assertEqual((x['s'], x['w']), i[1])
			self.assertEqual(x['n']-x['s'], 45)
			self.assertEqual(x['e']-x['w'], 45)
	
	def test_ezs42(self):
		x=geohash.bbox('ezs42')
		self.assertEqual(round(x['s'],3), 42.583)
		self.assertEqual(round(x['n'],3), 42.627)
	
	def test_issue12(self):
		ll=geohash.decode(geohash.encode(51.566141,-0.009434,24))
		self.assertAlmostEqual(ll[0], 51.566141)
		self.assertAlmostEqual(ll[1], -0.009434)

class TestNeighbors(unittest.TestCase):
	def test_empty(self):
		self.assertEqual([], geohash.neighbors(""))
	
	def test_one(self):
		self.assertEqual(set(['1', '2', '3', 'p', 'r']), set(geohash.neighbors("0")))
		self.assertEqual(set(['w', 'x', 'y', '8', 'b']), set(geohash.neighbors("z")))
		self.assertEqual(set(['2', '6', '1', '0', '4', '9', '8', 'd']), set(geohash.neighbors("3")))

if __name__=='__main__':
	unittest.main()
