# coding: UTF-8
import unittest
import jpiarea

def dms(d,m,s):
	return float(d) + (float(m) + float(s)/60)/60.0

class TestReference(unittest.TestCase):
	# hash code examples from open iarea document
	# http://www.nttdocomo.co.jp/service/imode/make/content/iarea/domestic/index.html
	def test_lv1(self):
		self.assertEqual("5438", jpiarea.encode(36,138)[0:4])
		self.assertEqual("5637", jpiarea.encode(dms(37,20,0),137)[0:4])
	
	def test_lv2(self):
		p = jpiarea.bbox("533946")
		self.assertAlmostEqual(503100000, p["s"]*3600*1000)
		self.assertAlmostEqual(503550000, p["n"]*3600*1000)
		self.assertAlmostEqual(128400000, p["w"]*3600*1000)
		self.assertAlmostEqual(128700000, p["e"]*3600*1000)
	
	def test_lv3(self):
		p = jpiarea.bbox("5339463")
		self.assertAlmostEqual(503325000, p["s"]*3600*1000)
		self.assertAlmostEqual(503550000, p["n"]*3600*1000)
		self.assertAlmostEqual(128550000, p["w"]*3600*1000)
		self.assertAlmostEqual(128700000, p["e"]*3600*1000)
	
	def test_lvN(self):
		self.assertEqual("53394600300",jpiarea.encode(dms(35,40,41), dms(139,46,9.527)))

if __name__=='__main__':
	unittest.main()
