import doctest
import sys

def additional_tests():
	if sys.version_info[0] >= 3 and sys.maxunicode >= 0x10000:
		return doctest.DocFileSuite("../README.rst")
	else:
		return doctest.DocFileSuite()
