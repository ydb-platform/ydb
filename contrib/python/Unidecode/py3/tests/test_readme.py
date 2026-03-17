import doctest
import sys

def additional_tests():
	if sys.maxunicode >= 0x10000:
		return doctest.DocFileSuite("../README.rst")
	else:
		return doctest.DocFileSuite()
