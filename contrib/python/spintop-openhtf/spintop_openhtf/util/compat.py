import sys

if sys.version_info >= (3, 0):
    def isstr(string):
        return isinstance(string, str)
else:
    def isstr(string):
        return isinstance(string, basestring)