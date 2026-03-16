import sys
PY3 = sys.version_info[0] == 3

if PY3:
    def int_from_byte(b):
        return b
else:
    int_from_byte = ord
