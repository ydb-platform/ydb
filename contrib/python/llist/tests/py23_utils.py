import sys


if sys.hexversion >= 0x03000000:
    # python 3 compatibility wrappers

    def py23_xrange(*args):
        return range(*args)

    def py23_range(*args):
        return list(range(*args))

    def cmp(a, b):
        if a == b:
            return 0
        elif a < b:
            return -1
        else:
            return 1

else:
    # python 2 compatibility wrappers

    def py23_xrange(*args):
        return xrange(*args)

    def py23_range(*args):
        return range(*args)
