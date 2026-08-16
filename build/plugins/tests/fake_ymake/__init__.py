def addparser():
    pass


def parser(*args, **kw):
    return lambda x: x


def macro(f=None, *, ignored_args=set()):
    if f:
        return f

    def impl(f):
        return f

    return impl


class Unit:
    pass
