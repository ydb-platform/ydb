import sys


def mlockall_current():
    if not sys.platform.startswith('linux'):
        return -1

    import library.python.mlockall.mlockall as ml

    return ml.mlockall_current()
