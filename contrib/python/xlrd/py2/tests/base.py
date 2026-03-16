import os


def from_this_dir(filename):
    try:
        import yatest.common
        return yatest.common.test_source_path(os.path.join(filename))
    except ImportError:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
