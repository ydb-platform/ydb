from tblib import pickling_support

import six
import sys
import pickle


def test_basic():
    pickling_support.install()

    try:
        raise ValueError
    except ValueError:
        s = pickle.dumps(sys.exc_info())

    f = None
    try:
        six.reraise(*pickle.loads(s))
    except ValueError:
        f = object()

    assert f is not None
