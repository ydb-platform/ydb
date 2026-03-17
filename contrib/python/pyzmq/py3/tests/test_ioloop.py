# Copyright (C) PyZMQ Developers
# Distributed under the terms of the Modified BSD License.


import pytest

tornado = pytest.importorskip("tornado")


def test_ioloop():
    # may have been imported before,
    # can't capture the warning
    from zmq.eventloop import ioloop

    assert ioloop.IOLoop is tornado.ioloop.IOLoop
    assert ioloop.ZMQIOLoop is ioloop.IOLoop


def test_ioloop_install():
    from zmq.eventloop import ioloop

    with pytest.warns(DeprecationWarning):
        ioloop.install()
