import socket

import pytest

# import uvloop  # TODO: run the entire test suite with both uvloop and the vanilla loop.


@pytest.fixture(scope="session")
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return f
