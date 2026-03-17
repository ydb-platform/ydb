from sanic import Sanic  # type: ignore

from sanic_testing.testing import SanicASGITestClient, SanicTestClient


class TestManager:
    __test__ = False

    def __init__(self, app: Sanic) -> None:
        self.test_client = SanicTestClient(app)
        self.asgi_client = SanicASGITestClient(app)
        app._test_manager = self  # type: ignore
