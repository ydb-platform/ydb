#!/usr/bin/env python
"""
    A py.test plugin which helps testing Flask applications.

    :copyright: (c) by Vital Kudzelka
    :license: MIT
"""
import pytest

from .fixtures import accept_any
from .fixtures import accept_json
from .fixtures import accept_jsonp
from .fixtures import accept_mimetype
from .fixtures import client
from .fixtures import client_class
from .fixtures import config
from .fixtures import live_server
from .pytest_compat import getfixturevalue


class JSONResponse:
    """Mixin with testing helper methods for JSON responses."""

    def __eq__(self, other):
        if isinstance(other, int):
            return self.status_code == other
        return super().__eq__(other)

    def __ne__(self, other):
        return not self == other


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, JSONResponse) and op == "==" and isinstance(right, int):
        return [
            "Mismatch in status code for response: {} != {}".format(
                left.status_code,
                right,
            ),
            "Response status: {}".format(left.status),
        ]
    return None


def _make_test_response_class(response_class):
    """Extends the response class with special attribute to test JSON
    responses. Don't override user-defined `json` attribute if any.

    :param response_class: An original response class.
    """
    if "json" in response_class.__dict__:
        return response_class

    return type(str(JSONResponse), (response_class, JSONResponse), {})


@pytest.fixture(autouse=True)
def _monkeypatch_response_class(request, monkeypatch):
    """Set custom response class before test suite and restore the original
    after. Custom response has `json` property to easily test JSON responses::

        @app.route('/ping')
        def ping():
            return jsonify(ping='pong')

        def test_json(client):
            res = client.get(url_for('ping'))
            assert res.json == {'ping': 'pong'}

    """

    if "app" not in request.fixturenames:
        return

    app = getfixturevalue(request, "app")
    monkeypatch.setattr(
        app, "response_class", _make_test_response_class(app.response_class)
    )


@pytest.fixture(autouse=True)
def _push_request_context(request):
    """During tests execution request context has been pushed, e.g. `url_for`,
    `session`, etc. can be used in tests as is::

        def test_app(app, client):
            assert client.get(url_for('myview')).status_code == 200

    """
    if "app" not in request.fixturenames:
        return

    app = getfixturevalue(request, "app")

    # Get application bound to the live server if ``live_server`` fixture
    # is applied. Live server application has an explicit ``SERVER_NAME``,
    # so ``url_for`` function generates a complete URL for endpoint which
    # includes application port as well.
    if "live_server" in request.fixturenames:
        app = getfixturevalue(request, "live_server").app

    ctx = app.test_request_context()
    ctx.push()

    def teardown():
        ctx.pop()

    request.addfinalizer(teardown)


@pytest.fixture(autouse=True)
def _configure_application(request, monkeypatch):
    """Use `pytest.mark.options` decorator to pass options to your application
    factory::

        @pytest.mark.options(debug=False)
        def test_something(app):
            assert not app.debug, 'the application works not in debug mode!'

    """
    if "app" not in request.fixturenames:
        return

    app = getfixturevalue(request, "app")
    for options in request.node.iter_markers("options"):
        for key, value in options.kwargs.items():
            monkeypatch.setitem(app.config, key.upper(), value)


def pytest_addoption(parser):
    group = parser.getgroup("flask")
    group.addoption(
        "--start-live-server",
        action="store_true",
        dest="start_live_server",
        default=True,
        help="start server automatically when live_server "
        "fixture is applied (enabled by default).",
    )
    group.addoption(
        "--no-start-live-server",
        action="store_false",
        dest="start_live_server",
        help="don't start server automatically when live_server " "fixture is applied.",
    )
    group.addoption(
        "--live-server-wait",
        action="store",
        dest="live_server_wait",
        default=5,
        type=float,
        help="the timeout after which test case is aborted if live server is "
        " not started.",
    )
    group.addoption(
        "--live-server-clean-stop",
        action="store_true",
        dest="live_server_clean_stop",
        default=True,
        help="attempt to kill the live server cleanly.",
    )
    group.addoption(
        "--no-live-server-clean-stop",
        action="store_false",
        dest="live_server_clean_stop",
        help="terminate the server forcefully after stop.",
    )
    group.addoption(
        "--live-server-host",
        action="store",
        default="localhost",
        type=str,
        help="use a host where to listen (default localhost).",
    )
    group.addoption(
        "--live-server-port",
        action="store",
        default=0,
        type=int,
        help="use a fixed port for the live_server fixture.",
    )
    parser.addini(
        "live_server_scope",
        "modify the scope of the live_server fixture.",
        default="session",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "app(options): pass options to your application factory"
    )
    config.addinivalue_line("markers", "options: app config manipulation")
