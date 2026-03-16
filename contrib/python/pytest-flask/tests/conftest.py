#!/usr/bin/env python
from textwrap import dedent

import pytest
from flask import Flask
from flask import jsonify

from pytest_flask.fixtures import mimetype

pytest_plugins = "pytester"


@pytest.fixture(scope="session")
def app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "42"

    @app.route("/")
    def index():
        return app.response_class("OK")

    @app.route("/ping")
    def ping():
        return jsonify(ping="pong")

    return app


@pytest.fixture
def appdir(testdir):
    app_root = testdir.tmpdir
    test_root = app_root.mkdir("tests")

    def create_test_module(code, filename="test_app.py"):
        f = test_root.join(filename)
        f.write(dedent(code), ensure=True)
        return f

    testdir.create_test_module = create_test_module

    testdir.create_test_module(
        """
        import pytest

        from flask import Flask

        @pytest.fixture(scope='session')
        def app():
            app = Flask(__name__)
            return app
    """,
        filename="conftest.py",
    )
    return testdir
