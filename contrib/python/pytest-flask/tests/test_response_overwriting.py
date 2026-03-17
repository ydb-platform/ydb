import pytest
from flask import Flask
from flask import jsonify
from flask import url_for


class TestResponseOverwriting:
    """
    we overwrite the app fixture here so we can test
    _monkeypatch_response_class (an autouse fixture)
    will return the original response_class since a
    json @property is already present in response_class
    """

    @pytest.fixture
    def app(self):
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "42"

        class MyResponse(app.response_class):
            @property
            def json(self):
                return 49

        @app.route("/ping")
        def ping():
            return jsonify(ping="pong")

        app.response_class = MyResponse

        return app

    def test_dont_rewrite_existing_implementation(self, accept_json, client):
        res = client.get(url_for("ping"), headers=accept_json)
        assert res.json == 49
