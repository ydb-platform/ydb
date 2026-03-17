import pytest
from flask import request
from flask import url_for


class TestFixtures:
    def test_config_access(self, config):
        assert config["SECRET_KEY"] == "42"

    def test_client(self, client):
        assert client.get(url_for("ping")).status == "200 OK"

    def test_accept_json(self, accept_json):
        assert accept_json == [("Accept", "application/json")]

    def test_accept_jsonp(self, accept_jsonp):
        assert accept_jsonp == [("Accept", "application/json-p")]

    def test_accept_mimetype(self, accept_mimetype):
        mimestrings = [[("Accept", "application/json")], [("Accept", "text/html")]]
        assert accept_mimetype in mimestrings

    def test_accept_any(self, accept_any):
        mimestrings = [[("Accept", "*")], [("Accept", "*/*")]]
        assert accept_any in mimestrings


@pytest.mark.usefixtures("client_class")
class TestClientClass:
    def test_client_attribute(self):
        assert hasattr(self, "client")
        assert self.client.get(url_for("ping")).json == {"ping": "pong"}
