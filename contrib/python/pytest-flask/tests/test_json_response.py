import pytest
from flask import url_for


class TestJSONResponse:
    def test_json_response(self, client, accept_json):
        res = client.get(url_for("ping"), headers=accept_json)
        assert res.json == {"ping": "pong"}

    def test_json_response_compare_to_status_code(self, client, accept_json):
        assert client.get(url_for("ping"), headers=accept_json) == 200
        assert client.get("fake-route", headers=accept_json) == 404
        assert client.get("fake-route", headers=accept_json) != "404"
        res = client.get(url_for("ping"), headers=accept_json)
        assert res == res

    def test_mismatching_eq_comparison(self, client, accept_json):
        with pytest.raises(AssertionError, match=r"Mismatch in status code"):
            assert client.get("fake-route", headers=accept_json) == 200
        with pytest.raises(AssertionError, match=r"404 NOT FOUND"):
            assert client.get("fake-route", headers=accept_json) == "200"
