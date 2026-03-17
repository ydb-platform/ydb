from __future__ import annotations

import json
from typing import Any, Dict, Sequence

import pytest

from aiodocker import utils


def test_clean_mapping() -> None:
    dirty_dict: Dict[Any, Any] = {"a": None, "b": {}, "c": [], "d": 1}
    clean_dict: Dict[Any, Any] = {"b": {}, "c": [], "d": 1}
    result = utils.clean_map(dirty_dict)
    assert result == clean_dict


def test_parse_content_type() -> None:
    ct = "text/plain"
    mt, st, opts = utils.parse_content_type(ct)
    assert mt == "text"
    assert st == "plain"
    assert opts == {}

    ct = "text/plain; charset=utf-8"
    mt, st, opts = utils.parse_content_type(ct)
    assert mt == "text"
    assert st == "plain"
    assert opts == {"charset": "utf-8"}

    ct = "text/plain; "
    mt, st, opts = utils.parse_content_type(ct)
    assert mt == "text"
    assert st == "plain"
    assert opts == {}

    ct = "text/plain; asdfasdf"
    with pytest.raises(ValueError):
        mt, st, opts = utils.parse_content_type(ct)


def test_format_env() -> None:
    assert utils.format_env("name", "hello") == "name=hello"
    assert utils.format_env("name", None) == "name"
    assert utils.format_env("name", b"hello") == "name=hello"


def test_clean_networks() -> None:
    networks: Sequence[str] = []
    assert utils.clean_networks(networks) == []

    networks = ("test-network-1", "test-network-2")
    with pytest.raises(TypeError) as excinfo:
        result = utils.clean_networks(networks)
    assert "networks parameter must be a list." in str(excinfo.value)

    networks = ["test-network-1", "test-network-2"]
    result = [{"Target": "test-network-1"}, {"Target": "test-network-2"}]
    assert utils.clean_networks(networks) == result


def test_clean_filters() -> None:
    filters = {"a": ["1", "2", "3", "4"], "b": "string"}
    result = {"a": ["1", "2", "3", "4"], "b": ["string"]}
    assert utils.clean_filters(filters=filters) == json.dumps(result)
    assert utils.clean_filters(filters={}) == "{}"
    assert utils.clean_filters(filters=None) == "{}"

    with pytest.raises(TypeError):
        assert utils.clean_filters(filters=())


def test_compose_auth_header():
    auth = {"username": "alice", "password": "~"}
    assert (
        utils.compose_auth_header(auth)
        == "eyJ1c2VybmFtZSI6ICJhbGljZSIsICJwYXNzd29yZCI6ICJ-In0="
    )
