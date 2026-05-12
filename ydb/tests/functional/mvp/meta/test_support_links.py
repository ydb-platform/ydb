from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

import pytest
from library.python.port_manager import PortManager

from support_links_env import (
    ABSOLUTE_GRAFANA_DASHBOARD_URL,
    CLUSTER_NAME,
    DATABASE_NAME,
    DATASOURCE_ID,
    MISSING_CLUSTER_ERROR,
    MISSING_CLUSTER_NAME,
    MISSING_CLUSTER_PARAMETER_ERROR,
    WORKSPACE_NAME,
    start_cluster_with_meta_table,
    start_meta_support_links_service,
    started_meta_support_links_env,
)


@dataclass(frozen=True)
class SupportLinksRequestCase:
    database: str | None
    expected_url: str


@dataclass(frozen=True)
class SupportLinksConfigCase:
    env_kwargs: dict
    expected_url: str


def assert_support_links_response(payload, expected_url):
    assert len(payload["links"]) == 1
    assert payload["links"][0]["title"] == "Overview"

    actual_url = payload["links"][0]["url"]
    assert_urls_match(actual_url, expected_url)
    assert "errors" not in payload


def assert_support_links_error(payload, message_substring):
    assert payload["links"] == []
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["source"] == "meta"
    assert message_substring in payload["errors"][0]["message"]


def assert_support_links_response_with_error(payload, expected_url, message_substring):
    assert len(payload["links"]) == 1
    assert payload["links"][0]["title"] == "Overview"

    actual_url = payload["links"][0]["url"]
    assert_urls_match(actual_url, expected_url)
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["source"] == "meta"
    assert message_substring in payload["errors"][0]["message"]


def assert_bad_request_response(payload, message_substring):
    assert payload["links"] == []
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["source"] == "meta"
    assert message_substring in payload["errors"][0]["message"]


def assert_urls_match(actual_url, expected_url):
    actual_split = urlsplit(actual_url)
    expected_split = urlsplit(expected_url)

    assert actual_split.scheme == expected_split.scheme
    assert actual_split.netloc == expected_split.netloc
    assert actual_split.path == expected_split.path
    assert sorted(parse_qsl(actual_split.query, keep_blank_values=True)) == sorted(
        parse_qsl(expected_split.query, keep_blank_values=True)
    )


def grafana_url_with_cluster(cluster_name=CLUSTER_NAME, extra_query=""):
    url = (
        "https://grafana.example.test/d/ydb/overview"
        f"?var-workspace={WORKSPACE_NAME}"
        f"&var-ds={DATASOURCE_ID}"
        f"&var-cluster={cluster_name}"
    )
    if extra_query:
        url += f"&{extra_query}"
    return url


def external_grafana_url(extra_query=""):
    url = (
        "https://external.example.test/d/ydb/overview"
        f"?var-workspace={WORKSPACE_NAME}"
        f"&var-ds={DATASOURCE_ID}"
        f"&var-cluster={CLUSTER_NAME}"
    )
    if extra_query:
        url += f"&{extra_query}"
    return url


def grafana_url_with_cluster_only(cluster_name):
    return "https://grafana.example.test/d/ydb/overview" f"?var-cluster={cluster_name}"


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            SupportLinksRequestCase(
                database=None,
                expected_url=grafana_url_with_cluster(),
            ),
            id="cluster-links",
        ),
        pytest.param(
            SupportLinksRequestCase(
                database=DATABASE_NAME,
                expected_url=grafana_url_with_cluster(extra_query=f"var-database={DATABASE_NAME}"),
            ),
            id="database-links",
        ),
    ],
)
def test_meta_support_links_returns_grafana_link(meta_support_links_env, case):
    assert_support_links_response(
        meta_support_links_env.get_ok_support_links_payload(CLUSTER_NAME, database=case.database),
        case.expected_url,
    )


def test_meta_support_links_returns_error_for_missing_cluster(meta_support_links_env):
    assert_support_links_response_with_error(
        meta_support_links_env.get_ok_support_links_payload(MISSING_CLUSTER_NAME),
        grafana_url_with_cluster_only(MISSING_CLUSTER_NAME),
        MISSING_CLUSTER_ERROR,
    )


def test_meta_support_links_does_not_start_with_invalid_config():
    cluster, driver = start_cluster_with_meta_table()
    try:
        with PortManager() as port_manager:
            http_port = port_manager.get_port()
            with pytest.raises(AssertionError, match="mvp_meta did not become ready on /ping"):
                start_meta_support_links_service(
                    cluster,
                    http_port,
                    include_grafana_endpoint=False,
                )
    finally:
        driver.stop()
        cluster.stop()


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(
            SupportLinksConfigCase(
                env_kwargs={"url": ABSOLUTE_GRAFANA_DASHBOARD_URL},
                expected_url=external_grafana_url(),
            ),
            id="absolute-url",
        ),
        pytest.param(
            SupportLinksConfigCase(
                env_kwargs={"datasource": ""},
                expected_url=(
                    "https://grafana.example.test/d/ydb/overview"
                    f"?var-workspace={WORKSPACE_NAME}"
                    f"&var-cluster={CLUSTER_NAME}"
                ),
            ),
            id="empty-datasource",
        ),
    ],
)
def test_meta_support_links_returns_expected_link_for_config(case):
    with started_meta_support_links_env(**case.env_kwargs) as env:
        assert_support_links_response(
            env.get_ok_support_links_payload(CLUSTER_NAME),
            case.expected_url,
        )


def test_meta_support_links_requires_cluster_parameter(meta_support_links_env):
    assert_bad_request_response(
        meta_support_links_env.get_bad_request_support_links_payload(database=DATABASE_NAME),
        MISSING_CLUSTER_PARAMETER_ERROR,
    )
