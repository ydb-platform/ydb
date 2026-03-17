import os

import pytest
import requests_mock

import zeep

import yatest.common
WSDL = yatest.common.test_source_path("integration/test_http_post.wsdl")


@pytest.mark.requests
def test_get_urlreplacement():
    client = zeep.Client(WSDL)

    with requests_mock.mock() as m:
        m.get("http://example.com/companyinfo/o1/EUR/", text="<root>Hoi</root>")
        result = client.service.o1("EUR")
        assert result == "Hoi"

        history = m.request_history[0]
        assert history._request.path_url == "/companyinfo/o1/EUR/"


@pytest.mark.requests
def test_post_mime_content():
    client = zeep.Client(WSDL, service_name="CompanyInfoService", port_name="Port3")

    with requests_mock.mock() as m:
        m.post("http://example.com/companyinfo/o1", text="<root>Hoi</root>")
        result = client.service.o1("EUR")
        assert result == "Hoi"

        history = m.request_history[0]
        assert history._request.path_url == "/companyinfo/o1"
