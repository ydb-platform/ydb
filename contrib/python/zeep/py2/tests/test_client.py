import os

import pytest
import requests_mock

from tests.utils import load_xml
from zeep import client, xsd
from zeep.exceptions import Error

import yatest.common


def test_bind():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    service = client_obj.bind()
    assert service


def test_unknown_transport():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap_transport_err.wsdl"))
    service = client_obj.bind()
    assert service


def test_bind_service():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    service = client_obj.bind("StockQuoteService")
    assert service


def test_bind_service_port():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    service = client_obj.bind("StockQuoteService", "StockQuotePort")
    assert service


def test_service_proxy_ok():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    assert client_obj.service.GetLastTradePrice


def test_service_proxy_non_existing():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    with pytest.raises(AttributeError):
        assert client_obj.service.NonExisting


def test_service_proxy_dir_operations():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    operations = [op for op in dir(client_obj.service) if not op.startswith("_")]
    assert set(operations) == set(["GetLastTradePrice", "GetLastTradePriceNoOutput"])


def test_operation_proxy_doc():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    assert (
        client_obj.service.GetLastTradePrice.__doc__
        == "GetLastTradePrice(tickerSymbol: xsd:string, "
        "account: ns0:account, "
        "country: ns0:country) -> price: xsd:float"
    )


def test_open_from_file_object():
    with open(yatest.common.test_source_path("wsdl_files/soap_transport_err.wsdl"), "rb") as fh:
        client_obj = client.Client(fh)
        service = client_obj.bind()
        assert service


def test_client_no_wsdl():
    with pytest.raises(ValueError):
        client.Client(None)


def test_client_cache_service():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    assert client_obj.service.GetLastTradePrice
    assert client_obj.service.GetLastTradePrice


def test_force_https():
    with open(yatest.common.test_source_path("wsdl_files/soap.wsdl")) as fh:
        response = fh.read()

    with requests_mock.mock() as m:
        url = "https://tests.python-zeep.org/wsdl"
        m.get(url, text=response, status_code=200)
        client_obj = client.Client(url)
        binding_options = client_obj.service._binding_options
        assert binding_options["address"].startswith("https")

        expected_url = "https://example.com/stockquote"
        assert binding_options["address"] == expected_url


@pytest.mark.requests
def test_create_service():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    service = client_obj.create_service(
        "{http://example.com/stockquote.wsdl}StockQuoteBinding",
        "http://test.python-zeep.org/x",
    )

    response = """
    <?xml version="1.0"?>
    <soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:stoc="http://example.com/stockquote.xsd">
       <soapenv:Header/>
       <soapenv:Body>
          <stoc:TradePrice>
             <price>120.123</price>
          </stoc:TradePrice>
       </soapenv:Body>
    </soapenv:Envelope>
    """.strip()

    with requests_mock.mock() as m:
        m.post("http://test.python-zeep.org/x", text=response)
        result = service.GetLastTradePrice("foobar")
        assert result == 120.123
        assert m.request_history[0].headers["User-Agent"].startswith("Zeep/")
        assert m.request_history[0].body.startswith(
            b"<?xml version='1.0' encoding='utf-8'?>"
        )


def test_create_message():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    data = client_obj.create_message(
        client_obj.service, "GetLastTradePrice", tickerSymbol="ZEEP"
    )
    assert data


def test_load_wsdl_with_file_prefix():
    cwd = yatest.common.test_source_path()
    client.Client("file://" + os.path.join(cwd, "wsdl_files/soap.wsdl"))


@pytest.mark.requests
def test_service_proxy():
    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))

    response = """
    <?xml version="1.0"?>
    <soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:stoc="http://example.com/stockquote.xsd">
       <soapenv:Header/>
       <soapenv:Body>
          <stoc:TradePrice>
             <price>120.123</price>
          </stoc:TradePrice>
       </soapenv:Body>
    </soapenv:Envelope>
    """.strip()

    with requests_mock.mock() as m:
        m.post("http://example.com/stockquote", text=response)
        result = client_obj.service.GetLastTradePrice("foobar")
        assert result == 120.123


@pytest.mark.requests
def test_call_method_fault():
    obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))

    response = """
        <?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope
            xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap:Body>
            <soap:Fault>
              <faultcode>soap:Server</faultcode>
              <faultstring>
                Big fatal error!!
              </faultstring>
              <faultactor>StockListByDate</faultactor>
              <detail>
                <Error xmlns="http://sherpa.sherpaan.nl/Sherpa">
                  <ErrorMessage>wrong security code</ErrorMessage>
                  <ErrorSource>StockListByDate</ErrorSource>
                </Error>
              </detail>
            </soap:Fault>
          </soap:Body>
        </soap:Envelope>
    """.strip()

    with requests_mock.mock() as m:
        m.post("http://example.com/stockquote", text=response, status_code=500)
        with pytest.raises(Error):
            obj.service.GetLastTradePrice(tickerSymbol="foobar")


@pytest.mark.requests
def test_default_soap_headers():
    header = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Element("{http://tests.python-zeep.org}name", xsd.String()),
                xsd.Element("{http://tests.python-zeep.org}password", xsd.String()),
            ]
        )
    )
    header_value = header(name="ik", password="foo")

    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    client_obj.set_default_soapheaders([header_value])

    response = """
    <?xml version="1.0"?>
    <soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:stoc="http://example.com/stockquote.xsd">
       <soapenv:Header/>
       <soapenv:Body>
          <stoc:TradePrice>
             <price>120.123</price>
          </stoc:TradePrice>
       </soapenv:Body>
    </soapenv:Envelope>
    """.strip()

    with requests_mock.mock() as m:
        m.post("http://example.com/stockquote", text=response)
        client_obj.service.GetLastTradePrice("foobar")

        doc = load_xml(m.request_history[0].body)
        header = doc.find("{http://schemas.xmlsoap.org/soap/envelope/}Header")
        assert header is not None
        assert len(list(header)) == 2


@pytest.mark.requests
def test_default_soap_headers_extra():
    header = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Element("{http://tests.python-zeep.org}name", xsd.String()),
                xsd.Element("{http://tests.python-zeep.org}password", xsd.String()),
            ]
        )
    )
    header_value = header(name="ik", password="geheim")

    extra_header = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Element("{http://tests.python-zeep.org}name", xsd.String()),
                xsd.Element("{http://tests.python-zeep.org}password", xsd.String()),
            ]
        )
    )
    extra_header_value = extra_header(name="ik", password="geheim")

    client_obj = client.Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    client_obj.set_default_soapheaders([header_value])

    response = """
    <?xml version="1.0"?>
    <soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:stoc="http://example.com/stockquote.xsd">
       <soapenv:Header/>
       <soapenv:Body>
          <stoc:TradePrice>
             <price>120.123</price>
          </stoc:TradePrice>
       </soapenv:Body>
    </soapenv:Envelope>
    """.strip()

    with requests_mock.mock() as m:
        m.post("http://example.com/stockquote", text=response)
        client_obj.service.GetLastTradePrice(
            "foobar", _soapheaders=[extra_header_value]
        )

        doc = load_xml(m.request_history[0].body)
        header = doc.find("{http://schemas.xmlsoap.org/soap/envelope/}Header")
        assert header is not None
        assert len(list(header)) == 4
