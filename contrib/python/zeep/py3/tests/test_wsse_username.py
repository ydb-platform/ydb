import datetime
import os

import pytest
import requests_mock
from freezegun import freeze_time

from tests.utils import assert_nodes_equal, load_xml
from zeep import client
from zeep.wsse import UsernameToken
from zeep.wsse.utils import WSU

import yatest.common as yc


@pytest.mark.requests
def test_integration():
    client_obj = client.Client(
        yc.test_source_path("wsdl_files/soap.wsdl"), wsse=UsernameToken("username", "password")
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
        m.post("http://example.com/stockquote", text=response)
        result = client_obj.service.GetLastTradePrice("foobar")
        assert result == 120.123


def test_password_text():
    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )

    token = UsernameToken("michael", "geheim")
    envelope, headers = token.apply(envelope, {})
    expected = """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header>
            <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
              <wsse:UsernameToken>
                <wsse:Username>michael</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">geheim</wsse:Password>
              </wsse:UsernameToken>
            </wsse:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """  # noqa
    assert_nodes_equal(envelope, expected)


@pytest.mark.network
@freeze_time("2016-05-08 12:00:00")
def test_password_digest(monkeypatch):
    monkeypatch.setattr(os, "urandom", lambda x: b"mocked-random")

    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )

    token = UsernameToken("michael", "geheim", use_digest=True)
    envelope, headers = token.apply(envelope, {})
    expected = """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header>
            <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
              <wsse:UsernameToken>
                <wsse:Username>michael</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">hVicspAQSg70JNhe67OHqD9gexc=</wsse:Password>
                <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">bW9ja2VkLXJhbmRvbQ==</wsse:Nonce>
                <wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">2016-05-08T12:00:00+00:00</wsu:Created>
              </wsse:UsernameToken>
            </wsse:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """  # noqa
    assert_nodes_equal(envelope, expected)


@pytest.mark.network
@freeze_time("2016-05-08 12:00:00")
def test_password_digest_custom(monkeypatch):
    monkeypatch.setattr(os, "urandom", lambda x: b"mocked-random")

    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )

    created = datetime.datetime(2016, 6, 4, 20, 10)
    token = UsernameToken(
        "michael",
        password_digest="12345",
        use_digest=True,
        nonce="iets",
        created=created,
    )
    envelope, headers = token.apply(envelope, {})
    expected = """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header>
            <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
              <wsse:UsernameToken>
                <wsse:Username>michael</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">12345</wsse:Password>
                <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">aWV0cw==</wsse:Nonce>
                <wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">2016-06-04T20:10:00+00:00</wsu:Created>
              </wsse:UsernameToken>
            </wsse:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """  # noqa
    assert_nodes_equal(envelope, expected)


def test_password_prepared():
    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Header xmlns:ns0="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <ns0:Security>
              <ns0:UsernameToken/>
            </ns0:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )  # noqa

    token = UsernameToken("michael", "geheim")
    envelope, headers = token.apply(envelope, {})
    expected = """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header xmlns:ns0="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <ns0:Security>
              <ns0:UsernameToken>
                <ns0:Username>michael</ns0:Username>
                <ns0:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">geheim</ns0:Password>
              </ns0:UsernameToken>
            </ns0:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """  # noqa
    assert_nodes_equal(envelope, expected)


def test_timestamp_token():
    envelope = load_xml(
        """
            <soap-env:Envelope
                xmlns:ns0="http://example.com/stockquote.xsd"
                xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
                xmlns:wsu="http://schemas.xmlsoap.org/ws/2003/06/utility"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            >
              <soap-env:Header xmlns:ns0="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <ns0:Security>
                  <ns0:UsernameToken/>
                </ns0:Security>
              </soap-env:Header>
              <soap-env:Body>
                <ns0:TradePriceRequest>
                  <tickerSymbol>foobar</tickerSymbol>
                  <ns0:country/>
                </ns0:TradePriceRequest>
              </soap-env:Body>
            </soap-env:Envelope>
        """
    )  # noqa

    timestamp_token = WSU.Timestamp()
    timestamp_token.attrib["Id"] = "id-21a27a50-9ebf-49cc-96bf-fcf7131e7858"
    some_date_obj = datetime.datetime(2018, 11, 18, 15, 44, 27, 440252)
    timestamp_elements = [
        WSU.Created(some_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")),
        WSU.Expires(
            (some_date_obj + datetime.timedelta(minutes=10)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        ),
    ]
    timestamp_token.extend(timestamp_elements)

    token = UsernameToken("Vishu", "Guntupalli", timestamp_token=timestamp_token)
    envelope, headers = token.apply(envelope, {})
    expected = """
            <soap-env:Envelope
                xmlns:ns0="http://example.com/stockquote.xsd"
                xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
                xmlns:wsu="http://schemas.xmlsoap.org/ws/2003/06/utility"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema">
              <soap-env:Header xmlns:ns0="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <ns0:Security>
                  <ns0:UsernameToken>
                    <ns0:Username>Vishu</ns0:Username>
                    <ns0:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">Guntupalli</ns0:Password>
                  </ns0:UsernameToken>
                  <wsu:Timestamp xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" Id="id-21a27a50-9ebf-49cc-96bf-fcf7131e7858">
                       <wsu:Created>2018-11-18T15:44:27Z</wsu:Created>
                       <wsu:Expires>2018-11-18T15:54:27Z</wsu:Expires>
                  </wsu:Timestamp>
                </ns0:Security>
              </soap-env:Header>
              <soap-env:Body>
                <ns0:TradePriceRequest>
                  <tickerSymbol>foobar</tickerSymbol>
                  <ns0:country/>
                </ns0:TradePriceRequest>
              </soap-env:Body>
            </soap-env:Envelope>
        """  # noqa
    assert_nodes_equal(envelope, expected)


@pytest.mark.network
@freeze_time("2016-05-08 12:00:00")
def test_bytes_like_password_digest(monkeypatch):
    monkeypatch.setattr(os, "urandom", lambda x: b"mocked-random")

    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )

    token = UsernameToken("michael", b"geheim", use_digest=True)
    envelope, headers = token.apply(envelope, {})
    expected = """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header>
            <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
              <wsse:UsernameToken>
                <wsse:Username>michael</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">hVicspAQSg70JNhe67OHqD9gexc=</wsse:Password>
                <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">bW9ja2VkLXJhbmRvbQ==</wsse:Nonce>
                <wsu:Created xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">2016-05-08T12:00:00+00:00</wsu:Created>
              </wsse:UsernameToken>
            </wsse:Security>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """  # noqa
    assert_nodes_equal(envelope, expected)
