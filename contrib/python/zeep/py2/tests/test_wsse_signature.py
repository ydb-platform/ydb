import os
import sys

import pytest
from lxml import etree
from lxml.etree import QName

from tests.utils import load_xml
from zeep import ns, wsse
from zeep.exceptions import SignatureVerificationFailed
from zeep.wsse import signature
from zeep.wsse.signature import xmlsec as xmlsec_installed

import yatest.common
KEY_FILE = yatest.common.test_source_path("cert_valid.pem")
KEY_FILE_PW = yatest.common.test_source_path("cert_valid_pw.pem")

SIGNATURE_METHODS_TESTDATA = (
    ("RSA_SHA1", "http://www.w3.org/2000/09/xmldsig#rsa-sha1"),
    ("RSA_SHA256", "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"),
)

DIGEST_METHODS_TESTDATA = (
    ("SHA1", "http://www.w3.org/2000/09/xmldsig#sha1"),
    ("SHA256", "http://www.w3.org/2001/04/xmlenc#sha256"),
)


skip_if_no_xmlsec = pytest.mark.skipif(
    sys.platform == "win32", reason="does not run on windows"
) and pytest.mark.skipif(
    xmlsec_installed is None, reason="xmlsec library not installed"
)


@skip_if_no_xmlsec
def test_sign_timestamp_if_present():
    envelope = load_xml(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:wsu="http://schemas.xmlsoap.org/ws/2003/06/utility"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap-env:Header xmlns:ns0="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
            <ns0:Security>
              <wsu:Timestamp xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
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
    """
    )

    signature.sign_envelope(envelope, KEY_FILE, KEY_FILE)
    signature.verify_envelope(envelope, KEY_FILE)


@skip_if_no_xmlsec
@pytest.mark.parametrize("digest_method,expected_digest_href", DIGEST_METHODS_TESTDATA)
@pytest.mark.parametrize(
    "signature_method,expected_signature_href", SIGNATURE_METHODS_TESTDATA
)
def test_sign(
    digest_method, signature_method, expected_digest_href, expected_signature_href
):
    envelope = load_xml(
        """
        <soapenv:Envelope
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">
          <soapenv:Header></soapenv:Header>
          <soapenv:Body>
            <tns:Function>
              <tns:Argument>OK</tns:Argument>
            </tns:Function>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    signature.sign_envelope(
        envelope,
        KEY_FILE,
        KEY_FILE,
        signature_method=getattr(xmlsec_installed.Transform, signature_method),
        digest_method=getattr(xmlsec_installed.Transform, digest_method),
    )
    signature.verify_envelope(envelope, KEY_FILE)

    digests = envelope.xpath("//ds:DigestMethod", namespaces={"ds": ns.DS})
    assert len(digests)
    for digest in digests:
        assert digest.get("Algorithm") == expected_digest_href
    signatures = envelope.xpath("//ds:SignatureMethod", namespaces={"ds": ns.DS})
    assert len(signatures)
    for sig in signatures:
        assert sig.get("Algorithm") == expected_signature_href


@skip_if_no_xmlsec
def test_sign_pw():
    envelope = load_xml(
        """
        <soapenv:Envelope
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">
          <soapenv:Header></soapenv:Header>
          <soapenv:Body>
            <tns:Function>
              <tns:Argument>OK</tns:Argument>
            </tns:Function>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    signature.sign_envelope(envelope, KEY_FILE_PW, KEY_FILE_PW, "geheim")
    signature.verify_envelope(envelope, KEY_FILE_PW)


@skip_if_no_xmlsec
def test_verify_error():
    envelope = load_xml(
        """
        <soapenv:Envelope
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">
          <soapenv:Header></soapenv:Header>
          <soapenv:Body>
            <tns:Function>
              <tns:Argument>OK</tns:Argument>
            </tns:Function>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    signature.sign_envelope(envelope, KEY_FILE, KEY_FILE)
    nsmap = {"tns": "http://tests.python-zeep.org/"}

    for elm in envelope.xpath("//tns:Argument", namespaces=nsmap):
        elm.text = "NOT!"

    with pytest.raises(SignatureVerificationFailed):
        signature.verify_envelope(envelope, KEY_FILE)


@skip_if_no_xmlsec
def test_signature():
    envelope = load_xml(
        """
        <soapenv:Envelope
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">
          <soapenv:Header></soapenv:Header>
          <soapenv:Body>
            <tns:Function>
              <tns:Argument>OK</tns:Argument>
            </tns:Function>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    plugin = wsse.Signature(KEY_FILE_PW, KEY_FILE_PW, "geheim")
    envelope, headers = plugin.apply(envelope, {})
    plugin.verify(envelope)


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
@pytest.mark.parametrize("digest_method,expected_digest_href", DIGEST_METHODS_TESTDATA)
@pytest.mark.parametrize(
    "signature_method,expected_signature_href", SIGNATURE_METHODS_TESTDATA
)
@skip_if_no_xmlsec
def test_signature_binary(
    digest_method, signature_method, expected_digest_href, expected_signature_href
):
    envelope = load_xml(
        """
        <soapenv:Envelope
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">
          <soapenv:Header></soapenv:Header>
          <soapenv:Body>
            <tns:Function>
              <tns:Argument>OK</tns:Argument>
            </tns:Function>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    plugin = wsse.BinarySignature(
        KEY_FILE_PW,
        KEY_FILE_PW,
        "geheim",
        signature_method=getattr(xmlsec_installed.Transform, signature_method),
        digest_method=getattr(xmlsec_installed.Transform, digest_method),
    )
    envelope, headers = plugin.apply(envelope, {})
    plugin.verify(envelope)
    # Test the reference
    bintok = envelope.xpath(
        "soapenv:Header/wsse:Security/wsse:BinarySecurityToken",
        namespaces={"soapenv": ns.SOAP_ENV_11, "wsse": ns.WSSE},
    )[0]
    ref = envelope.xpath(
        "soapenv:Header/wsse:Security/ds:Signature/ds:KeyInfo/wsse:SecurityTokenReference"
        "/wsse:Reference",
        namespaces={"soapenv": ns.SOAP_ENV_11, "wsse": ns.WSSE, "ds": ns.DS},
    )[0]
    assert "#" + bintok.attrib[QName(ns.WSU, "Id")] == ref.attrib["URI"]

    digests = envelope.xpath("//ds:DigestMethod", namespaces={"ds": ns.DS})
    assert len(digests)
    for digest in digests:
        assert digest.get("Algorithm") == expected_digest_href
    signatures = envelope.xpath("//ds:SignatureMethod", namespaces={"ds": ns.DS})
    assert len(signatures)
    for sig in signatures:
        assert sig.get("Algorithm") == expected_signature_href
