import platform

import pytest
from lxml import etree
from pretend import stub

from tests.utils import load_xml
from zeep import Client
from zeep.exceptions import Fault, TransportError
from zeep.wsdl import bindings

import yatest.common as yc


def test_soap11_no_output():
    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"))
    content = """
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:stoc="http://example.com/stockquote.xsd">
          <soapenv:Body></soapenv:Body>
        </soapenv:Envelope>
    """.strip()
    response = stub(status_code=200, headers={}, content=content)

    operation = client.service._binding._operations["GetLastTradePriceNoOutput"]
    res = client.service._binding.process_reply(client, operation, response)
    assert res is None


def test_soap11_process_error():
    response = load_xml(
        """
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:stoc="http://example.com/stockquote.xsd">
          <soapenv:Body>
            <soapenv:Fault>
              <faultcode>fault-code</faultcode>
              <faultstring>fault-string</faultstring>
              <detail>
                <e:myFaultDetails xmlns:e="http://myexample.org/faults">
                  <e:message>detail-message</e:message>
                  <e:errorcode>detail-code</e:errorcode>
                </e:myFaultDetails>
              </detail>
            </soapenv:Fault>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    binding = bindings.Soap11Binding(
        wsdl=None, name=None, port_name=None, transport=None, default_style=None
    )
    try:
        binding.process_error(response, None)
        assert False
    except Fault as exc:
        assert exc.message == "fault-string"
        assert exc.code == "fault-code"
        assert exc.actor is None
        assert exc.subcodes is None
        assert "detail-message" in etree.tostring(exc.detail).decode("utf-8")

    responseWithNamespaceInFault = load_xml(
        """
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:stoc="http://example.com/stockquote.xsd">
          <soapenv:Body>
            <soapenv:Fault xmlns="http://schemas.xmlsoap.org/soap/envelope/">
              <faultcode>fault-code-withNamespace</faultcode>
              <faultstring>fault-string-withNamespace</faultstring>
              <detail>
                <e:myFaultDetails xmlns:e="http://myexample.org/faults">
                  <e:message>detail-message-withNamespace</e:message>
                  <e:errorcode>detail-code-withNamespace</e:errorcode>
                </e:myFaultDetails>
              </detail>
            </soapenv:Fault>
          </soapenv:Body>
        </soapenv:Envelope>
    """
    )

    try:
        binding.process_error(responseWithNamespaceInFault, None)
        assert False
    except Fault as exc:
        assert exc.message == "fault-string-withNamespace"
        assert exc.code == "fault-code-withNamespace"
        assert exc.actor is None
        assert exc.subcodes is None
        assert "detail-message-withNamespace" in etree.tostring(exc.detail).decode(
            "utf-8"
        )


def test_soap12_process_error():
    response = """
        <soapenv:Envelope
            xmlns="http://example.com/example1"
            xmlns:ex="http://example.com/example2"
            xmlns:soapenv="http://www.w3.org/2003/05/soap-envelope">
          <soapenv:Body>
            <soapenv:Fault>
             <soapenv:Code>
               <soapenv:Value>fault-code</soapenv:Value>
               %s
             </soapenv:Code>
             <soapenv:Reason>
              <soapenv:Text xml:lang="en-US">us-error</soapenv:Text>
              <soapenv:Text xml:lang="nl-NL">nl-error</soapenv:Text>
             </soapenv:Reason>
             <soapenv:Detail>
              <e:myFaultDetails
                xmlns:e="http://myexample.org/faults" >
                <e:message>Invalid credit card details</e:message>
                <e:errorcode>999</e:errorcode>
              </e:myFaultDetails>
             </soapenv:Detail>
           </soapenv:Fault>
         </soapenv:Body>
        </soapenv:Envelope>
    """
    subcode = """
               <soapenv:Subcode>
                 <soapenv:Value>%s</soapenv:Value>
                 %s
               </soapenv:Subcode>
    """
    binding = bindings.Soap12Binding(
        wsdl=None, name=None, port_name=None, transport=None, default_style=None
    )

    try:
        binding.process_error(load_xml(response % ""), None)
        assert False
    except Fault as exc:
        assert exc.message == "us-error"
        assert exc.code == "fault-code"
        assert exc.subcodes == []

    try:
        binding.process_error(
            load_xml(response % subcode % ("fault-subcode1", "")), None
        )
        assert False
    except Fault as exc:
        assert exc.message == "us-error"
        assert exc.code == "fault-code"
        assert len(exc.subcodes) == 1
        assert exc.subcodes[0].namespace == "http://example.com/example1"
        assert exc.subcodes[0].localname == "fault-subcode1"

    try:
        binding.process_error(
            load_xml(
                response
                % subcode
                % ("fault-subcode1", subcode % ("ex:fault-subcode2", ""))
            ),
            None,
        )
        assert False
    except Fault as exc:
        assert exc.message == "us-error"
        assert exc.code == "fault-code"
        assert len(exc.subcodes) == 2
        assert exc.subcodes[0].namespace == "http://example.com/example1"
        assert exc.subcodes[0].localname == "fault-subcode1"
        assert exc.subcodes[1].namespace == "http://example.com/example2"
        assert exc.subcodes[1].localname == "fault-subcode2"


def test_no_content_type():
    data = """
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

    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"))
    binding = client.service._binding

    response = stub(status_code=200, content=data, encoding="utf-8", headers={})

    result = binding.process_reply(client, binding.get("GetLastTradePrice"), response)

    assert result == 120.123


@pytest.mark.skipif(platform.python_implementation() == "PyPy", reason="Fails on PyPy")
def test_wrong_content():
    data = """
        The request is answered something unexpected,
        like an html page or a raw internal stack trace
    """.strip()

    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"))
    binding = client.service._binding

    response = stub(status_code=200, content=data, encoding="utf-8", headers={})

    with pytest.raises(TransportError) as exc:
        binding.process_reply(client, binding.get("GetLastTradePrice"), response)
    assert 200 == exc.value.status_code
    assert data == exc.value.content


@pytest.mark.skipif(platform.python_implementation() == "PyPy", reason="Fails on PyPy")
def test_wrong_no_unicode_content():
    data = """
        The request is answered something unexpected,
        and the content charset is beyond unicode òñÇÿ
    """.strip()

    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"))
    binding = client.service._binding

    response = stub(status_code=200, content=data, encoding="utf-8", headers={})

    with pytest.raises(TransportError) as exc:
        binding.process_reply(client, binding.get("GetLastTradePrice"), response)

    assert 200 == exc.value.status_code
    assert data == exc.value.content


@pytest.mark.skipif(platform.python_implementation() == "PyPy", reason="Fails on PyPy")
def test_http_error():
    data = """
        Unauthorized!
    """.strip()

    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"))
    binding = client.service._binding

    response = stub(status_code=401, content=data, encoding="utf-8", headers={})

    with pytest.raises(TransportError) as exc:
        binding.process_reply(client, binding.get("GetLastTradePrice"), response)
    assert 401 == exc.value.status_code
    assert data == exc.value.content


def test_mime_multipart():
    data = "\r\n".join(
        line.strip()
        for line in """
        --MIME_boundary
        Content-Type: text/xml; charset=UTF-8
        Content-Transfer-Encoding: 8bit
        Content-ID: <claim061400a.xml@claiming-it.com>

        <?xml version='1.0' ?>
        <SOAP-ENV:Envelope
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
        <SOAP-ENV:Body>
        <claim:insurance_claim_auto id="insurance_claim_document_id"
        xmlns:claim="http://schemas.risky-stuff.com/Auto-Claim">
        <theSignedForm href="cid:claim061400a.tiff@claiming-it.com"/>
        <theCrashPhoto href="cid:claim061400a.jpeg@claiming-it.com"/>
        <!-- ... more claim details go here... -->
        </claim:insurance_claim_auto>
        </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>

        --MIME_boundary
        Content-Type: image/tiff
        Content-Transfer-Encoding: base64
        Content-ID: <claim061400a.tiff@claiming-it.com>

        Li4uQmFzZTY0IGVuY29kZWQgVElGRiBpbWFnZS4uLg==

        --MIME_boundary
        Content-Type: image/jpeg
        Content-Transfer-Encoding: binary
        Content-ID: <claim061400a.jpeg@claiming-it.com>

        ...Raw JPEG image..
        --MIME_boundary--
    """.splitlines()
    ).encode("utf-8")

    client = Client(yc.test_source_path("wsdl_files/claim.wsdl"))
    binding = client.service._binding

    response = stub(
        status_code=200,
        content=data,
        encoding="utf-8",
        headers={
            "Content-Type": 'multipart/related; type="text/xml"; start="<claim061400a.xml@claiming-it.com>"; boundary="MIME_boundary"'
        },
    )

    result = binding.process_reply(client, binding.get("GetClaimDetails"), response)

    assert result.root is None
    assert len(result.attachments) == 2

    assert result.attachments[0].content == b"...Base64 encoded TIFF image..."
    assert result.attachments[1].content == b"...Raw JPEG image.."


def test_mime_multipart_no_encoding():
    data = "\r\n".join(
        line.strip()
        for line in """
        --MIME_boundary
        Content-Type: text/xml
        Content-Transfer-Encoding: 8bit
        Content-ID: <claim061400a.xml@claiming-it.com>

        <?xml version='1.0' ?>
        <SOAP-ENV:Envelope
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
        <SOAP-ENV:Body>
        <claim:insurance_claim_auto id="insurance_claim_document_id"
        xmlns:claim="http://schemas.risky-stuff.com/Auto-Claim">
        <theSignedForm href="cid:claim061400a.tiff@claiming-it.com"/>
        <theCrashPhoto href="cid:claim061400a.jpeg@claiming-it.com"/>
        <!-- ... more claim details go here... -->
        </claim:insurance_claim_auto>
        </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>

        --MIME_boundary
        Content-Type: image/tiff
        Content-Transfer-Encoding: base64
        Content-ID: <claim061400a.tiff@claiming-it.com>

        Li4uQmFzZTY0IGVuY29kZWQgVElGRiBpbWFnZS4uLg==

        --MIME_boundary
        Content-Type: text/xml
        Content-ID: <claim061400a.jpeg@claiming-it.com>

        ...Raw JPEG image..
        --MIME_boundary--
    """.splitlines()
    ).encode("utf-8")

    client = Client(yc.test_source_path("wsdl_files/claim.wsdl"))
    binding = client.service._binding

    response = stub(
        status_code=200,
        content=data,
        encoding=None,
        headers={
            "Content-Type": 'multipart/related; type="text/xml"; start="<claim061400a.xml@claiming-it.com>"; boundary="MIME_boundary"'
        },
    )

    result = binding.process_reply(client, binding.get("GetClaimDetails"), response)

    assert result.root is None
    assert len(result.attachments) == 2

    assert result.attachments[0].content == b"...Base64 encoded TIFF image..."
    assert result.attachments[1].content == b"...Raw JPEG image.."


def test_unexpected_headers():
    data = """
        <?xml version="1.0"?>
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:stoc="http://example.com/stockquote.xsd">
           <soapenv:Header>
             <stoc:IamUnexpected>uhoh</stoc:IamUnexpected>
           </soapenv:Header>
           <soapenv:Body>
              <stoc:TradePrice>
                 <price>120.123</price>
              </stoc:TradePrice>
           </soapenv:Body>
        </soapenv:Envelope>
    """.strip()

    client = Client(yc.test_source_path("wsdl_files/soap_header.wsdl"))
    binding = client.service._binding

    response = stub(status_code=200, content=data, encoding="utf-8", headers={})

    result = binding.process_reply(client, binding.get("GetLastTradePrice"), response)

    assert result.body.price == 120.123
    assert result.header.body is None
    assert len(result.header._raw_elements) == 1


def test_response_201():
    client = Client(yc.test_source_path("wsdl_files/soap_header.wsdl"))
    binding = client.service._binding

    response = stub(status_code=201, content="", encoding="utf-8", headers={})

    result = binding.process_reply(client, binding.get("GetLastTradePrice"), response)
    assert result is None


def test_response_202():
    client = Client(yc.test_source_path("wsdl_files/soap_header.wsdl"))
    binding = client.service._binding

    response = stub(status_code=202, content="", encoding="utf-8", headers={})

    result = binding.process_reply(client, binding.get("GetLastTradePrice"), response)
    assert result is None
