import uuid
from io import StringIO

from pretend import stub

from tests.utils import DummyTransport, assert_nodes_equal
from zeep import Client, wsa, wsdl
from zeep.settings import Settings


def test_require_wsa(recwarn, monkeypatch):
    monkeypatch.setattr(
        uuid, "uuid4", lambda: uuid.UUID("ada686f9-5995-4088-bea4-239f694b2eaf")
    )

    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsaw="http://www.w3.org/2006/05/addressing/wsdl"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="dummyRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="dummyResponse">
            <wsdl:part name="response" element="tns:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="dummyRequest" wsaw:Action="urn:dummyRequest"/>
              <wsdl:output message="dummyResponse"  wsaw:Action="urn:dummyResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="TestService">
            <wsdl:documentation>Test service</wsdl:documentation>
            <wsdl:port name="TestPortType" binding="tns:TestBinding">
              <soap:address location="http://tests.python-zeep.org/test"/>
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """.strip()
    )

    client = stub(plugins=[], wsse=None)

    transport = DummyTransport()
    client = Client(wsdl_main, transport=transport)
    binding = client.wsdl.services.get("TestService").ports.get("TestPortType").binding

    envelope, headers = binding._create(
        "TestOperation1",
        args=["foo"],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )
    expected = """
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header  xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>urn:dummyRequest</wsa:Action>
            <wsa:MessageID>urn:uuid:ada686f9-5995-4088-bea4-239f694b2eaf</wsa:MessageID>
            <wsa:To>http://tests.python-zeep.org/test</wsa:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)


def test_duplicate_message_wsa(recwarn, monkeypatch):
    monkeypatch.setattr(
        uuid, "uuid4", lambda: uuid.UUID("ada686f9-5995-4088-bea4-239f694b2eaf")
    )

    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsaw="http://www.w3.org/2006/05/addressing/wsdl"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="dummyRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="dummyResponse">
            <wsdl:part name="response" element="tns:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="dummyRequest" wsaw:Action="urn:action1"/>
              <wsdl:output message="dummyResponse" wsaw:Action="urn:dummyResponse"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <wsdl:input message="dummyRequest" wsaw:Action="urn:action2"/>
              <wsdl:output message="dummyResponse" wsaw:Action="urn:dummyResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="TestService">
            <wsdl:documentation>Test service</wsdl:documentation>
            <wsdl:port name="TestPortType" binding="tns:TestBinding">
              <soap:address location="http://tests.python-zeep.org/test"/>
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """.strip()
    )

    client = stub(plugins=[], wsse=None)

    transport = DummyTransport()
    client = Client(wsdl_main, transport=transport)
    binding = client.wsdl.services.get("TestService").ports.get("TestPortType").binding

    envelope, headers = binding._create(
        "TestOperation1",
        args=["foo"],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )
    expected = """
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header  xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>urn:action1</wsa:Action>
            <wsa:MessageID>urn:uuid:ada686f9-5995-4088-bea4-239f694b2eaf</wsa:MessageID>
            <wsa:To>http://tests.python-zeep.org/test</wsa:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)


def test_force_wsa(recwarn, monkeypatch):
    monkeypatch.setattr(
        uuid, "uuid4", lambda: uuid.UUID("ada686f9-5995-4088-bea4-239f694b2eaf")
    )

    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="dummyRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="dummyResponse">
            <wsdl:part name="response" element="tns:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="dummyRequest"/>
              <wsdl:output message="dummyResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction="urn:dummyRequest"/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="TestService">
            <wsdl:documentation>Test service</wsdl:documentation>
            <wsdl:port name="TestPortType" binding="tns:TestBinding">
              <soap:address location="http://tests.python-zeep.org/test"/>
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """.strip()
    )

    transport = DummyTransport()
    client = Client(wsdl_main, transport=transport, plugins=[wsa.WsAddressingPlugin()])
    binding = client.wsdl.services.get("TestService").ports.get("TestPortType").binding

    envelope, headers = binding._create(
        "TestOperation1",
        args=["foo"],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )
    expected = """
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header  xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>urn:dummyRequest</wsa:Action>
            <wsa:MessageID>urn:uuid:ada686f9-5995-4088-bea4-239f694b2eaf</wsa:MessageID>
            <wsa:To>http://tests.python-zeep.org/test</wsa:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)


def test_force_wsa_soap12(recwarn, monkeypatch):
    monkeypatch.setattr(
        uuid, "uuid4", lambda: uuid.UUID("ada686f9-5995-4088-bea4-239f694b2eaf")
    )

    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap12/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap12/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="dummyRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="dummyResponse">
            <wsdl:part name="response" element="tns:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="dummyRequest"/>
              <wsdl:output message="dummyResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction="urn:dummyRequest"/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="TestService">
            <wsdl:documentation>Test service</wsdl:documentation>
            <wsdl:port name="TestPortType" binding="tns:TestBinding">
              <soap:address location="http://tests.python-zeep.org/test"/>
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """.strip()
    )

    client = stub(plugins=[wsa.WsAddressingPlugin()], wsse=None, settings=Settings())

    transport = DummyTransport()
    doc = wsdl.Document(wsdl_main, transport, settings=client.settings)
    binding = doc.services.get("TestService").ports.get("TestPortType").binding

    envelope, headers = binding._create(
        "TestOperation1",
        args=["foo"],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )
    expected = """
        <soap-env:Envelope
            xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope">
          <soap-env:Header  xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>urn:dummyRequest</wsa:Action>
            <wsa:MessageID>urn:uuid:ada686f9-5995-4088-bea4-239f694b2eaf</wsa:MessageID>
            <wsa:To>http://tests.python-zeep.org/test</wsa:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>

          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)

    assert headers["Content-Type"] == (
        'application/soap+xml; charset=utf-8; action="urn:dummyRequest"'
    )


def test_require_wsa_new(recwarn, monkeypatch):
    monkeypatch.setattr(
        uuid, "uuid4", lambda: uuid.UUID("ada686f9-5995-4088-bea4-239f694b2eaf")
    )

    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsam="http://www.w3.org/2007/05/addressing/metadata"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="dummyRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="dummyResponse">
            <wsdl:part name="response" element="tns:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="dummyRequest" wsam:Action="urn:dummyRequest"/>
              <wsdl:output message="dummyResponse"  wsam:Action="urn:dummyResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal"/>
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="TestService">
            <wsdl:documentation>Test service</wsdl:documentation>
            <wsdl:port name="TestPortType" binding="tns:TestBinding">
              <soap:address location="http://tests.python-zeep.org/test"/>
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """.strip()
    )

    client = stub(plugins=[], wsse=None)

    transport = DummyTransport()
    client = Client(wsdl_main, transport=transport)
    binding = client.wsdl.services.get("TestService").ports.get("TestPortType").binding

    envelope, headers = binding._create(
        "TestOperation1",
        args=["foo"],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )
    expected = """
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header  xmlns:wsa="http://www.w3.org/2005/08/addressing">
            <wsa:Action>urn:dummyRequest</wsa:Action>
            <wsa:MessageID>urn:uuid:ada686f9-5995-4088-bea4-239f694b2eaf</wsa:MessageID>
            <wsa:To>http://tests.python-zeep.org/test</wsa:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)
