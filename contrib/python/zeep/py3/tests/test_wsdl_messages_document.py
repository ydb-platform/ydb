from io import StringIO

from lxml import etree

from tests.utils import assert_nodes_equal, load_xml
from zeep import xsd
from zeep.wsdl import wsdl


def test_parse():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns">
          <xsd:element name="Request" type="xsd:string"/>
          <xsd:element name="Response" type="xsd:string"/>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>
      <message name="Output">
        <part element="tns:Response"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert (
        operation.input.body.signature(schema=root.types) == "ns0:Request(xsd:string)"
    )
    assert operation.input.header.signature(schema=root.types) == "soap-env:Header()"
    assert (
        operation.input.envelope.signature(schema=root.types)
        == "soap-env:envelope(body: xsd:string)"
    )
    assert operation.input.signature(as_output=False) == "xsd:string"

    assert (
        operation.output.body.signature(schema=root.types) == "ns0:Response(xsd:string)"
    )
    assert operation.output.header.signature(schema=root.types) == "soap-env:Header()"
    assert (
        operation.output.envelope.signature(schema=root.types)
        == "soap-env:envelope(body: xsd:string)"
    )
    assert operation.output.signature(as_output=True) == "xsd:string"


def test_parse_with_header():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns">
          <xsd:element name="Request" type="xsd:string"/>
          <xsd:element name="RequestHeader" type="xsd:string"/>
          <xsd:element name="Response" type="xsd:string"/>
          <xsd:element name="ResponseHeader" type="xsd:string"/>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
        <part name="auth" element="tns:RequestHeader"/>
      </message>
      <message name="Output">
        <part element="tns:Response"/>
        <part name="auth" element="tns:ResponseHeader"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:header message="tns:Input" part="auth" use="literal" />
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:header message="tns:Output" part="auth" use="literal" />
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert (
        operation.input.body.signature(schema=root.types) == "ns0:Request(xsd:string)"
    )
    assert (
        operation.input.header.signature(schema=root.types)
        == "soap-env:Header(auth: xsd:string)"
    )
    assert (
        operation.input.envelope.signature(schema=root.types)
        == "soap-env:envelope(header: {auth: xsd:string}, body: xsd:string)"
    )  # noqa
    assert (
        operation.input.signature(as_output=False)
        == "xsd:string, _soapheaders={auth: xsd:string}"
    )  # noqa

    assert (
        operation.output.body.signature(schema=root.types) == "ns0:Response(xsd:string)"
    )
    assert (
        operation.output.header.signature(schema=root.types)
        == "soap-env:Header(auth: xsd:string)"
    )
    assert (
        operation.output.envelope.signature(schema=root.types)
        == "soap-env:envelope(header: {auth: xsd:string}, body: xsd:string)"
    )  # noqa
    assert (
        operation.output.signature(as_output=True)
        == "header: {auth: xsd:string}, body: xsd:string"
    )  # noqa


def test_parse_with_header_type():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns">
          <xsd:element name="Request" type="xsd:string"/>
          <xsd:simpleType name="RequestHeaderType">
            <xsd:restriction base="xsd:string"/>
          </xsd:simpleType>
          <xsd:element name="Response" type="xsd:string"/>
          <xsd:simpleType name="ResponseHeaderType">
            <xsd:restriction base="xsd:string"/>
          </xsd:simpleType>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
        <part name="auth" type="tns:RequestHeaderType"/>
      </message>
      <message name="Output">
        <part element="tns:Response"/>
        <part name="auth" type="tns:ResponseHeaderType"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:header message="tns:Input" part="auth" use="literal" />
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:header message="tns:Output" part="auth" use="literal" />
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert (
        operation.input.body.signature(schema=root.types) == "ns0:Request(xsd:string)"
    )
    assert (
        operation.input.header.signature(schema=root.types)
        == "soap-env:Header(auth: ns0:RequestHeaderType)"
    )
    assert (
        operation.input.envelope.signature(schema=root.types)
        == "soap-env:envelope(header: {auth: ns0:RequestHeaderType}, body: xsd:string)"
    )  # noqa
    assert (
        operation.input.signature(as_output=False)
        == "xsd:string, _soapheaders={auth: ns0:RequestHeaderType}"
    )  # noqa

    assert (
        operation.output.body.signature(schema=root.types) == "ns0:Response(xsd:string)"
    )
    assert (
        operation.output.header.signature(schema=root.types)
        == "soap-env:Header(auth: ns0:ResponseHeaderType)"
    )
    assert (
        operation.output.envelope.signature(schema=root.types)
        == "soap-env:envelope(header: {auth: ns0:ResponseHeaderType}, body: xsd:string)"
    )  # noqa
    assert (
        operation.output.signature(as_output=True)
        == "header: {auth: ns0:ResponseHeaderType}, body: xsd:string"
    )  # noqa


def test_parse_with_header_other_message():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns">
          <xsd:element name="Request" type="xsd:string"/>
          <xsd:element name="RequestHeader" type="xsd:string"/>
        </xsd:schema>
      </types>

      <message name="InputHeader">
        <part name="header" element="tns:RequestHeader"/>
      </message>
      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:header message="tns:InputHeader" part="header" use="literal" />
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)
    root.types.set_ns_prefix("soap-env", "http://schemas.xmlsoap.org/soap/envelope/")

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert (
        operation.input.header.signature(schema=root.types)
        == "soap-env:Header(header: xsd:string)"
    )
    assert (
        operation.input.body.signature(schema=root.types) == "ns0:Request(xsd:string)"
    )

    header = root.types.get_element("{http://tests.python-zeep.org/tns}RequestHeader")(
        "foo"
    )
    serialized = operation.input.serialize("ah1", _soapheaders={"header": header})
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:RequestHeader xmlns:ns0="http://tests.python-zeep.org/tns">foo</ns0:RequestHeader>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">ah1</ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serialize():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    serialized = operation.input.serialize(arg1="ah1", arg2="ah2")
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serialize_multiple_parts():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part name="request1" element="tns:Request"/>
        <part name="request2" element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    serialized = operation.input.serialize(
        request1={"arg1": "ah1", "arg2": "ah2"}, request2={"arg1": "ah1", "arg2": "ah2"}
    )
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
            <ns0:arg1>ah1</ns0:arg1>
            <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
            <ns1:Request xmlns:ns1="http://tests.python-zeep.org/tns">
            <ns1:arg1>ah1</ns1:arg1>
            <ns1:arg2>ah2</ns1:arg2>
            </ns1:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serialize_with_header():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
          <xsd:element name="Authentication">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="username" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
        <part element="tns:Authentication" name="auth"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
            <soap:header message="tns:Input" part="auth" use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    AuthHeader = root.types.get_element(
        "{http://tests.python-zeep.org/tns}Authentication"
    )
    auth_header = AuthHeader(username="mvantellingen")

    serialized = operation.input.serialize(
        arg1="ah1", arg2="ah2", _soapheaders=[auth_header]
    )
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:Authentication xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:username>mvantellingen</ns0:username>
            </ns0:Authentication>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serialize_with_headers_simple():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
          <xsd:element name="Authentication">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="username" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
        <part element="tns:Authentication" name="Header"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
            <soap:header message="tns:Input" part="Header" use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    header = xsd.ComplexType(
        xsd.Sequence(
            [
                xsd.Element(
                    "{http://www.w3.org/2005/08/addressing}Action", xsd.String()
                ),
                xsd.Element("{http://www.w3.org/2005/08/addressing}To", xsd.String()),
            ]
        )
    )
    header_value = header(Action="doehet", To="server")
    serialized = operation.input.serialize(
        arg1="ah1", arg2="ah2", _soapheaders=[header_value]
    )
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:Action xmlns:ns0="http://www.w3.org/2005/08/addressing">doehet</ns0:Action>
            <ns1:To xmlns:ns1="http://www.w3.org/2005/08/addressing">server</ns1:To>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serialize_with_header_and_custom_mixed():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
          <xsd:element name="Authentication">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="username" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
        <part element="tns:Authentication" name="Header"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
            <soap:header message="tns:Input" part="Header" use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    header = root.types.get_element("{http://tests.python-zeep.org/tns}Authentication")
    header_1 = header(username="mvantellingen")

    header = xsd.Element(
        "{http://test.python-zeep.org/custom}custom",
        xsd.ComplexType(
            [xsd.Element("{http://test.python-zeep.org/custom}foo", xsd.String())]
        ),
    )
    header_2 = header(foo="bar")

    serialized = operation.input.serialize(
        arg1="ah1", arg2="ah2", _soapheaders=[header_1, header_2]
    )
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:Authentication xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:username>mvantellingen</ns0:username>
            </ns0:Authentication>
            <ns1:custom xmlns:ns1="http://test.python-zeep.org/custom">
              <ns1:foo>bar</ns1:foo>
            </ns1:custom>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serializer_with_header_custom_elm():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    header = xsd.Element(
        "{http://test.python-zeep.org/custom}auth",
        xsd.ComplexType(
            [xsd.Element("{http://test.python-zeep.org/custom}username", xsd.String())]
        ),
    )

    serialized = operation.input.serialize(
        arg1="ah1", arg2="ah2", _soapheaders=[header(username="mvantellingen")]
    )

    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:auth xmlns:ns0="http://test.python-zeep.org/custom">
              <ns0:username>mvantellingen</ns0:username>
            </ns0:auth>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_serializer_with_header_custom_xml():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)
    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    header_value = etree.Element("{http://test.python-zeep.org/custom}auth")
    etree.SubElement(
        header_value, "{http://test.python-zeep.org/custom}username"
    ).text = "mvantellingen"

    serialized = operation.input.serialize(
        arg1="ah1", arg2="ah2", _soapheaders=[header_value]
    )

    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:auth xmlns:ns0="http://test.python-zeep.org/custom">
              <ns0:username>mvantellingen</ns0:username>
            </ns0:auth>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)


def test_deserialize():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>
      <message name="Output">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    response_body = load_xml(
        """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:ns0="http://tests.python-zeep.org/tns"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request>
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )
    result = operation.process_reply(response_body)
    assert result.arg1 == "ah1"
    assert result.arg2 == "ah2"


def test_deserialize_no_content():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request" type="xsd:string"/>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>
      <message name="Output">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    response_body = load_xml(
        """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:ns0="http://tests.python-zeep.org/tns"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request/>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )
    result = operation.process_reply(response_body)
    assert result is None


def test_deserialize_choice():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:choice>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:choice>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>
      <message name="Output">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    response_body = load_xml(
        """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:ns0="http://tests.python-zeep.org/tns"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request>
              <ns0:arg1>ah1</ns0:arg1>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )
    result = operation.process_reply(response_body)
    assert result.arg1 == "ah1"


def test_deserialize_one_part():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <message name="Output">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    response_body = load_xml(
        """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:ns0="http://tests.python-zeep.org/tns"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:auth xmlns:ns0="http://test.python-zeep.org/custom">
              <ns0:username>mvantellingen</ns0:username>
            </ns0:auth>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request>
              <ns0:arg1>ah1</ns0:arg1>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )  # noqa

    serialized = operation.process_reply(response_body)
    assert serialized.arg1 == "ah1"
    assert serialized.arg2 == "ah2"


def test_deserialize_with_headers():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request1">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>

          <xsd:element name="Request2">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>

          <xsd:element name="Header1">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="username" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
          <xsd:element name="Header2" type="xsd:string"/>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request1"/>
      </message>

      <message name="Output">
        <part element="tns:Request1" name="request_1"/>
        <part element="tns:Request2" name="request_2"/>
        <part element="tns:Header1" name="header_1"/>
        <part element="tns:Header2" name="header_2"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
          <output>
            <soap:body use="literal"/>
            <soap:header message="tns:Output" part="header_1" use="literal">
              <soap:headerfault message="tns:OutputFault"
                    part="header_1_fault" use="literal"/>
            </soap:header>
            <soap:header message="tns:Output" part="header_2" use="literal"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    response_body = load_xml(
        """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:ns0="http://tests.python-zeep.org/tns"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Header>
            <ns0:Header1>
              <ns0:username>mvantellingen</ns0:username>
            </ns0:Header1>
            <ns0:Header2>foo</ns0:Header2>
          </soap-env:Header>
          <soap-env:Body>
            <ns0:Request1>
              <ns0:arg1>ah1</ns0:arg1>
            </ns0:Request1>
            <ns0:Request2>
              <ns0:arg2>ah2</ns0:arg2>
            </ns0:Request2>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    )  # noqa

    serialized = operation.process_reply(response_body)

    assert operation.output.signature(as_output=True) == (
        "header: {header_1: ns0:Header1, header_2: xsd:string}, body: {request_1: ns0:Request1, request_2: ns0:Request2}"
    )
    assert serialized.body.request_1.arg1 == "ah1"
    assert serialized.body.request_2.arg2 == "ah2"
    assert serialized.header.header_1.username == "mvantellingen"


def test_deserialize_part_no_element():
    wsdl_content = StringIO(
        """
    <wsdl:definitions xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" 
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" 
        xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
        targetNamespace="http://tests.python-zeep.org/tns"
        xmlns:tns="http://tests.python-zeep.org/tns">
        <wsdl:types>
            <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                targetNamespace="http://tests.python-zeep.org/tns"
                elementFormDefault="qualified"
                attributeFormDefault="qualified">
                <xsd:element name="RequestElement">
                    <xsd:complexType>
                        <xsd:sequence>
                            <xsd:element name="id" type="xsd:normalizedString" />
                        </xsd:sequence>
                    </xsd:complexType>
                </xsd:element>
            </xsd:schema>
        </wsdl:types>
        <wsdl:message name="Request">
            <wsdl:part name="parameters" element="tns:RequestElement" />
        </wsdl:message>
        <wsdl:message name="Response">
            <wsdl:part name="text" type="xsd:string" />
        </wsdl:message>
        <wsdl:portType name="Port1">
            <wsdl:operation name="Operation1">
                <wsdl:input message="Request" />
                <wsdl:output message="Response" />
            </wsdl:operation>
        </wsdl:portType>
        <wsdl:binding name="Binding" type="Port1">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="Operation1">
                <soap:operation soapAction="http://tests.python-zeep.org/tns/Request"/>
                <wsdl:input>
                    <soap:body use="literal"/>
                </wsdl:input>
                <wsdl:output>
                    <soap:body use="literal"/>
                </wsdl:output>
            </wsdl:operation>
        </wsdl:binding>
    </wsdl:definitions>
    """
    )
    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}Binding"]
    operation = binding.get("Operation1")

    response = load_xml(
        """
    <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
        <s:Header />
        <s:Body>
            <text>Some kind of interesting text</text>
        </s:Body>
    </s:Envelope>
    """
    )

    serialized = operation.process_reply(response)
    assert serialized == "Some kind of interesting text"


def test_serialize_any_type():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/tns"
                    elementFormDefault="qualified">
          <xsd:element name="Request">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="arg1" type="xsd:anyType"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part element="tns:Request"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <operation name="TestOperation">
          <soap:operation soapAction=""/>
          <input>
            <soap:body use="literal"/>
          </input>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    serialized = operation.input.serialize(arg1=xsd.AnyObject(xsd.String(), "ah1"))
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <ns0:Request xmlns:ns0="http://tests.python-zeep.org/tns">
              <ns0:arg1
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:type="xs:string">ah1</ns0:arg1>
            </ns0:Request>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)
    deserialized = operation.input.deserialize(serialized.content)

    assert deserialized == "ah1"


def test_empty_input_parse():
    wsdl_content = StringIO(
        """
    <wsdl:definitions
        xmlns:tns="http://tests.python-zeep.org/"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        targetNamespace="http://tests.python-zeep.org/">
      <wsdl:types>
        <schema xmlns="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/">
        <element name="Result">
            <complexType>
            <sequence>
                <element name="item" type="xsd:string"/>
            </sequence>
            </complexType>
        </element>
        </schema>
      </wsdl:types>
      <wsdl:message name="Request"></wsdl:message>
      <wsdl:message name="Response">
        <wsdl:part element="tns:Result" name="Result"/>
      </wsdl:message>
      <wsdl:portType name="PortType">
        <wsdl:operation name="getResult">
          <wsdl:input message="tns:Request" name="getResultRequest"/>
          <wsdl:output message="tns:Response" name="getResultResponse"/>
        </wsdl:operation>
      </wsdl:portType>
      <wsdl:binding name="Binding" type="tns:PortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
        <wsdl:operation name="getResult">
          <soap:operation soapAction=""/>
          <wsdl:input name="Result">
            <soap:body use="literal"/>
          </wsdl:input>
          </wsdl:operation>
      </wsdl:binding>
      <wsdl:service name="Service">
        <wsdl:port binding="tns:Binding" name="ActiveStations">
        <soap:address location="https://opendap.co-ops.nos.noaa.gov/axis/services/ActiveStations"/>
        </wsdl:port>
      </wsdl:service>
    </wsdl:definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/}Binding"]
    operation = binding.get("getResult")
    assert operation.input.signature() == ""

    serialized = operation.input.serialize()
    expected = """
        <?xml version="1.0"?>
        <soap-env:Envelope
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body/>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, serialized.content)
