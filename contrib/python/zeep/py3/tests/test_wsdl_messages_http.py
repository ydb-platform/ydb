from io import StringIO

from tests.utils import assert_nodes_equal, load_xml
from zeep.wsdl import wsdl


##
# URLEncoded Message
#
def test_urlencoded_serialize():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">

      <message name="Input">
        <part name="arg1" type="xsd:string"/>
        <part name="arg2" type="xsd:string"/>
      </message>
      <message name="Output">
        <part name="Body" type="xsd:string"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="test-operation"/>
          <input>
            <http:urlEncoded/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
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
        operation.input.body.signature(schema=root.types)
        == "TestOperation(arg1: xsd:string, arg2: xsd:string)"
    )
    assert (
        operation.input.signature(as_output=False)
        == "arg1: xsd:string, arg2: xsd:string"
    )

    assert (
        operation.output.body.signature(schema=root.types)
        == "TestOperation(Body: xsd:string)"
    )
    assert operation.output.signature(as_output=True) == "xsd:string"

    serialized = operation.input.serialize(arg1="ah1", arg2="ah2")
    assert serialized.headers == {"Content-Type": "text/xml; charset=utf-8"}
    assert serialized.path == "test-operation"
    assert serialized.content == {"arg1": "ah1", "arg2": "ah2"}


##
# URLReplacement Message
#
def test_urlreplacement_serialize():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">

      <message name="Input">
        <part name="arg1" type="xsd:string"/>
        <part name="arg2" type="xsd:string"/>
      </message>
      <message name="Output">
        <part name="Body" type="xsd:string"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="test-operation/(arg1)/(arg2)/"/>
          <input>
            <http:urlReplacement/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
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
        operation.input.body.signature(schema=root.types)
        == "TestOperation(arg1: xsd:string, arg2: xsd:string)"
    )
    assert (
        operation.input.signature(as_output=False)
        == "arg1: xsd:string, arg2: xsd:string"
    )

    assert (
        operation.output.body.signature(schema=root.types)
        == "TestOperation(Body: xsd:string)"
    )
    assert operation.output.signature(as_output=True) == "xsd:string"

    serialized = operation.input.serialize(arg1="ah1", arg2="ah2")
    assert serialized.headers == {"Content-Type": "text/xml; charset=utf-8"}
    assert serialized.path == "test-operation/ah1/ah2/"
    assert serialized.content == ""


##
# MimeContent Message
#
def test_mime_content_serialize_form_urlencoded():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">

      <message name="Input">
        <part name="arg1" type="xsd:string"/>
        <part name="arg2" type="xsd:string"/>
      </message>
      <message name="Output">
        <part name="Body" type="xsd:string"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="test-operation"/>
          <input>
            <mime:content type="application/x-www-form-urlencoded"/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
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
        operation.input.body.signature(schema=root.types)
        == "TestOperation(arg1: xsd:string, arg2: xsd:string)"
    )
    assert (
        operation.input.signature(as_output=False)
        == "arg1: xsd:string, arg2: xsd:string"
    )

    assert (
        operation.output.body.signature(schema=root.types)
        == "TestOperation(Body: xsd:string)"
    )
    assert operation.output.signature(as_output=True) == "xsd:string"

    serialized = operation.input.serialize(arg1="ah1", arg2="ah2")
    assert serialized.headers == {"Content-Type": "application/x-www-form-urlencoded"}
    assert serialized.path == "test-operation"
    assert serialized.content == "arg1=ah1&arg2=ah2"


def test_mime_content_serialize_text_xml():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">

      <message name="Input">
        <part name="arg1" type="xsd:string"/>
        <part name="arg2" type="xsd:string"/>
      </message>
      <message name="Output">
        <part name="Body" type="xsd:string"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="test-operation"/>
          <input>
            <mime:content type="text/xml"/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
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
        operation.input.body.signature(schema=root.types)
        == "TestOperation(arg1: xsd:string, arg2: xsd:string)"
    )
    assert (
        operation.input.signature(as_output=False)
        == "arg1: xsd:string, arg2: xsd:string"
    )

    assert (
        operation.output.body.signature(schema=root.types)
        == "TestOperation(Body: xsd:string)"
    )
    assert operation.output.signature(as_output=True) == "xsd:string"

    serialized = operation.input.serialize(arg1="ah1", arg2="ah2")
    assert serialized.headers == {"Content-Type": "text/xml"}
    assert serialized.path == "test-operation"
    assert_nodes_equal(
        load_xml(serialized.content),
        load_xml("<TestOperation><arg1>ah1</arg1><arg2>ah2</arg2></TestOperation>"),
    )


def test_mime_content_no_parts():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">

      <message name="Input"/>
      <message name="Output">
        <part name="Body" type="xsd:string"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="/test-operation"/>
          <input>
            <mime:content type="application/x-www-form-urlencoded"/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert operation.input.signature() == ""

    serialized = operation.input.serialize()
    assert serialized.content == ""


def test_mime_xml_deserialize():
    wsdl_content = StringIO(
        """
    <definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
                 xmlns:tns="http://tests.python-zeep.org/tns"
                 xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
                 xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
                 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                 targetNamespace="http://tests.python-zeep.org/tns">
      <types>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/tns"
            targetNamespace="http://tests.python-zeep.org/tns"
                elementFormDefault="qualified">
          <xsd:element name="response">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="item_1" type="xsd:string"/>
                <xsd:element name="item_2" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
      </types>

      <message name="Input">
        <part name="arg1" type="xsd:string"/>
        <part name="arg2" type="xsd:string"/>
      </message>
      <message name="Output">
        <part name="Body" element="tns:response"/>
      </message>

      <portType name="TestPortType">
        <operation name="TestOperation">
          <input message="Input"/>
          <output message="Output"/>
        </operation>
      </portType>

      <binding name="TestBinding" type="tns:TestPortType">
        <http:binding verb="POST"/>
        <operation name="TestOperation">
          <http:operation location="/test-operation"/>
          <input>
            <mime:content type="application/x-www-form-urlencoded"/>
          </input>
          <output>
            <mime:mimeXml part="Body"/>
          </output>
        </operation>
      </binding>
    </definitions>
    """.strip()
    )

    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")

    assert operation.input.signature() == "arg1: xsd:string, arg2: xsd:string"
    assert operation.output.signature(as_output=True) == (
        "item_1: xsd:string, item_2: xsd:string"
    )

    node = """
        <response xmlns="http://tests.python-zeep.org/tns">
          <item_1>foo</item_1>
          <item_2>bar</item_2>
        </response>
    """.strip()

    serialized = operation.output.deserialize(node)
    assert serialized.item_1 == "foo"
    assert serialized.item_2 == "bar"


def test_mime_multipart_parse():
    load_xml(
        """
        <output
            xmlns="http://schemas.xmlsoap.org/wsdl/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/">
          <mime:multipartRelated>
              <mime:part>
                  <soap:body parts="body" use="literal"/>
              </mime:part>
              <mime:part>
                  <mime:content part="test" type="text/html"/>
              </mime:part>
              <mime:part>
                  <mime:content part="logo" type="image/gif"/>
                  <mime:content part="logo" type="image/jpeg"/>
              </mime:part>
          </mime:multipartRelated>
       </output>
    """
    )
