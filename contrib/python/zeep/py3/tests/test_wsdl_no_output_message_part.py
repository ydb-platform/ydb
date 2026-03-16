from io import StringIO

from zeep.wsdl import wsdl


def test_wsdl_parses_operations_with_no_output():

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
    """
    )
    # parse the content
    root = wsdl.Document(wsdl_content, None)

    binding = root.bindings["{http://tests.python-zeep.org/tns}TestBinding"]
    operation = binding.get("TestOperation")
    # General assertions on the input
    assert (
        operation.input.body.signature(schema=root.types) == "ns0:Request(xsd:string)"
    )
    assert operation.input.header.signature(schema=root.types) == "soap-env:Header()"
    assert (
        operation.input.envelope.signature(schema=root.types)
        == "soap-env:envelope(body: xsd:string)"
    )
    assert operation.input.signature(as_output=False) == "xsd:string"
