import io

import pytest
from pretend import stub

from zeep import Client
from zeep.transports import Transport


@pytest.mark.requests
def test_parse_multiref_soap_response():
    wsdl_file = io.StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/">

          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
              <xsd:element name="input" type="xsd:string"/>

              <xsd:element name="output">
                <xsd:complexType>
                  <xsd:sequence>
                    <xsd:element name="item_1" type="tns:type_1"/>
                    <xsd:element name="item_2" type="tns:type_2"/>
                  </xsd:sequence>
                </xsd:complexType>
             </xsd:element>

              <xsd:complexType name="type_1">
                <xsd:sequence>
                  <xsd:element name="subitem_1" type="xsd:string"/>
                  <xsd:element name="subitem_2" type="xsd:string"/>
                </xsd:sequence>
              </xsd:complexType>
              <xsd:complexType name="type_2">
                <xsd:sequence>
                  <xsd:element name="subitem_1" type="tns:type_1"/>
                  <xsd:element name="subitem_2" type="xsd:string"/>
                </xsd:sequence>
              </xsd:complexType>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="TestOperationRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>

          <wsdl:message name="TestOperationResponse">
            <wsdl:part name="response" element="tns:output"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation">
              <wsdl:input message="TestOperationRequest"/>
              <wsdl:output message="TestOperationResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="rpc" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation">
              <soap:operation soapAction=""/>
              <wsdl:input name="TestOperationRequest">
                <soap:body use="encoded"
                           encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" />
              </wsdl:input>
              <wsdl:output name="TestOperationResponse">
                <soap:body use="encoded"
                           encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" />
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
    )  # noqa

    content = """
        <?xml version="1.0"?>
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
           <soapenv:Body>
              <tns:TestOperationResponse>
                <tns:output>
                  <tns:item_1 href="#id0"/>
                  <tns:item_2>
                    <tns:subitem_1>
                      <tns:subitem_1>foo</tns:subitem_1>
                      <tns:subitem_2>bar</tns:subitem_2>
                    </tns:subitem_1>
                  <tns:subitem_2>bar</tns:subitem_2>
                  </tns:item_2>
                </tns:output>
              </tns:TestOperationResponse>

              <multiRef id="id0">
                <tns:subitem_1>foo</tns:subitem_1>
                <tns:subitem_2>bar</tns:subitem_2>
              </multiRef>
           </soapenv:Body>
        </soapenv:Envelope>
    """.strip()

    client = Client(wsdl_file, transport=Transport())
    response = stub(status_code=200, headers={}, content=content)

    operation = client.service._binding._operations["TestOperation"]
    result = client.service._binding.process_reply(client, operation, response)

    assert result.item_1.subitem_1 == "foo"
    assert result.item_1.subitem_2 == "bar"
    assert result.item_2.subitem_1.subitem_1 == "foo"
    assert result.item_2.subitem_1.subitem_2 == "bar"
    assert result.item_2.subitem_2 == "bar"


@pytest.mark.requests
def test_parse_multiref_soap_response_child():
    wsdl_file = io.StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/">

          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
              <xsd:element name="input" type="xsd:string"/>

              <xsd:element name="output">
                <xsd:complexType>
                  <xsd:sequence>
                    <xsd:element name="item_1" type="tns:type_1"/>
                    <xsd:element name="item_2" type="tns:type_2"/>
                  </xsd:sequence>
                </xsd:complexType>
             </xsd:element>

              <xsd:complexType name="type_1">
                <xsd:sequence>
                  <xsd:element name="subitem_1" type="xsd:string"/>
                  <xsd:element name="subitem_2" type="xsd:string"/>
                  <xsd:element name="subitem_3" type="tns:type_3"/>
                </xsd:sequence>
              </xsd:complexType>
              <xsd:complexType name="type_2">
                <xsd:sequence>
                  <xsd:element name="subitem_1" type="tns:type_1"/>
                  <xsd:element name="subitem_2" type="xsd:string"/>
                </xsd:sequence>
              </xsd:complexType>
              <xsd:complexType name="type_3" nillable="true">
                <xsd:sequence>
                </xsd:sequence>
              </xsd:complexType>
            </xsd:schema>
          </wsdl:types>

          <wsdl:message name="TestOperationRequest">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>

          <wsdl:message name="TestOperationResponse">
            <wsdl:part name="response" element="tns:output"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation">
              <wsdl:input message="TestOperationRequest"/>
              <wsdl:output message="TestOperationResponse"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="rpc" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation">
              <soap:operation soapAction=""/>
              <wsdl:input name="TestOperationRequest">
                <soap:body use="encoded" encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" />
              </wsdl:input>
              <wsdl:output name="TestOperationResponse">
                <soap:body use="encoded" encodingStyle="http://schemas.xmlsoap.org/soap/encoding/" />
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
    )  # noqa

    content = """
        <?xml version="1.0"?>
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
           <soapenv:Body>
              <tns:TestOperationResponse>
                <tns:output>
                  <tns:item_1 href="#id0"/>
                  <tns:item_2>
                    <tns:subitem_1>
                      <tns:subitem_1>foo</tns:subitem_1>
                      <tns:subitem_2>bar</tns:subitem_2>
                    </tns:subitem_1>
                  <tns:subitem_2>bar</tns:subitem_2>
                  </tns:item_2>
                </tns:output>
              </tns:TestOperationResponse>

              <multiRef id="id0">
                <tns:subitem_1>foo</tns:subitem_1>
                <tns:subitem_2>bar</tns:subitem_2>
                <tns:subitem_3 xmlns:tns2="http://tests.python-zeep.org/" xsi:type="tns2:type_3"></tns:subitem_3>
              </multiRef>
           </soapenv:Body>
        </soapenv:Envelope>
    """.strip()  # noqa

    client = Client(wsdl_file, transport=Transport())
    response = stub(status_code=200, headers={}, content=content)

    operation = client.service._binding._operations["TestOperation"]
    result = client.service._binding.process_reply(client, operation, response)

    assert result.item_1.subitem_1 == "foo"
    assert result.item_1.subitem_2 == "bar"
    assert result.item_2.subitem_1.subitem_1 == "foo"
    assert result.item_2.subitem_1.subitem_2 == "bar"
    assert result.item_2.subitem_2 == "bar"
