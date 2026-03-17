from io import StringIO

import pytest
import requests_mock
from lxml import etree
from pretend import stub

from tests.utils import DummyTransport, assert_nodes_equal
from zeep import Client, Settings, wsdl
from zeep.exceptions import DTDForbidden, EntitiesForbidden
from zeep.transports import Transport

import yatest.common as yc


@pytest.mark.requests
def test_parse_soap_wsdl():
    client = Client(yc.test_source_path("wsdl_files/soap.wsdl"), transport=Transport())

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

    client.set_ns_prefix("stoc", "http://example.com/stockquote.xsd")

    with requests_mock.mock() as m:
        m.post("http://example.com/stockquote", text=response)
        account_type = client.get_type("stoc:account")
        account = account_type(id=100)
        account.user = "mvantellingen"
        country = client.get_element("stoc:country").type()
        country.name = "The Netherlands"
        country.code = "NL"

        result = client.service.GetLastTradePrice(
            tickerSymbol="foobar", account=account, country=country
        )
        assert result == 120.123

        request = m.request_history[0]

        # Compare request body
        expected = """
        <soap-env:Envelope
                xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:stoc="http://example.com/stockquote.xsd">
            <soap-env:Body>
              <stoc:TradePriceRequest>
                <tickerSymbol>foobar</tickerSymbol>
                <account>
                  <id>100</id>
                  <user>mvantellingen</user>
                </account>
                <stoc:country>
                  <name>The Netherlands</name>
                  <code>NL</code>
                </stoc:country>
              </stoc:TradePriceRequest>
           </soap-env:Body>
        </soap-env:Envelope>
        """
        assert_nodes_equal(expected, request.body)


@pytest.mark.requests
def test_parse_soap_header_wsdl():
    client = Client(yc.test_source_path("wsdl_files/soap_header.wsdl"), transport=Transport())

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
        result = client.service.GetLastTradePrice(
            tickerSymbol="foobar",
            _soapheaders={"header": {"username": "ikke", "password": "oeh-is-geheim!"}},
        )

        assert result.body.price == 120.123
        assert result.header.body is None

        request = m.request_history[0]

        # Compare request body
        expected = """
        <soap-env:Envelope
                xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
           <soap-env:Header>
              <ns0:Authentication xmlns:ns0="http://example.com/stockquote.xsd">
                 <username>ikke</username>
                 <password>oeh-is-geheim!</password>
              </ns0:Authentication>
           </soap-env:Header>
           <soap-env:Body>
              <ns0:TradePriceRequest xmlns:ns0="http://example.com/stockquote.xsd">
                 <tickerSymbol>foobar</tickerSymbol>
              </ns0:TradePriceRequest>
           </soap-env:Body>
        </soap-env:Envelope>
        """
        assert_nodes_equal(expected, request.body)


def test_parse_types_multiple_schemas():

    content = StringIO(
        """
    <?xml version="1.0"?>
    <wsdl:definitions xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:s1="http://microsoft.com/wsdl/types/"
        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
        xmlns:tns="http://tests.python-zeep.org/"
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        targetNamespace="http://tests.python-zeep.org/">
      <wsdl:types>
        <xsd:schema elementFormDefault="qualified"
            xmlns:s1="http://microsoft.com/wsdl/types/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:tns="http://tests.python-zeep.org//"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/">
          <xsd:import namespace="http://microsoft.com/wsdl/types/" />
          <xsd:element name="foobardiedar" type="s1:guid"/>
        </xsd:schema>
        <xsd:schema elementFormDefault="qualified"
           targetNamespace="http://microsoft.com/wsdl/types/">
          <xsd:simpleType name="guid">
            <xsd:restriction base="xsd:string"/>
          </xsd:simpleType>
        </xsd:schema>
      </wsdl:types>
    </wsdl:definitions>
    """.strip()
    )

    assert wsdl.Document(content, None)


def test_parse_types_nsmap_issues():
    content = StringIO(
        r"""
    <?xml version="1.0"?>
    <wsdl:definitions targetNamespace="urn:ec.europa.eu:taxud:vies:services:checkVat"
      xmlns:tns1="urn:ec.europa.eu:taxud:vies:services:checkVat:types"
      xmlns:soapenc="http://schemas.xmlsoap.org/soap/encoding/"
      xmlns:impl="urn:ec.europa.eu:taxud:vies:services:checkVat"
      xmlns:apachesoap="http://xml.apache.org/xml-soap"
      xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/">
      <wsdl:types>
        <xsd:schema attributeFormDefault="qualified"
            elementFormDefault="qualified"
            targetNamespace="urn:ec.europa.eu:taxud:vies:services:checkVat:types"
            xmlns="urn:ec.europa.eu:taxud:vies:services:checkVat:types">
                <xsd:element name="checkVatApprox">
                    <xsd:complexType>
                        <xsd:sequence>
                            <xsd:element maxOccurs="1" minOccurs="0"
                                name="traderCompanyType"
                                type="tns1:companyTypeCode"/>
                        </xsd:sequence>
                    </xsd:complexType>
                </xsd:element>
                <xsd:simpleType name="companyTypeCode">
                    <xsd:restriction base="xsd:string">
                        <xsd:pattern value="[A-Z]{2}\-[1-9][0-9]?"/>
                    </xsd:restriction>
                </xsd:simpleType>
            </xsd:schema>
      </wsdl:types>
    </wsdl:definitions>
    """.strip()
    )
    assert wsdl.Document(content, None)


@pytest.mark.requests
def test_parse_soap_import_wsdl():
    client = stub(transport=Transport(), wsse=None)
    content = open(yc.test_source_path("wsdl_files/soap-enc.xsd"), encoding="utf-8").read()

    with requests_mock.mock() as m:
        m.get("http://schemas.xmlsoap.org/soap/encoding/", text=content)

        obj = wsdl.Document(
            yc.test_source_path("wsdl_files/soap_import_main.wsdl"), transport=client.transport
        )
        assert len(obj.services) == 1
        assert obj.types.is_empty is False
        obj.dump()


def test_multiple_extension():
    content = StringIO(
        """
    <?xml version="1.0"?>
    <wsdl:definitions
      xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/">
      <wsdl:types>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

            <xs:import namespace="http://tests.python-zeep.org/b"/>

            <xs:complexType name="type_a">
              <xs:complexContent>
                <xs:extension base="b:type_b"/>
              </xs:complexContent>
            </xs:complexType>
            <xs:element name="typetje" type="tns:type_a"/>
        </xs:schema>

        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            xmlns:c="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:import namespace="http://tests.python-zeep.org/c"/>

            <xs:complexType name="type_b">
              <xs:complexContent>
                <xs:extension base="c:type_c"/>
              </xs:complexContent>
            </xs:complexType>
        </xs:schema>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/c"
            targetNamespace="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:complexType name="type_c">
              <xs:complexContent>
                <xs:extension base="tns:type_d"/>
              </xs:complexContent>
            </xs:complexType>

            <xs:complexType name="type_d">
                <xs:attribute name="wat" type="xs:string" />
            </xs:complexType>
        </xs:schema>
      </wsdl:types>
    </wsdl:definitions>
    """.strip()
    )
    document = wsdl.Document(content, None)

    type_a = document.types.get_element("ns0:typetje")
    type_a(wat="x")

    type_a = document.types.get_type("ns0:type_a")
    type_a(wat="x")


def test_create_import_schema(recwarn):
    content = StringIO(
        """
    <?xml version="1.0"?>
    <wsdl:definitions
      xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/">

      <wsdl:types>
        <xsd:schema>
          <xsd:import namespace="http://tests.python-zeep.org/a"
                      schemaLocation="a.xsd"/>
        </xsd:schema>
        <xsd:schema>
          <xsd:import namespace="http://tests.python-zeep.org/b"
                      schemaLocation="b.xsd"/>
        </xsd:schema>
      </wsdl:types>
    </wsdl:definitions>
    """.strip()
    )

    schema_node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
        </xsd:schema>
    """.strip()
    )

    schema_node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

            <xsd:element name="global" type="xsd:string"/>
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_node_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_node_b)

    document = wsdl.Document(
        content, transport, "http://tests.python-zeep.org/content.wsdl"
    )

    assert len(recwarn) == 0
    assert document.types.get_element("{http://tests.python-zeep.org/b}global")


def test_wsdl_imports_xsd(recwarn):
    content = StringIO(
        """
    <?xml version="1.0"?>
    <wsdl:definitions
      xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/">
      <wsdl:import location="a.xsd" namespace="http://tests.python-zeep.org/a"/>
    </wsdl:definitions>
    """.strip()
    )

    schema_node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
          <xsd:import namespace="http://tests.python-zeep.org/b" schemaLocation="b.xsd"/>
        </xsd:schema>
    """.strip()
    )

    schema_node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_node_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_node_b)

    wsdl.Document(content, transport, "http://tests.python-zeep.org/content.wsdl")


def test_import_schema_without_location(recwarn):
    content = StringIO(
        """
    <?xml version="1.0"?>
    <wsdl:definitions
      xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
      xmlns:xsd="http://www.w3.org/2001/XMLSchema"
      xmlns:b="http://tests.python-zeep.org/b"
      xmlns:c="http://tests.python-zeep.org/c"
      xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
      targetNamespace="http://tests.python-zeep.org/transient"
      xmlns:tns="http://tests.python-zeep.org/transient">

      <wsdl:types>
        <xsd:schema>
          <xsd:import namespace="http://tests.python-zeep.org/a"
                      schemaLocation="a.xsd"/>
        </xsd:schema>
        <xsd:schema targetNamespace="http://tests.python-zeep.org/c">
          <xsd:element name="bar" type="b:foo"/>
        </xsd:schema>
      </wsdl:types>
      <wsdl:message name="method">
        <wsdl:part name="param" element="c:bar"/>
      </wsdl:message>
      <wsdl:portType name="port_type">
        <wsdl:operation name="method" parameterOrder="param">
          <wsdl:input message="tns:method" />
        </wsdl:operation>
      </wsdl:portType>
      <wsdl:binding name="binding" type="tns:port_type" >
        <soap:binding style="rpc" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="method" >
          <soap:operation soapAction="method"/>
          <wsdl:input>
            <soap:body use="literal" />
          </wsdl:input>
        </wsdl:operation>
      </wsdl:binding>
    </wsdl:definitions>
    """.strip()
    )

    schema_node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

          <xsd:import namespace="http://tests.python-zeep.org/b"
                      schemaLocation="b.xsd"/>

        </xsd:schema>
    """.strip()
    )

    schema_node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

          <xsd:complexType name="foo">
            <xsd:sequence>
              <xsd:element name="item_1" type="xsd:string"/>
            </xsd:sequence>
          </xsd:complexType>
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_node_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_node_b)

    document = wsdl.Document(
        content, transport, "http://tests.python-zeep.org/content.wsdl"
    )
    assert len(recwarn) == 0
    assert document.types.get_type("{http://tests.python-zeep.org/b}foo")


def test_wsdl_import(recwarn):
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
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-secondary"
            location="http://tests.python-zeep.org/schema-2.wsdl"/>
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-1">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="message-1"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <wsdl:input message="sec:message-2"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
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

    wsdl_2 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:mine="http://tests.python-zeep.org/xsd-secondary"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/wsdl-secondary">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-secondary"
                xmlns:tns="http://tests.python-zeep.org/xsd-secondary">
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-2">
            <wsdl:part name="response" element="mine:input2"/>
          </wsdl:message>
        </wsdl:definitions>
    """.strip()

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/schema-2.wsdl", wsdl_2)
    document = wsdl.Document(wsdl_main, transport)
    document.dump()


def test_wsdl_import_transitive(recwarn):
    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-2"
          xmlns:third="http://tests.python-zeep.org/wsdl-3"
          xmlns:fourth="http://tests.python-zeep.org/wsdl-4"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-2"
            location="http://tests.python-zeep.org/schema-2.wsdl"/>
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-1">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="message-1"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <wsdl:input message="sec:message-2"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation3">
              <wsdl:input message="third:message-3"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation4">
              <wsdl:input message="fourth:message-4"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
            </wsdl:operation>
            <wsdl:operation name="TestOperation3">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
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

    wsdl_2 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/wsdl-2"
          xmlns:mine="http://tests.python-zeep.org/xsd-2"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/wsdl-2">
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-3"
            location="http://tests.python-zeep.org/schema-3.wsdl"/>
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-2"
                xmlns:tns="http://tests.python-zeep.org/xsd-2">
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-2">
            <wsdl:part name="response" element="mine:input2"/>
          </wsdl:message>
        </wsdl:definitions>
    """.strip()

    wsdl_3 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/wsdl-third"
          xmlns:mine="http://tests.python-zeep.org/xsd-3"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/wsdl-3">
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-2"
            location="http://tests.python-zeep.org/schema-2.wsdl"/>
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-4"
            location="http://tests.python-zeep.org/schema-4.wsdl"/>
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-3"
                xmlns:tns="http://tests.python-zeep.org/xsd-3">
              <xsd:element name="input3" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-3">
            <wsdl:part name="response" element="mine:input3"/>
          </wsdl:message>
        </wsdl:definitions>
    """.strip()

    wsdl_4 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/wsdl-4"
          xmlns:mine="http://tests.python-zeep.org/xsd-4"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/wsdl-4">
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-3"
            location="http://tests.python-zeep.org/schema-3.wsdl"/>
          <wsdl:message name="message-4">
            <wsdl:part name="response" type="xsd:string"/>
          </wsdl:message>
        </wsdl:definitions>
    """.strip()

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/schema-2.wsdl", wsdl_2)
    transport.bind("http://tests.python-zeep.org/schema-3.wsdl", wsdl_3)
    transport.bind("http://tests.python-zeep.org/schema-4.wsdl", wsdl_4)

    document = wsdl.Document(wsdl_main, transport)
    document.dump()


def test_wsdl_import_xsd_references(recwarn):
    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:xsd-sec="http://tests.python-zeep.org/xsd-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">
          <wsdl:import namespace="http://tests.python-zeep.org/wsdl-secondary"
            location="http://tests.python-zeep.org/schema-2.wsdl"/>
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-1">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>
          <wsdl:message name="message-2">
            <wsdl:part name="response" element="xsd-sec:input2"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="message-1"/>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <wsdl:input message="sec:message-2"/>
            </wsdl:operation>
          </wsdl:portType>

          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
            </wsdl:operation>
            <wsdl:operation name="TestOperation2">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
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

    wsdl_2 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:mine="http://tests.python-zeep.org/xsd-secondary"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/wsdl-secondary">
          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-secondary"
                xmlns:tns="http://tests.python-zeep.org/xsd-secondary">
              <xsd:element name="input2" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-2">
            <wsdl:part name="response" element="mine:input2"/>
          </wsdl:message>
        </wsdl:definitions>
    """.strip()

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/schema-2.wsdl", wsdl_2)
    document = wsdl.Document(wsdl_main, transport)
    document.dump()


def test_parse_operation_empty_nodes():
    content = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions xmlns:s="http://www.w3.org/2001/XMLSchema"
            xmlns:http="http://schemas.xmlsoap.org/wsdl/http/"
            xmlns:tns="http://tests.python-zeep.org/"
            xmlns:s1="http://microsoft.com/wsdl/types/"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"
            targetNamespace="http://tests.python-zeep.org/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
          <wsdl:types>
            <s:schema targetNamespace="http://tests.python-zeep.org/">
              <s:import namespace="http://microsoft.com/wsdl/types/" />
              <s:element name="ExampleMethod">
              </s:element>
            </s:schema>
            <s:schema targetNamespace="http://microsoft.com/wsdl/types/">
              <s:simpleType name="char">
                <s:restriction base="s:unsignedShort" />
              </s:simpleType>
            </s:schema>
          </wsdl:types>
          <wsdl:message name="MessageIn">
            <wsdl:part name="parameters" element="tns:ExampleMethod" />
          </wsdl:message>
          <wsdl:message name="MessageOut">
            <wsdl:part name="parameters" element="tns:ExampleMethod" />
          </wsdl:message>
          <wsdl:portType name="ExampleSoap">
            <wsdl:operation name="ExampleMethod">
              <wsdl:documentation xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
                Example documentation.
              </wsdl:documentation>
              <wsdl:input message="tns:MessageIn" />
              <wsdl:output message="tns:MessageOut" />
            </wsdl:operation>
          </wsdl:portType>
          <wsdl:binding name="ExampleSoap" type="tns:ExampleSoap">
            <http:binding verb="POST" />
            <wsdl:operation name="ExampleMethod">
              <http:operation location="/ExampleMethod" />
              <wsdl:input>
                <mime:content type="application/x-www-form-urlencoded" />
              </wsdl:input>
              <wsdl:output />
            </wsdl:operation>
          </wsdl:binding>
        </wsdl:definitions>
    """.strip()
    )

    assert wsdl.Document(content, None)


def test_wsdl_duplicate_tns(recwarn):
    wsdl_main = StringIO(
        """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:sec="http://tests.python-zeep.org/wsdl-secondary"
          xmlns:xsd-sec="http://tests.python-zeep.org/xsd-secondary"
          xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">

          <wsdl:import namespace="http://tests.python-zeep.org/xsd-main"
            location="http://tests.python-zeep.org/schema-2.wsdl"/>


          <wsdl:binding name="TestBinding" type="tns:TestPortType">
            <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
            <wsdl:operation name="TestOperation1">
              <soap:operation soapAction=""/>
              <wsdl:input>
                <soap:body use="literal"/>
              </wsdl:input>
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

    wsdl_2 = """
        <?xml version="1.0"?>
        <wsdl:definitions
          xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          xmlns:tns="http://tests.python-zeep.org/xsd-main"
          xmlns:mine="http://tests.python-zeep.org/xsd-secondary"
          xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
          targetNamespace="http://tests.python-zeep.org/xsd-main">

          <wsdl:types>
            <xsd:schema
                targetNamespace="http://tests.python-zeep.org/xsd-main"
                xmlns:tns="http://tests.python-zeep.org/xsd-main">
              <xsd:element name="input" type="xsd:string"/>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="message-1">
            <wsdl:part name="response" element="tns:input"/>
          </wsdl:message>

          <wsdl:portType name="TestPortType">
            <wsdl:operation name="TestOperation1">
              <wsdl:input message="message-1"/>
            </wsdl:operation>
          </wsdl:portType>
        </wsdl:definitions>
    """.strip()

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/schema-2.wsdl", wsdl_2)
    document = wsdl.Document(wsdl_main, transport)
    document.dump()


def test_wsdl_dtd_entities_rules():
    wsdl_declaration = """<!DOCTYPE Author [
        <!ENTITY writer "Donald Duck.">
        ]>
        <wsdl:definitions
        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:tns="http://tests.python-zeep.org/xsd-main"
        xmlns:mine="http://tests.python-zeep.org/xsd-secondary"
        xmlns:wsdlsoap="http://schemas.xmlsoap.org/wsdl/soap/"
        targetNamespace="http://tests.python-zeep.org/xsd-main">
        <wsdl:types>
          <xsd:schema
              targetNamespace="http://tests.python-zeep.org/xsd-main"
              xmlns:tns="http://tests.python-zeep.org/xsd-main">
            <xsd:element name="input" type="xsd:string"/>
          </xsd:schema>
        </wsdl:types>
        <wsdl:message name="message-1">
          <wsdl:part name="response" element="tns:input"/>
        </wsdl:message>
        <wsdl:portType name="TestPortType">
          <wsdl:operation name="TestOperation1">
            <wsdl:input message="message-1"/>
          </wsdl:operation>
        </wsdl:portType>
        </wsdl:definitions>
    """.strip()

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/schema-2.wsdl", wsdl_declaration)

    with pytest.raises(DTDForbidden):
        wsdl.Document(
            StringIO(wsdl_declaration), transport, settings=Settings(forbid_dtd=True)
        )

    with pytest.raises(EntitiesForbidden):
        wsdl.Document(StringIO(wsdl_declaration), transport)

    document = wsdl.Document(
        StringIO(wsdl_declaration), transport, settings=Settings(forbid_entities=False)
    )
    document.dump()


def test_extra_http_headers(recwarn, monkeypatch):

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

    client = stub(settings=Settings(), plugins=[], wsse=None)

    transport = DummyTransport()
    doc = wsdl.Document(wsdl_main, transport, settings=client.settings)
    binding = doc.services.get("TestService").ports.get("TestPortType").binding

    headers = {"Authorization": "Bearer 1234"}
    with client.settings(extra_http_headers=headers):
        envelope, headers = binding._create(
            "TestOperation1",
            args=["foo"],
            kwargs={},
            client=client,
            options={"address": "http://tests.python-zeep.org/test"},
        )

    expected = """
        <soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope">
          <soap-env:Body>
            <ns0:input xmlns:ns0="http://tests.python-zeep.org/xsd-main">foo</ns0:input>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)

    assert headers["Authorization"] == "Bearer 1234"


def test_wsdl_no_schema_namespace():
    wsdl_main = StringIO(
        """
        <wsdl:definitions
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:tns="http://Example.org"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://Example.org"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
          <wsdl:types>
            <xsd:schema elementFormDefault="qualified" >
              <xsd:element name="AddResponse">
                <xsd:complexType>
                    <xsd:sequence>
                    <xsd:element minOccurs="0" maxOccurs="1" ref="demo" />
                    </xsd:sequence>
                </xsd:complexType>
              </xsd:element>
              <xsd:element name="Add">
                <xsd:complexType>
                  <xsd:sequence>
                    <xsd:element minOccurs="1" name="a" type="xsd:int" />
                    <xsd:element minOccurs="1" name="b" type="xsd:int" />
                  </xsd:sequence>
                </xsd:complexType>
              </xsd:element>
              <xsd:element name="AddResponse">
                <xsd:complexType>
                  <xsd:sequence>
                    <xsd:element minOccurs="0" name="result" type="xsd:int" />
                  </xsd:sequence>
                </xsd:complexType>
              </xsd:element>
            </xsd:schema>
            <xsd:schema elementFormDefault="qualified" >
              <xsd:element name="demo">
              </xsd:element>
            </xsd:schema>
          </wsdl:types>
          <wsdl:message name="ICalculator_Add_InputMessage">
            <wsdl:part name="parameters" element="Add" />
          </wsdl:message>
          <wsdl:message name="ICalculator_Add_OutputMessage">
            <wsdl:part name="parameters" element="AddResponse" />
          </wsdl:message>
          <wsdl:portType name="ICalculator">
            <wsdl:operation name="Add">
              <wsdl:input message="tns:ICalculator_Add_InputMessage" />
              <wsdl:output message="tns:ICalculator_Add_OutputMessage" />
            </wsdl:operation>
          </wsdl:portType>
          <wsdl:binding name="DefaultBinding_ICalculator" type="tns:ICalculator">
            <soap:binding transport="http://schemas.xmlsoap.org/soap/http" />
            <wsdl:operation name="Add">
              <soap:operation soapAction="http://Example.org/ICalculator/Add" style="document" />
              <wsdl:input>
                <soap:body use="literal" />
              </wsdl:input>
              <wsdl:output>
                <soap:body use="literal" />
              </wsdl:output>
            </wsdl:operation>
          </wsdl:binding>
          <wsdl:service name="CalculatorService">
            <wsdl:port name="ICalculator" binding="tns:DefaultBinding_ICalculator">
              <soap:address location="http://Example.org/ICalculator" />
            </wsdl:port>
          </wsdl:service>
        </wsdl:definitions>
    """
    )
    client = stub(settings=Settings(), plugins=[], wsse=None)

    transport = DummyTransport()
    document = wsdl.Document(wsdl_main, transport)
    binding = (
        document.services.get("CalculatorService").ports.get("ICalculator").binding
    )

    envelope, headers = binding._create(
        "Add",
        args=[3, 4],
        kwargs={},
        client=client,
        options={"address": "http://tests.python-zeep.org/test"},
    )

    expected = """
        <soap-env:Envelope xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
          <soap-env:Body>
            <Add>
                <a>3</a>
                <b>4</b>
            </Add>
          </soap-env:Body>
        </soap-env:Envelope>
    """
    assert_nodes_equal(expected, envelope)


def test_namespaced_wsdl_with_empty_import():
    # See https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#src-resolve for details
    wsdl_main = StringIO(
        """
        <wsdl:definitions xmlns:s="http://www.w3.org/2001/XMLSchema" targetNamespace="weird_wsdl" xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
        <wsdl:types>
            <s:schema elementFormDefault="qualified" targetNamespace="weird_wsdl">
                <s:import />
                <s:element name="GetVTInfoResponse">
                    <s:complexType>
                    <s:sequence>
                        <s:element minOccurs="0" maxOccurs="1" ref="GetVTInfoResult" />
                    </s:sequence>
                    </s:complexType>
                </s:element>
            </s:schema>
            <s:schema elementFormDefault="qualified">
                <s:element name="GetVTInfoResult" />
            </s:schema>
        </wsdl:types>
        </wsdl:definitions>
    """
    )

    transport = DummyTransport()
    document = wsdl.Document(wsdl_main, transport)
    document.dump()


def test_import_cyclic():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/b.xsd"
                namespace="http://tests.python-zeep.org/b"/>

        </xs:schema>
    """.strip()
    )

    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/a.xsd"
                namespace="http://tests.python-zeep.org/a"/>
            <xs:element name="bar" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    wsdl_content = StringIO(
        """
    <?xml version='1.0'?>
    <definitions
        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
        xmlns:tns="http://tests.python-zeep.org/root"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns="http://schemas.xmlsoap.org/wsdl/" targetNamespace="http://tests.python-zeep.org/root" name="root">
        <types>
          <xsd:schema
              xmlns:xs="http://www.w3.org/2001/XMLSchema"
              xmlns:tns="http://tests.python-zeep.org/b"
              targetNamespace="http://tests.python-zeep.org/b"
              elementFormDefault="qualified">

              <xs:import
                  schemaLocation="http://tests.python-zeep.org/a.xsd"
                  namespace="http://tests.python-zeep.org/a"/>
              <xs:import
                  schemaLocation="http://tests.python-zeep.org/b.xsd"
                  namespace="http://tests.python-zeep.org/b"/>

              <xs:element name="foo" type="xs:string"/>
          </xsd:schema>
        </types>
    </definitions>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("https://tests.python-zeep.org/a.xsd", node_a)
    transport.bind("https://tests.python-zeep.org/b.xsd", node_b)

    document = wsdl.Document(
        wsdl_content, transport, "https://tests.python-zeep.org/content.wsdl"
    )


def test_import_no_location():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

        </xs:schema>
    """.strip()
    )

    wsdl_content = StringIO(
        """
    <?xml version='1.0'?>
    <definitions
        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
        xmlns:tns="http://tests.python-zeep.org/root"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns="http://schemas.xmlsoap.org/wsdl/" targetNamespace="http://tests.python-zeep.org/root" name="root">
        <types>
          <xsd:schema
              xmlns:xs="http://www.w3.org/2001/XMLSchema"
              xmlns:tns="http://tests.python-zeep.org/b"
              targetNamespace="http://tests.python-zeep.org/b"
              elementFormDefault="qualified">

              <xs:import namespace="http://tests.python-zeep.org/a"/>
              <xs:element name="foo" type="xs:string"/>
          </xsd:schema>
        </types>
    </definitions>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("https://tests.python-zeep.org/a.xsd", node_a)

    document = wsdl.Document(
        wsdl_content, transport, "https://tests.python-zeep.org/content.wsdl"
    )
