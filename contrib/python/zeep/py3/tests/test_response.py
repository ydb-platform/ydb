from lxml import etree

from zeep.xsd import Schema


def test_parse_response():
    schema_node = etree.fromstring(
        b"""
        <?xml version="1.0"?>
        <wsdl:definitions
            xmlns="http://www.w3.org/2001/XMLSchema"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:tns="http://tests.python-zeep.org/">
          <wsdl:types>
            <schema targetNamespace="http://tests.python-zeep.org/"
                xmlns:tns="http://tests.python-zeep.org/"
                elementFormDefault="qualified">
              <complexType name="Item">
                <sequence>
                  <element minOccurs="0" maxOccurs="1" name="Key" type="string" />
                  <element minOccurs="1" maxOccurs="1" name="Value" type="int" />
                </sequence>
              </complexType>
              <complexType name="ArrayOfItems">
                <sequence>
                  <element minOccurs="0" maxOccurs="unbounded" name="Item" nillable="true" type="tns:Item" />
                </sequence>
              </complexType>
              <complexType name="ZeepExampleResult">
                <sequence>
                  <element minOccurs="1" maxOccurs="1" name="SomeValue" type="int" />
                  <element minOccurs="0" maxOccurs="1" name="Results"
                    type="tns:ArrayOfItems" />
                </sequence>
              </complexType>
              <element name="ZeepExampleResponse">
                <complexType>
                  <sequence>
                    <element minOccurs="0" maxOccurs="1" name="ZeepExampleResult" type="tns:ZeepExampleResult" />
                  </sequence>
                </complexType>
              </element>
            </schema>
          </wsdl:types>
        </wsdl:definitions>
    """.strip()
    )  # noqa

    response_node = etree.fromstring(
        b"""
        <?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope
            xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap:Body>
            <ZeepExampleResponse xmlns="http://tests.python-zeep.org/">
              <ZeepExampleResult>
                <SomeValue>45313</SomeValue>
                <Results>
                  <Item>
                    <Key>ABC100</Key>
                    <Value>10</Value>
                  </Item>
                  <Item>
                    <Key>ABC200</Key>
                    <Value>20</Value>
                  </Item>
                </Results>
              </ZeepExampleResult>
            </ZeepExampleResponse>
          </soap:Body>
        </soap:Envelope>
    """.strip()
    )
    schema = Schema(schema_node.find("*/{http://www.w3.org/2001/XMLSchema}schema"))
    assert schema
    response_type = schema.get_element(
        "{http://tests.python-zeep.org/}ZeepExampleResponse"
    )

    nsmap = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "tns": "http://tests.python-zeep.org/",
    }
    node = response_node.find("soap:Body/tns:ZeepExampleResponse", namespaces=nsmap)
    assert node is not None
    obj = response_type.parse(node, schema)
    assert obj.ZeepExampleResult.SomeValue == 45313
    assert len(obj.ZeepExampleResult.Results.Item) == 2
    assert obj.ZeepExampleResult.Results.Item[0].Key == "ABC100"
    assert obj.ZeepExampleResult.Results.Item[0].Value == 10
    assert obj.ZeepExampleResult.Results.Item[1].Key == "ABC200"
    assert obj.ZeepExampleResult.Results.Item[1].Value == 20
