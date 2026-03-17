from lxml import etree

from tests.utils import load_xml
from zeep import xsd


def test_parse_xmlelements_mismatching_namespace():
    schema = xsd.Schema(
        load_xml(
            b"""
        <?xml version="1.0" encoding="utf-8"?>
        <xsd:schema xmlns:tns="http://tests.python-zeep.org/"
          xmlns:xsd="http://www.w3.org/2001/XMLSchema"
          elementFormDefault="qualified"
          targetNamespace="http://tests.python-zeep.org/">
          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element name="item" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>
        </xsd:schema>
    """
        )
    )

    container = schema.get_element("{http://tests.python-zeep.org/}container")
    schema.settings.strict = False

    expected = etree.fromstring(
        """
        <ns0:container
            xmlns:ns0="http://mismatch.python-zeep.org/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <ns0:item>foobar</ns0:item>
        </ns0:container>
    """
    )
    obj = container.parse(expected, schema)
    assert obj.item == "foobar"
