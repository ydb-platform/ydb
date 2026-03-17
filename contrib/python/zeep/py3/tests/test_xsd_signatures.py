from lxml import etree

from tests.utils import load_xml
from zeep import xsd


def test_signature_complex_type_choice():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Choice(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                ]
            )
        ),
    )
    assert (
        custom_type.signature()
        == "{http://tests.python-zeep.org/}authentication(({item_1: xsd:string} | {item_2: xsd:string}))"
    )


def test_signature_complex_type_choice_sequence():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Choice(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Sequence(
                        [
                            xsd.Element(
                                etree.QName(
                                    "http://tests.python-zeep.org/", "item_2_1"
                                ),
                                xsd.String(),
                            ),
                            xsd.Element(
                                etree.QName(
                                    "http://tests.python-zeep.org/", "item_2_2"
                                ),
                                xsd.String(),
                            ),
                        ]
                    ),
                ]
            )
        ),
    )
    assert custom_type.signature() == (
        "{http://tests.python-zeep.org/}authentication(({item_1: xsd:string} | {item_2_1: xsd:string, item_2_2: xsd:string}))"
    )


def test_signature_nested_sequences():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                    xsd.Sequence(
                        [
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_3"),
                                xsd.String(),
                            ),
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_4"),
                                xsd.String(),
                            ),
                        ]
                    ),
                    xsd.Choice(
                        [
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_5"),
                                xsd.String(),
                            ),
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_6"),
                                xsd.String(),
                            ),
                            xsd.Sequence(
                                [
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "item_5"
                                        ),
                                        xsd.String(),
                                    ),
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "item_6"
                                        ),
                                        xsd.String(),
                                    ),
                                ]
                            ),
                        ]
                    ),
                ]
            )
        ),
    )

    assert custom_type.signature() == (
        "{http://tests.python-zeep.org/}authentication(item_1: xsd:string, item_2: xsd:string, item_3: xsd:string, item_4: xsd:string, ({item_5: xsd:string} | {item_6: xsd:string} | {item_5: xsd:string, item_6: xsd:string}))"
    )


def test_signature_nested_sequences_multiple():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Sequence(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.String(),
                    ),
                    xsd.Sequence(
                        [
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_3"),
                                xsd.String(),
                            ),
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_4"),
                                xsd.String(),
                            ),
                        ]
                    ),
                    xsd.Choice(
                        [
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_5"),
                                xsd.String(),
                            ),
                            xsd.Element(
                                etree.QName("http://tests.python-zeep.org/", "item_6"),
                                xsd.String(),
                            ),
                            xsd.Sequence(
                                [
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "item_5"
                                        ),
                                        xsd.String(),
                                    ),
                                    xsd.Element(
                                        etree.QName(
                                            "http://tests.python-zeep.org/", "item_6"
                                        ),
                                        xsd.String(),
                                    ),
                                ]
                            ),
                        ],
                        min_occurs=2,
                        max_occurs=3,
                    ),
                ]
            )
        ),
    )

    assert custom_type.signature() == (
        "{http://tests.python-zeep.org/}authentication(item_1: xsd:string, item_2: xsd:string, item_3: xsd:string, item_4: xsd:string, ({item_5: xsd:string} | {item_6: xsd:string} | {item_5: xsd:string, item_6: xsd:string})[])"
    )


def test_signature_complex_type_any():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Choice(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Any(),
                ]
            )
        ),
    )
    assert (
        custom_type.signature()
        == "{http://tests.python-zeep.org/}authentication(({item_1: xsd:string} | {_value_1: ANY}))"
    )
    custom_type(item_1="foo")


def test_signature_complex_type_sequence_with_any():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Choice(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.ComplexType(xsd.Sequence([xsd.Any()])),
                    ),
                ]
            )
        ),
    )
    assert custom_type.signature() == (
        "{http://tests.python-zeep.org/}authentication(({item_1: xsd:string} | {item_2: {_value_1: ANY}}))"
    )


def test_signature_complex_type_sequence_with_anys():
    custom_type = xsd.Element(
        etree.QName("http://tests.python-zeep.org/", "authentication"),
        xsd.ComplexType(
            xsd.Choice(
                [
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_1"),
                        xsd.String(),
                    ),
                    xsd.Element(
                        etree.QName("http://tests.python-zeep.org/", "item_2"),
                        xsd.ComplexType(xsd.Sequence([xsd.Any(), xsd.Any()])),
                    ),
                ]
            )
        ),
    )
    assert custom_type.signature() == (
        "{http://tests.python-zeep.org/}authentication("
        + "({item_1: xsd:string} | {item_2: {_value_1: ANY, _value_2: ANY}})"
        + ")"
    )


def test_schema_recursive_ref():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">

          <xsd:element name="Container">
              <xsd:complexType>
                  <xsd:sequence>
                      <xsd:element ref="tns:Container" />
                  </xsd:sequence>
              </xsd:complexType>
          </xsd:element>

        </xsd:schema>
    """
        )
    )

    elm = schema.get_element("ns0:Container")
    elm.signature(schema)
