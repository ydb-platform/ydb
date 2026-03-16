import pytest
from lxml import etree

from tests.utils import DummyTransport, assert_nodes_equal, load_xml, render_node
from zeep import exceptions, xsd
from zeep.xsd import Schema
from zeep.xsd.types.unresolved import UnresolvedType

import yatest.common as yc


def test_default_types():
    schema = xsd.Schema()
    xsd_string = schema.get_type("{http://www.w3.org/2001/XMLSchema}string")
    assert xsd_string == xsd.String()


def test_default_types_not_found():
    schema = xsd.Schema()
    with pytest.raises(exceptions.LookupError):
        schema.get_type("{http://www.w3.org/2001/XMLSchema}bar")


def test_default_elements():
    schema = xsd.Schema()
    xsd_schema = schema.get_element("{http://www.w3.org/2001/XMLSchema}schema")
    isinstance(xsd_schema, Schema)


def test_default_elements_not_found():
    schema = xsd.Schema()
    with pytest.raises(exceptions.LookupError):
        schema.get_element("{http://www.w3.org/2001/XMLSchema}bar")


def test_invalid_namespace_handling():
    schema = xsd.Schema()
    qname = "{http://tests.python-zeep.org/404}foo"

    with pytest.raises(exceptions.NamespaceError) as exc:
        schema.get_element(qname)
    assert qname in str(exc.value.message)

    with pytest.raises(exceptions.NamespaceError) as exc:
        schema.get_type(qname)
    assert qname in str(exc.value.message)

    with pytest.raises(exceptions.NamespaceError) as exc:
        schema.get_group(qname)
    assert qname in str(exc.value.message)

    with pytest.raises(exceptions.NamespaceError) as exc:
        schema.get_attribute(qname)
    assert qname in str(exc.value.message)

    with pytest.raises(exceptions.NamespaceError) as exc:
        schema.get_attribute_group(qname)
    assert qname in str(exc.value.message)


def test_invalid_localname_handling():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
        </xs:schema>
    """
        )
    )

    qname = "{http://tests.python-zeep.org/}foo"
    namespace = "http://tests.python-zeep.org/"
    localname = "foo"

    with pytest.raises(exceptions.LookupError) as exc:
        schema.get_element(qname)
    assert namespace in str(exc.value.message)
    assert localname in str(exc.value.message)

    with pytest.raises(exceptions.LookupError) as exc:
        schema.get_type(qname)
    assert namespace in str(exc.value.message)
    assert localname in str(exc.value.message)

    with pytest.raises(exceptions.LookupError) as exc:
        schema.get_group(qname)
    assert namespace in str(exc.value.message)
    assert localname in str(exc.value.message)

    with pytest.raises(exceptions.LookupError) as exc:
        schema.get_attribute(qname)
    assert namespace in str(exc.value.message)
    assert localname in str(exc.value.message)

    with pytest.raises(exceptions.LookupError) as exc:
        schema.get_attribute_group(qname)
    assert namespace in str(exc.value.message)
    assert localname in str(exc.value.message)


def test_schema_repr_none():
    schema = xsd.Schema()
    assert repr(schema) == "<Schema()>"


def test_schema_repr_val():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
        </xs:schema>
    """
        )
    )
    assert (
        repr(schema) == "<Schema(location=None, tns='http://tests.python-zeep.org/')>"
    )


def test_schema_doc_repr_val():
    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
        </xs:schema>
    """
        )
    )
    docs = schema._get_schema_documents("http://tests.python-zeep.org/")
    assert len(docs) == 1
    doc = docs[0]
    assert (
        repr(doc)
        == "<SchemaDocument(location=None, tns='http://tests.python-zeep.org/', is_empty=True)>"
    )


def test_multiple_extension():
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

            <xs:complexType name="type_a">
              <xs:complexContent>
                <xs:extension base="b:type_b"/>
              </xs:complexContent>
            </xs:complexType>
            <xs:element name="typetje" type="tns:type_a"/>

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
            xmlns:c="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/c.xsd"
                namespace="http://tests.python-zeep.org/c"/>

            <xs:complexType name="type_b">
              <xs:complexContent>
                <xs:extension base="c:type_c"/>
              </xs:complexContent>
            </xs:complexType>
        </xs:schema>
    """.strip()
    )

    node_c = etree.fromstring(
        """
        <?xml version="1.0"?>
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
    """.strip()
    )
    etree.XMLSchema(node_c)

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", node_c)

    schema = xsd.Schema(node_a, transport=transport)
    type_a = schema.get_type("ns0:type_a")
    type_a(wat="x")


def test_global_element_and_type():
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

            <xs:complexType name="refs">
              <xs:sequence>
                <xs:element ref="b:ref_elm"/>
              </xs:sequence>
              <xs:attribute ref="b:ref_attr"/>
            </xs:complexType>

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
            xmlns:c="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/c.xsd"
                namespace="http://tests.python-zeep.org/c"/>

            <xs:element name="ref_elm" type="xs:string"/>
            <xs:attribute name="ref_attr" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    node_c = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/c"
            targetNamespace="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:complexType name="type_a">
              <xs:sequence>
                <xs:element name="item_a" type="xs:string"/>
              </xs:sequence>
            </xs:complexType>
            <xs:element name="item" type="xs:string"/>
        </xs:schema>
    """.strip()
    )
    etree.XMLSchema(node_c)

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", node_c)

    schema = xsd.Schema(node_a, transport=transport)
    type_a = schema.get_type("{http://tests.python-zeep.org/c}type_a")

    type_a = schema.get_type("{http://tests.python-zeep.org/c}type_a")
    type_a(item_a="x")

    elm = schema.get_element("{http://tests.python-zeep.org/c}item")
    elm("x")

    elm = schema.get_type("{http://tests.python-zeep.org/a}refs")
    elm(ref_elm="foo", ref_attr="bar")


def test_cyclic_imports():
    schema_a = etree.fromstring(
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

    schema_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            xmlns:c="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/c.xsd"
                namespace="http://tests.python-zeep.org/c"/>
        </xs:schema>
    """.strip()
    )

    schema_c = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/c"
            targetNamespace="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/a.xsd"
                namespace="http://tests.python-zeep.org/a"/>
        </xs:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", schema_c)
    xsd.Schema(
        schema_a, transport=transport, location="http://tests.python-zeep.org/a.xsd"
    )


def test_get_type_through_import():
    schema_a = etree.fromstring(
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
            <xs:element name="foo" type="b:bar"/>

        </xs:schema>
    """.strip()
    )

    schema_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            xmlns:c="http://tests.python-zeep.org/c"
            elementFormDefault="qualified">

            <xs:complexType name="bar"/>

        </xs:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_b)
    xsd.Schema(schema_a, transport=transport)


def test_duplicate_target_namespace():
    schema_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            elementFormDefault="qualified">

            <xs:import
                schemaLocation="http://tests.python-zeep.org/b.xsd"
                namespace="http://tests.python-zeep.org/duplicate"/>
            <xs:import
                schemaLocation="http://tests.python-zeep.org/c.xsd"
                namespace="http://tests.python-zeep.org/duplicate"/>
        </xs:schema>
    """.strip()
    )

    schema_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/duplicate"
            targetNamespace="http://tests.python-zeep.org/duplicate"
            elementFormDefault="qualified">
            <xsd:element name="elm-in-b" type="tns:item-c"/>
            <xsd:complexType name="item-c">
              <xsd:sequence>
                <xsd:element name="item-a" type="xsd:string"/>
                <xsd:element name="item-b" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>
        </xsd:schema>
    """.strip()
    )

    schema_c = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/duplicate"
            targetNamespace="http://tests.python-zeep.org/duplicate"
            elementFormDefault="qualified">
            <xsd:element name="elm-in-c" type="tns:item-c"/>
            <xsd:complexType name="item-c">
              <xsd:sequence>
                <xsd:element name="item-a" type="xsd:string"/>
                <xsd:element name="item-b" type="xsd:string"/>
              </xsd:sequence>
            </xsd:complexType>

        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/a.xsd", schema_a)
    transport.bind("http://tests.python-zeep.org/b.xsd", schema_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", schema_c)
    schema = xsd.Schema(schema_a, transport=transport)

    elm_b = schema.get_element("{http://tests.python-zeep.org/duplicate}elm-in-b")
    elm_c = schema.get_element("{http://tests.python-zeep.org/duplicate}elm-in-c")
    assert not isinstance(elm_b.type, UnresolvedType)
    assert not isinstance(elm_c.type, UnresolvedType)


def test_multiple_no_namespace():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            elementFormDefault="qualified">

          <xsd:import schemaLocation="http://tests.python-zeep.org/b.xsd"/>
          <xsd:import schemaLocation="http://tests.python-zeep.org/c.xsd"/>
        </xsd:schema>
    """.strip()
    )

    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified">
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", node_b)
    xsd.Schema(node_a, transport=transport)


def test_multiple_only_target_ns():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            elementFormDefault="qualified">

          <xsd:import schemaLocation="http://tests.python-zeep.org/b.xsd"/>
          <xsd:import schemaLocation="http://tests.python-zeep.org/c.xsd"/>
        </xsd:schema>
    """.strip()
    )

    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified"
            targetNamespace="http://tests.python-zeep.org/duplicate-ns">
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", node_b)
    xsd.Schema(node_a, transport=transport)


def test_schema_error_handling():
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
    transport = DummyTransport()
    schema = xsd.Schema(node_a, transport=transport)

    with pytest.raises(ValueError):
        schema.get_element("nonexisting:something")
    with pytest.raises(ValueError):
        schema.get_type("nonexisting:something")
    with pytest.raises(exceptions.NamespaceError):
        schema.get_element("{nonexisting}something")
    with pytest.raises(exceptions.NamespaceError):
        schema.get_type("{nonexisting}something")
    with pytest.raises(exceptions.LookupError):
        schema.get_element("ns0:something")
    with pytest.raises(exceptions.LookupError):
        schema.get_type("ns0:something")


def test_schema_import_xmlsoap():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
          <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
        </xsd:schema>
    """.strip()
    )
    transport = DummyTransport()
    xsd.Schema(node_a, transport=transport)


def test_schema_import_unresolved():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
          <xsd:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
        </xsd:schema>
    """.strip()
    )
    transport = DummyTransport()
    xsd.Schema(node_a, transport=transport)


def test_no_target_namespace():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            elementFormDefault="qualified">

          <xsd:import schemaLocation="http://tests.python-zeep.org/b.xsd"/>

          <xsd:element name="container">
            <xsd:complexType>
              <xsd:sequence>
                <xsd:element ref="bla"/>
              </xsd:sequence>
            </xsd:complexType>
          </xsd:element>

        </xsd:schema>
    """.strip()
    )

    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            elementFormDefault="qualified">
            <xsd:element name="bla" type="xsd:string"/>
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    xsd.Schema(node_a, transport=transport)


def test_include_recursion():
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

            <xs:include schemaLocation="http://tests.python-zeep.org/c.xsd"/>
            <xs:element name="bar" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    node_c = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b"
            targetNamespace="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">

            <xs:include schemaLocation="http://tests.python-zeep.org/b.xsd"/>

            <xs:element name="foo" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/c.xsd", node_c)

    schema = xsd.Schema(node_a, transport=transport)
    schema.get_element("{http://tests.python-zeep.org/b}foo")
    schema.get_element("{http://tests.python-zeep.org/b}bar")


def test_include_relative():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns="http://tests.python-zeep.org/tns"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/a"
            elementFormDefault="qualified">

            <xs:include schemaLocation="http://tests.python-zeep.org/subdir/b.xsd"/>

        </xs:schema>
    """.strip()
    )

    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
            <xs:include schemaLocation="c.xsd"/>
            <xs:element name="bar" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    node_c = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
            <xs:element name="foo" type="xs:string"/>
        </xs:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/subdir/b.xsd", node_b)
    transport.bind("http://tests.python-zeep.org/subdir/c.xsd", node_c)

    schema = xsd.Schema(node_a, transport=transport)
    schema.get_element("{http://tests.python-zeep.org/a}foo")
    schema.get_element("{http://tests.python-zeep.org/a}bar")


def test_include_no_default_namespace():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns="http://tests.python-zeep.org/tns"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/tns"
            elementFormDefault="qualified">

            <xs:include
                schemaLocation="http://tests.python-zeep.org/b.xsd"/>

            <xs:element name="container" type="foo"/>
        </xs:schema>
    """.strip()
    )

    # include without default namespace, other xsd prefix
    node_b = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xsd:schema
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b">

            <xsd:simpleType name="my-string">
              <xsd:restriction base="xsd:boolean"/>
            </xsd:simpleType>

            <xsd:complexType name="foo">
              <xsd:sequence>
                <xsd:element name="item" type="my-string"/>
              </xsd:sequence>
            </xsd:complexType>
        </xsd:schema>
    """.strip()
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)
    schema = xsd.Schema(node_a, transport=transport)
    item = schema.get_element("{http://tests.python-zeep.org/tns}container")
    assert item


def test_include_no_parent_default_namespace():
    schema_root = """
        <?xml version="1.0"?>
        <xs:schema xmlns="http://tests.python-zeep.org/rootns" xmlns:tns="http://tests.python-zeep.org/tns" xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="http://tests.python-zeep.org/rootns" elementFormDefault="qualified">
            <xs:import namespace="http://tests.python-zeep.org/tns" schemaLocation="http://tests.python-zeep.org/tns.xsd"/>
            <xs:element name="root">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="container" type="tns:containerType" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:schema>
    """.strip()

    # no default namespace, but targetNamespace
    schema_tns = """
        <?xml version="1.0"?>
        <xs:schema xmlns:tns="http://tests.python-zeep.org/tns" targetNamespace="http://tests.python-zeep.org/tns" xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
            <xs:include schemaLocation="http://tests.python-zeep.org/include.xsd" />
        </xs:schema>
        """.strip()

    # no default namespace and no targetNamespace
    schema_include = """
        <?xml version="1.0"?>
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
            <xs:complexType name="containerType">
                <xs:sequence>
                    <xs:element name="item" type="itemType" />
                </xs:sequence>
            </xs:complexType>
            <xs:complexType name="itemType">
              <xs:sequence>
                <xs:element name="intVal" type="xs:int" />
                <xs:element name="boolVal" type="xs:boolean" />
              </xs:sequence>
            </xs:complexType>
        </xs:schema>
        """.strip()

    class IncludeSchemaResolver(etree.Resolver):
        def resolve(self, url, id, context):
            if url == "http://tests.python-zeep.org/tns.xsd":
                return self.resolve_string(schema_tns, context)
            elif url == "http://tests.python-zeep.org/include.xsd":
                return self.resolve_string(schema_include, context)

    parser = etree.XMLParser()
    parser.resolvers.add(IncludeSchemaResolver())

    schema = etree.XMLSchema(etree.fromstring(schema_root, parser=parser))

    xml = """
        <?xml version="1.0"?>
        <root xmlns="http://tests.python-zeep.org/rootns">
            <container xmlns:tns="http://tests.python-zeep.org/tns">
                <tns:item>
                    <tns:intVal>42</tns:intVal>
                    <tns:boolVal>true</tns:boolVal>
                </tns:item>
            </container>
        </root>
    """.strip()

    xml = etree.fromstring(xml)
    schema.assertValid(xml)  # schema is ok for lxml

    schema_root = etree.fromstring(schema_root)
    schema_tns = etree.fromstring(schema_tns)
    schema_include = etree.fromstring(schema_include)

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/tns.xsd", schema_tns)
    transport.bind("http://tests.python-zeep.org/include.xsd", schema_include)
    xsd.Schema(schema_root, transport=transport)


def test_include_different_form_defaults():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns="http://tests.python-zeep.org/"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            targetNamespace="http://tests.python-zeep.org/">

            <xs:include
                schemaLocation="http://tests.python-zeep.org/b.xsd"/>
        </xs:schema>
    """.strip()
    )

    # include without default namespace, other xsd prefix
    node_b = load_xml(
        """
        <?xml version="1.0"?>
        <xsd:schema
            elementFormDefault="qualified"
            attributeFormDefault="qualified"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/b">

            <xsd:element name="container" type="foo"/>

            <xsd:complexType name="foo">
              <xsd:sequence>
                <xsd:element name="item" type="xsd:string"/>
              </xsd:sequence>
              <xsd:attribute name="attr" type="xsd:string"/>
            </xsd:complexType>
        </xsd:schema>
    """
    )

    transport = DummyTransport()
    transport.bind("http://tests.python-zeep.org/b.xsd", node_b)

    schema = xsd.Schema(node_a, transport=transport)
    item = schema.get_element("{http://tests.python-zeep.org/}container")
    obj = item(item="foo", attr="bar")
    node = render_node(item, obj)

    expected = load_xml(
        """
        <document>
          <ns0:container xmlns:ns0="http://tests.python-zeep.org/" ns0:attr="bar">
            <ns0:item>foo</ns0:item>
          </ns0:container>
        </document>
    """
    )
    assert_nodes_equal(expected, node)


def test_merge():
    node_a = etree.fromstring(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/a"
            targetNamespace="http://tests.python-zeep.org/a"
            xmlns:b="http://tests.python-zeep.org/b"
            elementFormDefault="qualified">
          <xs:element name="foo" type="xs:string"/>
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
          <xs:element name="foo" type="xs:int"/>
        </xs:schema>
    """.strip()
    )

    schema_a = xsd.Schema(node_a)
    schema_b = xsd.Schema(node_b)
    schema_a.merge(schema_b)

    schema_a.get_element("{http://tests.python-zeep.org/a}foo")
    schema_a.get_element("{http://tests.python-zeep.org/b}foo")


def test_xml_namespace():
    xmlns = load_xml(
        """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:xml="http://www.w3.org/XML/1998/namespace"
            targetNamespace="http://www.w3.org/XML/1998/namespace"
            elementFormDefault="qualified">
          <xs:attribute name="lang" type="xs:string"/>
        </xs:schema>
    """
    )

    transport = DummyTransport()
    transport.bind("http://www.w3.org/2001/xml.xsd", xmlns)

    xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xs:import namespace="http://www.w3.org/XML/1998/namespace"
                     schemaLocation="http://www.w3.org/2001/xml.xsd"/>
          <xs:element name="container">
            <xs:complexType>
              <xs:sequence/>
              <xs:attribute ref="xml:lang"/>
            </xs:complexType>
          </xs:element>
        </xs:schema>
    """
        ),
        transport=transport,
    )


def test_auto_import_known_schema():
    content = open(yc.test_source_path("wsdl_files/soap-enc.xsd"), "rb").read()

    transport = DummyTransport()
    transport.bind("http://schemas.xmlsoap.org/soap/encoding/", content)

    schema = xsd.Schema(
        load_xml(
            """
        <?xml version="1.0"?>
        <xs:schema
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:soap-enc="http://schemas.xmlsoap.org/soap/encoding/"
            xmlns:tns="http://tests.python-zeep.org/"
            targetNamespace="http://tests.python-zeep.org/"
            elementFormDefault="qualified">
          <xs:import namespace="http://schemas.xmlsoap.org/soap/encoding/"/>
          <xs:group ref="soap-enc:Struct"/>
        </xs:schema>
    """
        ),
        transport=transport,
    )
    schema.set_ns_prefix("soap-enc", "http://schemas.xmlsoap.org/soap/encoding/")
    schema.get_group("soap-enc:Struct")
