import pytest

from zeep import Client

import yatest.common


def test_factory_namespace():
    client = Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    factory = client.type_factory("http://example.com/stockquote.xsd")
    obj = factory.Address(NameFirst="Michael", NameLast="van Tellingen")
    assert obj.NameFirst == "Michael"
    assert obj.NameLast == "van Tellingen"


def test_factory_no_reference():
    client = Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))

    obj_1 = client.get_type("ns0:ArrayOfAddress")()
    obj_1.Address.append({"NameFirst": "J", "NameLast": "Doe"})
    obj_2 = client.get_type("ns0:ArrayOfAddress")()
    assert len(obj_2.Address) == 0


def test_factory_ns_auto_prefix():
    client = Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    factory = client.type_factory("ns0")
    obj = factory.Address(NameFirst="Michael", NameLast="van Tellingen")
    assert obj.NameFirst == "Michael"
    assert obj.NameLast == "van Tellingen"


def test_factory_ns_custom_prefix():
    client = Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))
    client.set_ns_prefix("sq", "http://example.com/stockquote.xsd")
    factory = client.type_factory("sq")
    obj = factory.Address(NameFirst="Michael", NameLast="van Tellingen")
    assert obj.NameFirst == "Michael"
    assert obj.NameLast == "van Tellingen"


def test_factory_ns_unknown_prefix():
    client = Client(yatest.common.test_source_path("wsdl_files/soap.wsdl"))

    with pytest.raises(ValueError):
        client.type_factory("bla")
