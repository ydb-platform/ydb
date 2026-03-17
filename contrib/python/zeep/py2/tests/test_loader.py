import pytest
from defusedxml import DTDForbidden, EntitiesForbidden
from pytest import raises as assert_raises

from tests.utils import DummyTransport
from zeep.loader import parse_xml
from zeep.settings import Settings


def test_huge_text():
    # libxml2>=2.7.3 has XML_MAX_TEXT_LENGTH 10000000 without XML_PARSE_HUGE
    settings = Settings(xml_huge_tree=True)
    tree = parse_xml(
        u"""
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
         <s:Body>
          <HugeText xmlns="http://hugetext">%s</HugeText>
         </s:Body>
        </s:Envelope>
    """
        % (u"\u00e5" * 10000001),
        DummyTransport(),
        settings=settings,
    )

    assert tree[0][0].text == u"\u00e5" * 10000001


def test_allow_entities_and_dtd():
    xml = u"""
        <!DOCTYPE Author [
          <!ENTITY writer "Donald Duck.">
        ]>
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
         <s:Body>
            <Author>&writer;</Author>
         </s:Body>
        </s:Envelope>
    """
    # DTD is allowed by default in defusexml so we follow this behaviour
    with pytest.raises(DTDForbidden):
        parse_xml(xml, DummyTransport(), settings=Settings(forbid_dtd=True))

    with pytest.raises(EntitiesForbidden):
        parse_xml(xml, DummyTransport())

    tree = parse_xml(xml, DummyTransport(), settings=Settings(forbid_entities=False))

    assert tree[0][0].tag == "Author"
