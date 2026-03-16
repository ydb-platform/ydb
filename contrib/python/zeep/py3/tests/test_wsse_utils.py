from lxml import etree

from zeep.wsse import utils


def test_get_security_header():
    doc = etree.fromstring(
        """
        <soap-env:Envelope
            xmlns:ns0="http://example.com/stockquote.xsd"
            xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
            xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        >
          <soap-env:Body>
            <ns0:TradePriceRequest>
              <tickerSymbol>foobar</tickerSymbol>
              <ns0:country/>
            </ns0:TradePriceRequest>
          </soap-env:Body>
        </soap-env:Envelope>
    """.strip()
    )

    element = utils.get_security_header(doc)
    assert (
        element.tag
        == "{http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd}Security"
    )  # noqa
