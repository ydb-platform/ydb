from lxml import etree


def load_xml(xml):
    parser = etree.XMLParser(
        remove_blank_text=True, remove_comments=True, resolve_entities=False
    )
    return etree.fromstring(xml.strip(), parser=parser)


def assert_nodes_equal(result, expected):
    def _convert_node(node):
        if isinstance(node, (str, bytes)):
            return load_xml(node)
        return node

    # assert node_1 == node_2
    result = etree.tostring(_convert_node(result), pretty_print=True)
    expected = etree.tostring(_convert_node(expected), pretty_print=True)

    result = result.decode("utf-8")
    expected = expected.decode("utf-8")
    assert result == expected


def render_node(element, value):
    node = etree.Element("document")
    element.render(node, value)
    return node


class DummyTransport:
    def __init__(self):
        self._items = {}

    def bind(self, url, node):
        self._items[url] = node

    def load(self, url):
        data = self._items[url]
        if isinstance(data, (bytes, str)):
            return data
        return etree.tostring(data)
