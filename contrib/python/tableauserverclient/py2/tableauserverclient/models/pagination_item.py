import xml.etree.ElementTree as ET


class PaginationItem(object):
    def __init__(self):
        self._page_number = None
        self._page_size = None
        self._total_available = None

    @property
    def page_number(self):
        return self._page_number

    @property
    def page_size(self):
        return self._page_size

    @property
    def total_available(self):
        return self._total_available

    @classmethod
    def from_response(cls, resp, ns):
        parsed_response = ET.fromstring(resp)
        pagination_xml = parsed_response.find('t:pagination', namespaces=ns)
        pagination_item = cls()
        if pagination_xml is not None:
            pagination_item._page_number = int(pagination_xml.get('pageNumber', '-1'))
            pagination_item._page_size = int(pagination_xml.get('pageSize', '-1'))
            pagination_item._total_available = int(pagination_xml.get('totalAvailable', '-1'))
        return pagination_item

    @classmethod
    def from_single_page_list(cls, single_page_list):
        item = cls()
        item._page_number = 1
        item._page_size = len(single_page_list)
        item._total_available = len(single_page_list)

        return item
