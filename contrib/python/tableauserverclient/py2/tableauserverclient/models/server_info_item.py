import xml.etree.ElementTree as ET


class ServerInfoItem(object):
    def __init__(self, product_version, build_number, rest_api_version):
        self._product_version = product_version
        self._build_number = build_number
        self._rest_api_version = rest_api_version

    @property
    def product_version(self):
        return self._product_version

    @property
    def build_number(self):
        return self._build_number

    @property
    def rest_api_version(self):
        return self._rest_api_version

    @classmethod
    def from_response(cls, resp, ns):
        parsed_response = ET.fromstring(resp)
        product_version_tag = parsed_response.find('.//t:productVersion', namespaces=ns)
        rest_api_version_tag = parsed_response.find('.//t:restApiVersion', namespaces=ns)

        build_number = product_version_tag.get('build', None)
        product_version = product_version_tag.text
        rest_api_version = rest_api_version_tag.text

        return cls(product_version, build_number, rest_api_version)
