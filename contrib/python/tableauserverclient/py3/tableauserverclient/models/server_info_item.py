import logging
import warnings
import xml

from defusedxml.ElementTree import fromstring
from tableauserverclient.helpers.logging import logger


class ServerInfoItem:
    """
    The ServerInfoItem class contains the build and version information for
    Tableau Server. The server information is accessed with the
    server_info.get() method, which returns an instance of the ServerInfo class.

    Attributes
    ----------
    product_version : str
        Shows the version of the Tableau Server or Tableau Cloud
        (for example, 10.2.0).

    build_number : str
        Shows the specific build number (for example, 10200.17.0329.1446).

    rest_api_version : str
        Shows the supported REST API version number. Note that this might be
        different from the default value specified for the server, with the
        Server.version attribute. To take advantage of new features, you should
        query the server and set the Server.version to match the supported REST
        API version number.
    """

    def __init__(self, product_version, build_number, rest_api_version):
        self._product_version = product_version
        self._build_number = build_number
        self._rest_api_version = rest_api_version

    def __repr__(self):
        return (
            "ServerInfoItem: [product version: "
            + self._product_version
            + ", build no.:"
            + self._build_number
            + ", REST API version:"
            + self.rest_api_version
            + "]"
        )

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
        try:
            parsed_response = fromstring(resp)
        except xml.etree.ElementTree.ParseError as error:
            logger.exception(f"Unexpected response for ServerInfo: {resp}")
            return cls("Unknown", "Unknown", "Unknown")
        except Exception as error:
            logger.exception(f"Unexpected response for ServerInfo: {resp}")
            raise error

        product_version_tag = parsed_response.find(".//t:productVersion", namespaces=ns)
        rest_api_version_tag = parsed_response.find(".//t:restApiVersion", namespaces=ns)

        build_number = product_version_tag.get("build", None)
        product_version = product_version_tag.text
        rest_api_version = rest_api_version_tag.text

        return cls(product_version, build_number, rest_api_version)
