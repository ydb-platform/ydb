from typing import Optional, List
from defusedxml.ElementTree import fromstring
import xml.etree.ElementTree as ET


class ExtractItem:
    """
    An extract refresh task item.

    Attributes
    ----------
    id : str
        The ID of the extract refresh task
    priority : int
        The priority of the task
    type : str
        The type of extract refresh (incremental or full)
    workbook_id : str, optional
        The ID of the workbook if this is a workbook extract
    datasource_id : str, optional
        The ID of the datasource if this is a datasource extract
    """

    def __init__(
        self, priority: int, refresh_type: str, workbook_id: Optional[str] = None, datasource_id: Optional[str] = None
    ):
        self._id: Optional[str] = None
        self._priority = priority
        self._type = refresh_type
        self._workbook_id = workbook_id
        self._datasource_id = datasource_id

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def type(self) -> str:
        return self._type

    @property
    def workbook_id(self) -> Optional[str]:
        return self._workbook_id

    @property
    def datasource_id(self) -> Optional[str]:
        return self._datasource_id

    @classmethod
    def from_response(cls, resp: str, ns: dict) -> List["ExtractItem"]:
        """Create ExtractItem objects from XML response."""
        parsed_response = fromstring(resp)
        return cls.from_xml_element(parsed_response, ns)

    @classmethod
    def from_xml_element(cls, parsed_response: ET.Element, ns: dict) -> List["ExtractItem"]:
        """Create ExtractItem objects from XML element."""
        all_extract_items = []
        all_extract_xml = parsed_response.findall(".//t:extract", namespaces=ns)

        for extract_xml in all_extract_xml:
            extract_id = extract_xml.get("id", None)
            priority = int(extract_xml.get("priority", 0))
            refresh_type = extract_xml.get("type", "")

            # Check for workbook or datasource
            workbook_elem = extract_xml.find(".//t:workbook", namespaces=ns)
            datasource_elem = extract_xml.find(".//t:datasource", namespaces=ns)

            workbook_id = workbook_elem.get("id", None) if workbook_elem is not None else None
            datasource_id = datasource_elem.get("id", None) if datasource_elem is not None else None

            extract_item = cls(priority, refresh_type, workbook_id, datasource_id)
            extract_item._id = extract_id

            all_extract_items.append(extract_item)

        return all_extract_items
