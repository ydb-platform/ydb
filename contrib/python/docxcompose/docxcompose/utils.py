import re

from docx.oxml.ns import nsmap
from docx.oxml.xmlchemy import BaseOxmlElement


NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "asvg": "http://schemas.microsoft.com/office/drawing/2016/SVG/main",
    "cp": "http://schemas.openxmlformats.org/officeDocument/2006/custom-properties",
    "o": "urn:schemas-microsoft-com:office:office",
    "pic": "http://schemas.openxmlformats.org/drawingml/2006/picture",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "v": "urn:schemas-microsoft-com:vml",
    "vt": "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes",
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "wp": "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing",
    "dgm": "http://schemas.openxmlformats.org/drawingml/2006/diagram",
}
nsmap.update(NS)


def xpath(element, xpath_str):
    """Performs an XPath query on the given element and returns all matching
    elements.
    Works with lxml.etree._Element and with
    docx.oxml.xmlchemy.BaseOxmlElement elements.
    """
    if isinstance(element, BaseOxmlElement):
        return element.xpath(xpath_str)
    else:
        return element.xpath(xpath_str, namespaces=NS)


# Format specifications for docproperties can be found at
# https://support.microsoft.com/en-us/office/format-field-results-baa61f5a-5636-4f11-ab4f-6c36ae43508c?ui=en-US&rs=en-US&ad=US#ID0EAABAAA=Date-Time_format_switch_(\@)
date_format_map = (
    ("Y", "y"),  # Upper or lower case Y are equivalent
    ("D", "d"),
    ("dddd", "EEEE"),
    ("ddd", "E"),
    ("am/pm", "AM/PM"),  # Upper or lower case are equivalent
    ("AM/PM", "a"),
)


def word_to_python_date_format(format_str):
    for word_format, python_format in date_format_map:
        format_str = re.sub(word_format, python_format, format_str)
    return format_str
