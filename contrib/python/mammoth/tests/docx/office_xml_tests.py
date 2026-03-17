from __future__ import unicode_literals

import io

from mammoth.docx import xmlparser as xml, office_xml
from ..testing import assert_equal


class AlternateContentTests(object):
    def test_when_fallback_is_present_then_fallback_is_read(self):
        xml_string = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
            '<numbering xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006">' +
            '<mc:AlternateContent>' +
            '<mc:Choice Requires="w14">' +
            '<choice/>' +
            '</mc:Choice>' +
            '<mc:Fallback>' +
            '<fallback/>' +
            '</mc:Fallback>' +
            '</mc:AlternateContent>' +
            '</numbering>')

        result = office_xml.read(io.StringIO(xml_string))
        assert_equal([xml.element("fallback")], result.children)


    def test_when_fallback_is_not_present_then_element_is_ignored(self):
        xml_string = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
            '<numbering xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006">' +
            '<mc:AlternateContent>' +
            '<mc:Choice Requires="w14">' +
            '<choice/>' +
            '</mc:Choice>' +
            '</mc:AlternateContent>' +
            '</numbering>')

        result = office_xml.read(io.StringIO(xml_string))
        assert_equal([], result.children)
