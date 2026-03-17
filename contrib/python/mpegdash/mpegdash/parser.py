from urllib.request import urlopen
from xml.dom import minidom

from mpegdash.nodes import MPEGDASH
from mpegdash.utils import parse_child_nodes, write_child_node
from mpegdash.prettyprinter import pretty_print


class MPEGDASHParser(object):
    @classmethod
    def load_xmldom(cls, string_or_url):
        if '<MPD' in string_or_url:
            mpd_string = string_or_url
        else:
            try:
                mpd_string = urlopen(string_or_url).read()
            except ValueError:
                with open(string_or_url, 'r') as f:
                    mpd_string = f.read()

        return minidom.parseString(mpd_string)

    @classmethod
    def parse(cls, string_or_url):
        xml_root_node = cls.load_xmldom(string_or_url)
        return parse_child_nodes(xml_root_node, 'MPD', MPEGDASH)[0]

    @classmethod
    def get_as_doc(cls, mpd):
        xml_doc = minidom.Document()
        write_child_node(xml_doc, 'MPD', mpd)
        return xml_doc

    @classmethod
    def write(cls, mpd, filepath):
        with open(filepath, 'w') as f:
            cls.get_as_doc(mpd).writexml(f, indent='    ', addindent='    ', newl='\n')

    @classmethod
    def toprettyxml(cls, mpd):
        return pretty_print(cls.get_as_doc(mpd).toxml())
