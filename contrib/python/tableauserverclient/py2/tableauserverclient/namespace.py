from xml.etree import ElementTree as ET
import re

OLD_NAMESPACE = 'http://tableausoftware.com/api'
NEW_NAMESPACE = 'http://tableau.com/api'
NAMESPACE_RE = re.compile(r'\{(.*?)\}')


class UnknownNamespaceError(Exception):
    pass


class Namespace(object):
    def __init__(self):
        self._namespace = {'t': NEW_NAMESPACE}
        self._detected = False

    def __call__(self):
        return self._namespace

    def detect(self, xml):
        if self._detected:
            return

        if not xml.startswith(b'<?xml'):
            return  # Not an xml file, don't detect anything

        root = ET.fromstring(xml)
        matches = NAMESPACE_RE.match(root.tag)
        if matches:
            detected_ns = matches.group(1)
            if detected_ns in (OLD_NAMESPACE, NEW_NAMESPACE):
                self._namespace = {'t': detected_ns}
                self._detected = True
            else:
                raise UnknownNamespaceError(detected_ns)
