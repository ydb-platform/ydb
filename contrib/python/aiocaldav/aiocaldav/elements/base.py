#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from lxml import etree
from aiocaldav.lib.namespace import nsmap


class BaseElement(object):
    children = None
    tag = None
    value = None
    attributes = None

    def __init__(self, name=None, value=None):
        self.children = []
        self.attributes = {}
        self.value = None
        if name is not None:
            self.attributes['name'] = name
        if value is not None:
            self.value = value

    def __add__(self, other):
        return self.append(other)

    def __str__(self):
        utf8 = etree.tostring(self.xmlelement(), encoding="utf-8",
                              xml_declaration=True, pretty_print=True)
        return str(utf8, 'utf-8')

    def xmlelement(self):
        root = etree.Element(self.tag, nsmap=nsmap)
        if self.value is not None:
            root.text = self.value
        if len(self.attributes) > 0:
            for k in list(self.attributes.keys()):
                root.set(k, self.attributes[k])
        self.xmlchildren(root)
        return root

    def xmlchildren(self, root):
        for c in self.children:
            root.append(c.xmlelement())

    def append(self, element):
        try:
            iter(element)
            self.children.extend(element)
        except TypeError:
            self.children.append(element)
        return self


class NamedBaseElement(BaseElement):
    def __init__(self, name=None):
        super(NamedBaseElement, self).__init__(name=name)

    def xmlelement(self):
        if self.attributes.get('name') is None:
            raise Exception("name attribute must be defined")
        return super(NamedBaseElement, self).xmlelement()


class ValuedBaseElement(BaseElement):
    def __init__(self, value=None):
        super(ValuedBaseElement, self).__init__(value=value)
