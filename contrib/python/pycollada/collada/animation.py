####################################################################
#                                                                  #
# THIS FILE IS PART OF THE pycollada LIBRARY SOURCE CODE.          #
# USE, DISTRIBUTION AND REPRODUCTION OF THIS LIBRARY SOURCE IS     #
# GOVERNED BY A BSD-STYLE SOURCE LICENSE INCLUDED WITH THIS SOURCE #
# IN 'COPYING'. PLEASE READ THESE TERMS BEFORE DISTRIBUTING.       #
#                                                                  #
# THE pycollada SOURCE CODE IS (C) COPYRIGHT 2011                  #
# by Jeff Terrace and contributors                                 #
#                                                                  #
####################################################################

"""Contains objects representing animations."""

from collada import source
from collada.common import DaeObject
from collada.common import DaeError


class Animation(DaeObject):
    """Class for holding animation data coming from <animation> tags."""

    def __init__(self, id, name, sourceById, children, xmlnode=None):
        self.id = id
        self.name = name
        self.children = children
        self.sourceById = sourceById
        self.xmlnode = xmlnode
        if self.xmlnode is None:
            self.xmlnode = None

    @staticmethod
    def load(collada, localscope, node):
        id = node.get('id') or ''
        name = node.get('name') or ''

        sourcebyid = localscope
        sourcenodes = node.findall(collada.tag('source'))
        for sourcenode in sourcenodes:
            ch = source.Source.load(collada, {}, sourcenode)
            sourcebyid[ch.id] = ch

        child_nodes = node.findall(collada.tag('animation'))
        children = []
        for child in child_nodes:
            try:
                child = Animation.load(collada, sourcebyid, child)
                children.append(child)
            except DaeError as ex:
                collada.handleError(ex)

        anim = Animation(id, name, sourcebyid, children, node)
        return anim

    def __str__(self):
        return '<Animation id=%s, children=%d>' % (self.id, len(self.children))

    def __repr__(self):
        return str(self)
