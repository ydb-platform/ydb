# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
Provides XML I{document} classes.
"""

from suds import *
from suds.sax import *
from suds.sax.element import Element


class Document:
    """ An XML Document """

    DECL = '<?xml version="1.0" encoding="UTF-8"?>'

    def __init__(self, root=None):
        """
        @param root: A root L{Element} or name used to build
            the document root element.
        @type root: (L{Element}|str|None)
        """
        self.__root = None
        self.append(root)

    def root(self):
        """
        Get the document root element (can be None)
        @return: The document root.
        @rtype: L{Element}
        """
        return self.__root

    def append(self, node):
        """
        Append (set) the document root.
        @param node: A root L{Element} or name used to build
            the document root element.
        @type node: (L{Element}|str|None)
        """
        if isinstance(node, str):
            self.__root = Element(node)
            return
        if isinstance(node, Element):
            self.__root = node
            return

    def getChild(self, name, ns=None, default=None):
        """
        Get a child by (optional) name and/or (optional) namespace.
        @param name: The name of a child element (may contain prefix).
        @type name: basestring
        @param ns: An optional namespace used to match the child.
        @type ns: (I{prefix}, I{name})
        @param default: Returned when child not-found.
        @type default: L{Element}
        @return: The requested child, or I{default} when not-found.
        @rtype: L{Element}
        """
        if self.__root is None:
            return default
        if ns is None:
            prefix, name = splitPrefix(name)
            if prefix is None:
                ns = None
            else:
                ns = self.__root.resolvePrefix(prefix)
        if self.__root.match(name, ns):
            return self.__root
        else:
            return default

    def childAtPath(self, path):
        """
        Get a child at I{path} where I{path} is a (/) separated
        list of element names that are expected to be children.
        @param path: A (/) separated list of element names.
        @type path: basestring
        @return: The leaf node at the end of I{path}
        @rtype: L{Element}
        """
        if self.__root is None:
            return None
        if path[0] == '/':
            path = path[1:]
        path = path.split('/',1)
        if self.getChild(path[0]) is None:
            return None
        if len(path) > 1:
            return self.__root.childAtPath(path[1])
        else:
            return self.__root

    def childrenAtPath(self, path):
        """
        Get a list of children at I{path} where I{path} is a (/) separated
        list of element names that are expected to be children.
        @param path: A (/) separated list of element names.
        @type path: basestring
        @return: The collection leaf nodes at the end of I{path}
        @rtype: [L{Element},...]
        """
        if self.__root is None:
            return []
        if path[0] == '/':
            path = path[1:]
        path = path.split('/',1)
        if self.getChild(path[0]) is None:
            return []
        if len(path) > 1:
            return self.__root.childrenAtPath(path[1])
        else:
            return [self.__root,]

    def getChildren(self, name=None, ns=None):
        """
        Get a list of children by (optional) name and/or (optional) namespace.
        @param name: The name of a child element (may contain prefix).
        @type name: basestring
        @param ns: An optional namespace used to match the child.
        @type ns: (I{prefix}, I{name})
        @return: The list of matching children.
        @rtype: [L{Element},...]
        """
        if name is None:
            matched = self.__root
        else:
            matched = self.getChild(name, ns)
        if matched is None:
            return []
        else:
            return [matched,]

    def str(self):
        """
        Get a string representation of this XML document.
        @return: A I{pretty} string.
        @rtype: basestring
        """
        s = []
        s.append(self.DECL)
        root = self.root()
        if root is not None:
            s.append('\n')
            s.append(root.str())
        return ''.join(s)

    def plain(self):
        """
        Get a string representation of this XML document.
        @return: A I{plain} string.
        @rtype: basestring
        """
        s = []
        s.append(self.DECL)
        root = self.root()
        if root is not None:
            s.append(root.plain())
        return ''.join(s)

    def __unicode__(self):
        return self.str()
