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

"""Contains COLLADA asset information."""

import datetime
import dateutil.parser

from collada.common import DaeObject, E
from collada.util import _correctValInNode


class UP_AXIS:
    """The up-axis of the collada document."""
    X_UP = 'X_UP'
    """Indicates X direction is up"""
    Y_UP = 'Y_UP'
    """Indicates Y direction is up"""
    Z_UP = 'Z_UP'
    """Indicates Z direction is up"""


class Contributor(DaeObject):
    """Defines authoring information for asset management"""

    def __init__(self, author=None, authoring_tool=None, comments=None, copyright=None, source_data=None, xmlnode=None):
        """Create a new contributor

        :param str author:
          The author's name
        :param str authoring_tool:
          Name of the authoring tool
        :param str comments:
          Comments from the contributor
        :param str copyright:
          Copyright information
        :param str source_data:
          URI referencing the source data
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.author = author
        """Contains a string with the author's name."""
        self.authoring_tool = authoring_tool
        """Contains a string with the name of the authoring tool."""
        self.comments = comments
        """Contains a string with comments from this contributor."""
        self.copyright = copyright
        """Contains a string with copyright information."""
        self.source_data = source_data
        """Contains a string with a URI referencing the source data for this asset."""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the contributor."""
        else:
            self.xmlnode = E.contributor()
            if author is not None:
                self.xmlnode.append(E.author(str(author)))
            if authoring_tool is not None:
                self.xmlnode.append(E.authoring_tool(str(authoring_tool)))
            if comments is not None:
                self.xmlnode.append(E.comments(str(comments)))
            if copyright is not None:
                self.xmlnode.append(E.copyright(str(copyright)))
            if source_data is not None:
                self.xmlnode.append(E.source_data(str(source_data)))

    @staticmethod
    def load(collada, localscope, node):
        author = node.find(collada.tag('author'))
        authoring_tool = node.find(collada.tag('authoring_tool'))
        comments = node.find(collada.tag('comments'))
        copyright = node.find(collada.tag('copyright'))
        source_data = node.find(collada.tag('source_data'))
        if author is not None:
            author = author.text
        if authoring_tool is not None:
            authoring_tool = authoring_tool.text
        if comments is not None:
            comments = comments.text
        if copyright is not None:
            copyright = copyright.text
        if source_data is not None:
            source_data = source_data.text
        return Contributor(author=author, authoring_tool=authoring_tool,
                           comments=comments, copyright=copyright, source_data=source_data, xmlnode=node)

    def save(self):
        """Saves the contributor info back to :attr:`xmlnode`"""
        _correctValInNode(self.xmlnode, 'author', self.author)
        _correctValInNode(self.xmlnode, 'authoring_tool', self.authoring_tool)
        _correctValInNode(self.xmlnode, 'comments', self.comments)
        _correctValInNode(self.xmlnode, 'copyright', self.copyright)
        _correctValInNode(self.xmlnode, 'source_data', self.source_data)

    def __str__(self):
        return '<Contributor author=%s>' % (str(self.author),)

    def __repr__(self):
        return str(self)


class Asset(DaeObject):
    """Defines asset-management information"""

    def __init__(self, created=None, modified=None, title=None, subject=None, revision=None,
                 keywords=None, unitname=None, unitmeter=None, upaxis=None, contributors=None, xmlnode=None):
        """Create a new set of information about an asset

        :param datetime.datetime created:
          When the asset was created. If None, this will be set to the current date and time.
        :param datetime.datetime modified:
          When the asset was modified. If None, this will be set to the current date and time.
        :param str title:
          The title of the asset
        :param str subject:
          The description of the topical subject of the asset
        :param str revision:
          Revision information about the asset
        :param str keywords:
          A list of words used for search criteria for the asset
        :param str unitname:
          The name of the unit of distance for this asset
        :param float unitmeter:
          How many real-world meters are in one distance unit
        :param `collada.asset.UP_AXIS` upaxis:
          The up-axis of the asset. If None, this will be set to Y_UP
        :param list contributors:
          The list of contributors for the asset
        :param xmlnode:
          If loaded from xml, the xml node

        """

        if created is None:
            created = datetime.datetime.now()
        self.created = created
        """Instance of :class:`datetime.datetime` indicating when the asset was created"""

        if modified is None:
            modified = datetime.datetime.now()
        self.modified = modified
        """Instance of :class:`datetime.datetime` indicating when the asset was modified"""

        self.title = title
        """String containing the title of the asset"""
        self.subject = subject
        """String containing the description of the topical subject of the asset"""
        self.revision = revision
        """String containing revision information about the asset"""
        self.keywords = keywords
        """String containing a list of words used for search criteria for the asset"""
        self.unitname = unitname
        """String containing the name of the unit of distance for this asset"""
        self.unitmeter = unitmeter
        """Float containing how many real-world meters are in one distance unit"""

        if upaxis is None:
            upaxis = UP_AXIS.Y_UP
        self.upaxis = upaxis
        """Instance of type :class:`collada.asset.UP_AXIS` indicating the up-axis of the asset"""

        if contributors is None:
            contributors = []
        self.contributors = contributors
        """A list of instances of :class:`collada.asset.Contributor`"""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the asset."""
        else:
            self._recreateXmlNode()

    def _recreateXmlNode(self):
        self.xmlnode = E.asset()
        for contributor in self.contributors:
            self.xmlnode.append(contributor.xmlnode)
        self.xmlnode.append(E.created(self.created.isoformat()))
        if self.keywords is not None:
            self.xmlnode.append(E.keywords(self.keywords))
        self.xmlnode.append(E.modified(self.modified.isoformat()))
        if self.revision is not None:
            self.xmlnode.append(E.revision(self.revision))
        if self.subject is not None:
            self.xmlnode.append(E.subject(self.subject))
        if self.title is not None:
            self.xmlnode.append(E.title(self.title))
        if self.unitmeter is not None and self.unitname is not None:
            self.xmlnode.append(E.unit(name=self.unitname, meter=str(self.unitmeter)))
        self.xmlnode.append(E.up_axis(self.upaxis))

    def save(self):
        """Saves the asset info back to :attr:`xmlnode`"""
        self._recreateXmlNode()

    @staticmethod
    def load(collada, localscope, node):
        contributornodes = node.findall(collada.tag('contributor'))
        contributors = [Contributor.load(collada, localscope, cn) for cn in contributornodes]

        created = node.find(collada.tag('created'))
        if created is not None:
            try:
                created = dateutil.parser.parse(created.text)
            except BaseException:
                created = None

        keywords = node.find(collada.tag('keywords'))
        if keywords is not None:
            keywords = keywords.text

        modified = node.find(collada.tag('modified'))
        if modified is not None:
            try:
                modified = dateutil.parser.parse(modified.text)
            except BaseException:
                modified = None

        revision = node.find(collada.tag('revision'))
        if revision is not None:
            revision = revision.text

        subject = node.find(collada.tag('subject'))
        if subject is not None:
            subject = subject.text

        title = node.find(collada.tag('title'))
        if title is not None:
            title = title.text

        unitnode = node.find(collada.tag('unit'))
        if unitnode is not None:
            unitname = unitnode.get('name')
            try:
                unitmeter = float(unitnode.get('meter'))
            except BaseException:
                unitname = None
                unitmeter = None
        else:
            unitname = None
            unitmeter = None

        upaxis = node.find(collada.tag('up_axis'))
        if upaxis is not None:
            upaxis = upaxis.text
            if not (upaxis == UP_AXIS.X_UP or
                    upaxis == UP_AXIS.Y_UP or
                    upaxis == UP_AXIS.Z_UP):
                upaxis = None

        return Asset(created=created, modified=modified, title=title,
                     subject=subject, revision=revision, keywords=keywords,
                     unitname=unitname, unitmeter=unitmeter, upaxis=upaxis,
                     contributors=contributors, xmlnode=node)

    def __str__(self):
        return '<Asset title=%s>' % (str(self.title),)

    def __repr__(self):
        return str(self)
