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

"""Contains objects for representing cameras"""

from collada.common import DaeIncompleteError
from collada.common import DaeMalformedError
from collada.common import DaeObject
from collada.common import DaeUnsupportedError
from collada.common import E


class Camera(DaeObject):
    """Base camera class holding data from <camera> tags."""

    @staticmethod
    def load(collada, localscope, node):
        tecnode = node.find(f"{collada.tag('optics')}/{collada.tag('technique_common')}")
        if tecnode is None or len(tecnode) == 0:
            raise DaeIncompleteError('Missing common technique in camera')
        camnode = tecnode[0]
        if camnode.tag == collada.tag('perspective'):
            return PerspectiveCamera.load(collada, localscope, node)
        elif camnode.tag == collada.tag('orthographic'):
            return OrthographicCamera.load(collada, localscope, node)
        else:
            raise DaeUnsupportedError('Unrecognized camera type: %s' % camnode.tag)


class PerspectiveCamera(Camera):
    """Perspective camera as defined in COLLADA tag <perspective>."""

    def __init__(self, id, znear, zfar, xfov=None, yfov=None,
                 aspect_ratio=None, xmlnode=None):
        """Create a new perspective camera.

        Note: ``aspect_ratio = tan(0.5*xfov) / tan(0.5*yfov)``

        You can specify one of:
         * :attr:`xfov` alone
         * :attr:`yfov` alone
         * :attr:`xfov` and :attr:`yfov`
         * :attr:`xfov` and :attr:`aspect_ratio`
         * :attr:`yfov` and :attr:`aspect_ratio`

        Any other combination will raise :class:`collada.common.DaeMalformedError`

        :param str id:
          Identifier for the camera
        :param float znear:
          Distance to the near clipping plane
        :param float zfar:
          Distance to the far clipping plane
        :param float xfov:
          Horizontal field of view, in degrees
        :param float yfov:
          Vertical field of view, in degrees
        :param float aspect_ratio:
          Aspect ratio of the field of view
        :param xmlnode:
          If loaded from xml, the xml node

        """

        self.id = id
        """Identifier for the camera"""
        self.xfov = xfov
        """Horizontal field of view, in degrees"""
        self.yfov = yfov
        """Vertical field of view, in degrees"""
        self.aspect_ratio = aspect_ratio
        """Aspect ratio of the field of view"""
        self.znear = znear
        """Distance to the near clipping plane"""
        self.zfar = zfar
        """Distance to the far clipping plane"""

        self._checkValidParams()

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the data."""
        else:
            self._recreateXmlNode()

    def _recreateXmlNode(self):
        perspective_node = E.perspective()
        if self.xfov is not None:
            perspective_node.append(E.xfov(str(self.xfov)))
        if self.yfov is not None:
            perspective_node.append(E.yfov(str(self.yfov)))
        if self.aspect_ratio is not None:
            perspective_node.append(E.aspect_ratio(str(self.aspect_ratio)))
        perspective_node.append(E.znear(str(self.znear)))
        perspective_node.append(E.zfar(str(self.zfar)))
        self.xmlnode = E.camera(
            E.optics(
                E.technique_common(perspective_node)
            ), id=self.id, name=self.id)

    def _checkValidParams(self):
        if self.xfov is not None and self.yfov is None \
                and self.aspect_ratio is None:
            pass
        elif self.xfov is None and self.yfov is not None \
                and self.aspect_ratio is None:
            pass
        elif self.xfov is not None and self.yfov is None \
                and self.aspect_ratio is not None:
            pass
        elif self.xfov is None and self.yfov is not None \
                and self.aspect_ratio is not None:
            pass
        elif self.xfov is not None and self.yfov is not None \
                and self.aspect_ratio is None:
            pass
        else:
            raise DaeMalformedError("Received invalid combination of xfov (%s), yfov (%s), and aspect_ratio (%s)" %
                                    (str(self.xfov), str(self.yfov), str(self.aspect_ratio)))

    def save(self):
        """Saves the perspective camera's properties back to xmlnode"""
        self._checkValidParams()
        self._recreateXmlNode()

    @staticmethod
    def load(collada, localscope, node):
        persnode = node.find(f"{collada.tag('optics')}/{collada.tag('technique_common')}/{collada.tag('perspective')}")

        if persnode is None:
            raise DaeIncompleteError('Missing perspective for camera definition')

        xfov = persnode.find(collada.tag('xfov'))
        yfov = persnode.find(collada.tag('yfov'))
        aspect_ratio = persnode.find(collada.tag('aspect_ratio'))
        znearnode = persnode.find(collada.tag('znear'))
        zfarnode = persnode.find(collada.tag('zfar'))
        id = node.get('id', '')

        try:
            if xfov is not None:
                xfov = float(xfov.text)
            if yfov is not None:
                yfov = float(yfov.text)
            if aspect_ratio is not None:
                aspect_ratio = float(aspect_ratio.text)
            znear = float(znearnode.text)
            zfar = float(zfarnode.text)
        except (TypeError, ValueError):
            raise DaeMalformedError('Corrupted float values in camera definition')

        # There are some exporters that incorrectly output all three of these.
        # Worse, they actually got the calculation of aspect_ratio wrong!
        # So instead of failing to load, let's just add one more hack because of terrible exporters
        if xfov is not None and yfov is not None and aspect_ratio is not None:
            aspect_ratio = None

        return PerspectiveCamera(id, znear, zfar, xfov=xfov, yfov=yfov,
                                 aspect_ratio=aspect_ratio, xmlnode=node)

    def bind(self, matrix):
        """Create a bound camera of itself based on a transform matrix.

        :param numpy.array matrix:
          A numpy transformation matrix of size 4x4

        :rtype: :class:`collada.camera.BoundPerspectiveCamera`

        """
        return BoundPerspectiveCamera(self, matrix)

    def __str__(self):
        return '<PerspectiveCamera id=%s>' % self.id

    def __repr__(self):
        return str(self)


class OrthographicCamera(Camera):
    """Orthographic camera as defined in COLLADA tag <orthographic>."""

    def __init__(self, id, znear, zfar, xmag=None, ymag=None, aspect_ratio=None, xmlnode=None):
        """Create a new orthographic camera.

        Note: ``aspect_ratio = xmag / ymag``

        You can specify one of:
         * :attr:`xmag` alone
         * :attr:`ymag` alone
         * :attr:`xmag` and :attr:`ymag`
         * :attr:`xmag` and :attr:`aspect_ratio`
         * :attr:`ymag` and :attr:`aspect_ratio`

        Any other combination will raise :class:`collada.common.DaeMalformedError`

        :param str id:
          Identifier for the camera
        :param float znear:
          Distance to the near clipping plane
        :param float zfar:
          Distance to the far clipping plane
        :param float xmag:
          Horizontal magnification of the view
        :param float ymag:
          Vertical magnification of the view
        :param float aspect_ratio:
          Aspect ratio of the field of view
        :param xmlnode:
          If loaded from xml, the xml node

        """

        self.id = id
        """Identifier for the camera"""
        self.xmag = xmag
        """Horizontal magnification of the view"""
        self.ymag = ymag
        """Vertical magnification of the view"""
        self.aspect_ratio = aspect_ratio
        """Aspect ratio of the field of view"""
        self.znear = znear
        """Distance to the near clipping plane"""
        self.zfar = zfar
        """Distance to the far clipping plane"""

        self._checkValidParams()

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the data."""
        else:
            self._recreateXmlNode()

    def _recreateXmlNode(self):
        orthographic_node = E.orthographic()
        if self.xmag is not None:
            orthographic_node.append(E.xmag(str(self.xmag)))
        if self.ymag is not None:
            orthographic_node.append(E.ymag(str(self.ymag)))
        if self.aspect_ratio is not None:
            orthographic_node.append(E.aspect_ratio(str(self.aspect_ratio)))
        orthographic_node.append(E.znear(str(self.znear)))
        orthographic_node.append(E.zfar(str(self.zfar)))
        self.xmlnode = E.camera(
            E.optics(
                E.technique_common(orthographic_node)
            ), id=self.id, name=self.id)

    def _checkValidParams(self):
        if self.xmag is not None and self.ymag is None \
                and self.aspect_ratio is None:
            pass
        elif self.xmag is None and self.ymag is not None \
                and self.aspect_ratio is None:
            pass
        elif self.xmag is not None and self.ymag is None \
                and self.aspect_ratio is not None:
            pass
        elif self.xmag is None and self.ymag is not None \
                and self.aspect_ratio is not None:
            pass
        elif self.xmag is not None and self.ymag is not None \
                and self.aspect_ratio is None:
            pass
        else:
            raise DaeMalformedError("Received invalid combination of xmag (%s), ymag (%s), and aspect_ratio (%s)" %
                                    (str(self.xmag), str(self.ymag), str(self.aspect_ratio)))

    def save(self):
        """Saves the orthographic camera's properties back to xmlnode"""
        self._checkValidParams()
        self._recreateXmlNode()

    @staticmethod
    def load(collada, localscope, node):
        orthonode = node.find(f"{collada.tag('optics')}/{collada.tag('technique_common')}/{collada.tag('orthographic')}")

        if orthonode is None:
            raise DaeIncompleteError('Missing orthographic for camera definition')

        xmag = orthonode.find(collada.tag('xmag'))
        ymag = orthonode.find(collada.tag('ymag'))
        aspect_ratio = orthonode.find(collada.tag('aspect_ratio'))
        znearnode = orthonode.find(collada.tag('znear'))
        zfarnode = orthonode.find(collada.tag('zfar'))
        id = node.get('id', '')

        try:
            if xmag is not None:
                xmag = float(xmag.text)
            if ymag is not None:
                ymag = float(ymag.text)
            if aspect_ratio is not None:
                aspect_ratio = float(aspect_ratio.text)
            znear = float(znearnode.text)
            zfar = float(zfarnode.text)
        except (TypeError, ValueError):
            raise DaeMalformedError('Corrupted float values in camera definition')

        # There are some exporters that incorrectly output all three of these.
        # Worse, they actually got the calculation of aspect_ratio wrong!
        # So instead of failing to load, let's just add one more hack because of terrible exporters
        if xmag is not None and ymag is not None and aspect_ratio is not None:
            aspect_ratio = None

        return OrthographicCamera(id, znear, zfar, xmag=xmag, ymag=ymag,
                                  aspect_ratio=aspect_ratio, xmlnode=node)

    def bind(self, matrix):
        """Create a bound camera of itself based on a transform matrix.

        :param numpy.array matrix:
          A numpy transformation matrix of size 4x4

        :rtype: :class:`collada.camera.BoundOrthographicCamera`

        """
        return BoundOrthographicCamera(self, matrix)

    def __str__(self):
        return '<OrthographicCamera id=%s>' % self.id

    def __repr__(self):
        return str(self)


class BoundCamera(object):
    """Base class for bound cameras"""


class BoundPerspectiveCamera(BoundCamera):
    """Perspective camera bound to a scene with a transform. This gets created when a
        camera is instantiated in a scene. Do not create this manually."""

    def __init__(self, cam, matrix):
        self.xfov = cam.xfov
        """Horizontal field of view, in degrees"""
        self.yfov = cam.yfov
        """Vertical field of view, in degrees"""
        self.aspect_ratio = cam.aspect_ratio
        """Aspect ratio of the field of view"""
        self.znear = cam.znear
        """Distance to the near clipping plane"""
        self.zfar = cam.zfar
        """Distance to the far clipping plane"""
        self.matrix = matrix
        """The matrix bound to"""
        self.position = matrix[:3, 3]
        """The position of the camera"""
        self.direction = -matrix[:3, 2]
        """The direction the camera is facing"""
        self.up = matrix[:3, 1]
        """The up vector of the camera"""
        self.original = cam
        """Original :class:`collada.camera.PerspectiveCamera` object this is bound to."""

    def __str__(self):
        return '<BoundPerspectiveCamera bound to %s>' % self.original.id

    def __repr__(self):
        return str(self)


class BoundOrthographicCamera(BoundCamera):
    """Orthographic camera bound to a scene with a transform. This gets created when a
        camera is instantiated in a scene. Do not create this manually."""

    def __init__(self, cam, matrix):
        self.xmag = cam.xmag
        """Horizontal magnification of the view"""
        self.ymag = cam.ymag
        """Vertical magnification of the view"""
        self.aspect_ratio = cam.aspect_ratio
        """Aspect ratio of the field of view"""
        self.znear = cam.znear
        """Distance to the near clipping plane"""
        self.zfar = cam.zfar
        """Distance to the far clipping plane"""
        self.matrix = matrix
        """The matrix bound to"""
        self.position = matrix[:3, 3]
        """The position of the camera"""
        self.direction = -matrix[:3, 2]
        """The direction the camera is facing"""
        self.up = matrix[:3, 1]
        """The up vector of the camera"""
        self.original = cam
        """Original :class:`collada.camera.OrthographicCamera` object this is bound to."""

    def __str__(self):
        return '<BoundOrthographicCamera bound to %s>' % self.original.id

    def __repr__(self):
        return str(self)
