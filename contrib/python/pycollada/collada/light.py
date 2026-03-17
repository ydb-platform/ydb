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

"""Contains objects for representing lights."""

import numpy

from collada.common import DaeObject, E, tag
from collada.common import DaeIncompleteError, DaeMalformedError, \
    DaeUnsupportedError
from collada.util import _correctValInNode


class Light(DaeObject):
    """Base light class holding data from <light> tags."""

    @staticmethod
    def load(collada, localscope, node):
        tecnode = node.find(collada.tag('technique_common'))
        if tecnode is None or len(tecnode) == 0:
            raise DaeIncompleteError('Missing common technique in light')
        lightnode = tecnode[0]
        if lightnode.tag == collada.tag('directional'):
            return DirectionalLight.load(collada, localscope, node)
        elif lightnode.tag == collada.tag('point'):
            return PointLight.load(collada, localscope, node)
        elif lightnode.tag == collada.tag('ambient'):
            return AmbientLight.load(collada, localscope, node)
        elif lightnode.tag == collada.tag('spot'):
            return SpotLight.load(collada, localscope, node)
        else:
            raise DaeUnsupportedError('Unrecognized light type: %s' % lightnode.tag)


class DirectionalLight(Light):
    """Directional light as defined in COLLADA tag <directional> tag."""

    def __init__(self, id, color, xmlnode=None):
        """Create a new directional light.

        :param str id:
          A unique string identifier for the light
        :param tuple color:
          Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The unique string identifier for the light"""
        self.direction = numpy.array([0, 0, -1], dtype=numpy.float32)
        # Not documenting this because it doesn't make sense to set the direction
        # of an unbound light. The direction isn't set until binding in a scene.
        self.color = color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the light."""
        else:
            self.xmlnode = E.light(
                E.technique_common(
                    E.directional(
                        E.color(' '.join(map(str, self.color)))
                    )
                ), id=self.id, name=self.id)

    def save(self):
        """Saves the light's properties back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        colornode = self.xmlnode.find(f"{tag('technique_common')}/{tag('directional')}/{tag('color')}")
        colornode.text = ' '.join(map(str, self.color))

    @staticmethod
    def load(collada, localscope, node):
        colornode = node.find(f"{collada.tag('technique_common')}/{collada.tag('directional')}/{collada.tag('color')}")
        if colornode is None:
            raise DaeIncompleteError('Missing color for directional light')
        try:
            color = tuple(float(v) for v in colornode.text.split())
        except ValueError:
            raise DaeMalformedError('Corrupted color values in light definition')
        return DirectionalLight(node.get('id'), color, xmlnode=node)

    def bind(self, matrix):
        """Binds this light to a transform matrix.

        :param numpy.array matrix:
          A 4x4 numpy float matrix

        :rtype: :class:`collada.light.BoundDirectionalLight`

        """
        return BoundDirectionalLight(self, matrix)

    def __str__(self):
        return '<DirectionalLight id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class AmbientLight(Light):
    """Ambient light as defined in COLLADA tag <ambient>."""

    def __init__(self, id, color, xmlnode=None):
        """Create a new ambient light.

        :param str id:
          A unique string identifier for the light
        :param tuple color:
          Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The unique string identifier for the light"""
        self.color = color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the light."""
        else:
            self.xmlnode = E.light(
                E.technique_common(
                    E.ambient(
                        E.color(' '.join(map(str, self.color)))
                    )
                ), id=self.id, name=self.id)

    def save(self):
        """Saves the light's properties back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        colornode = self.xmlnode.find(f"{tag('technique_common')}/{tag('ambient')}/{tag('color')}")
        colornode.text = ' '.join(map(str, self.color))

    @staticmethod
    def load(collada, localscope, node):
        colornode = node.find(f"{collada.tag('technique_common')}/{collada.tag('ambient')}/{collada.tag('color')}")
        if colornode is None:
            raise DaeIncompleteError('Missing color for ambient light')
        try:
            color = tuple(float(v) for v in colornode.text.split())
        except ValueError:
            raise DaeMalformedError('Corrupted color values in light definition')
        return AmbientLight(node.get('id'), color, xmlnode=node)

    def bind(self, matrix):
        """Binds this light to a transform matrix.

        :param numpy.array matrix:
          A 4x4 numpy float matrix

        :rtype: :class:`collada.light.BoundAmbientLight`

        """
        return BoundAmbientLight(self, matrix)

    def __str__(self):
        return '<AmbientLight id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class PointLight(Light):
    """Point light as defined in COLLADA tag <point>."""

    def __init__(self, id, color, constant_att=None, linear_att=None,
                 quad_att=None, zfar=None, xmlnode=None):
        """Create a new sun light.

        :param str id:
          A unique string identifier for the light
        :param tuple color:
          Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light
        :param float constant_att:
          Constant attenuation factor
        :param float linear_att:
          Linear attenuation factor
        :param float quad_att:
          Quadratic attenuation factor
        :param float zfar:
          Distance to the far clipping plane
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The unique string identifier for the light"""
        self.position = numpy.array([0, 0, 0], dtype=numpy.float32)
        # Not documenting this because it doesn't make sense to set the position
        # of an unbound light. The position isn't set until binding in a scene.
        self.color = color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.constant_att = constant_att
        """Constant attenuation factor."""
        self.linear_att = linear_att
        """Linear attenuation factor."""
        self.quad_att = quad_att
        """Quadratic attenuation factor."""
        self.zfar = zfar
        """Distance to the far clipping plane"""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the light."""
        else:
            pnode = E.point(
                E.color(' '.join(map(str, self.color)))
            )
            if self.constant_att is not None:
                pnode.append(E.constant_attenuation(str(self.constant_att)))
            if self.linear_att is not None:
                pnode.append(E.linear_attenuation(str(self.linear_att)))
            if self.quad_att is not None:
                pnode.append(E.quadratic_attenuation(str(self.quad_att)))
            if self.zfar is not None:
                pnode.append(E.zfar(str(self.zvar)))

            self.xmlnode = E.light(
                E.technique_common(pnode), id=self.id, name=self.id)

    def save(self):
        """Saves the light's properties back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        pnode = self.xmlnode.find(f"{tag('technique_common')}/{tag('point')}")
        colornode = pnode.find(tag('color'))
        colornode.text = ' '.join(map(str, self.color))
        _correctValInNode(pnode, 'constant_attenuation', self.constant_att)
        _correctValInNode(pnode, 'linear_attenuation', self.linear_att)
        _correctValInNode(pnode, 'quadratic_attenuation', self.quad_att)
        _correctValInNode(pnode, 'zfar', self.zfar)

    @staticmethod
    def load(collada, localscope, node):
        pnode = node.find(f"{collada.tag('technique_common')}/{collada.tag('point')}")
        colornode = pnode.find(collada.tag('color'))
        if colornode is None:
            raise DaeIncompleteError('Missing color for point light')
        try:
            color = tuple(float(v) for v in colornode.text.split())
        except ValueError:
            raise DaeMalformedError('Corrupted color values in light definition')
        constant_att = linear_att = quad_att = zfar = None
        qattnode = pnode.find(collada.tag('quadratic_attenuation'))
        cattnode = pnode.find(collada.tag('constant_attenuation'))
        lattnode = pnode.find(collada.tag('linear_attenuation'))
        zfarnode = pnode.find(collada.tag('zfar'))
        try:
            if cattnode is not None:
                constant_att = float(cattnode.text)
            if lattnode is not None:
                linear_att = float(lattnode.text)
            if qattnode is not None:
                quad_att = float(qattnode.text)
            if zfarnode is not None:
                zfar = float(zfarnode.text)
        except ValueError:
            raise DaeMalformedError('Corrupted values in light definition')
        return PointLight(node.get('id'), color, constant_att, linear_att,
                          quad_att, zfar, xmlnode=node)

    def bind(self, matrix):
        """Binds this light to a transform matrix.

        :param numpy.array matrix:
          A 4x4 numpy float matrix

        :rtype: :class:`collada.light.BoundPointLight`

        """
        return BoundPointLight(self, matrix)

    def __str__(self):
        return '<PointLight id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class SpotLight(Light):
    """Spot light as defined in COLLADA tag <spot>."""

    def __init__(self, id, color, constant_att=None, linear_att=None,
                 quad_att=None, falloff_ang=None, falloff_exp=None, xmlnode=None):
        """Create a new spot light.

        :param str id:
          A unique string identifier for the light
        :param tuple color:
          Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light
        :param float constant_att:
          Constant attenuation factor
        :param float linear_att:
          Linear attenuation factor
        :param float quad_att:
          Quadratic attenuation factor
        :param float falloff_ang:
          Falloff angle
        :param float falloff_exp:
          Falloff exponent
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The unique string identifier for the light"""
        self.color = color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.constant_att = constant_att
        """Constant attenuation factor."""
        self.linear_att = linear_att
        """Linear attenuation factor."""
        self.quad_att = quad_att
        """Quadratic attenuation factor."""
        self.falloff_ang = falloff_ang
        """Falloff angle"""
        self.falloff_exp = falloff_exp
        """Falloff exponent"""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the light."""
        else:
            pnode = E.spot(
                E.color(' '.join(map(str, self.color))),
            )
            if self.constant_att is not None:
                pnode.append(E.constant_attenuation(str(self.constant_att)))
            if self.linear_att is not None:
                pnode.append(E.linear_attenuation(str(self.linear_att)))
            if self.quad_att is not None:
                pnode.append(E.quadratic_attenuation(str(self.quad_att)))
            if self.falloff_ang is not None:
                pnode.append(E.falloff_angle(str(self.falloff_ang)))
            if self.falloff_exp is not None:
                pnode.append(E.falloff_exponent(str(self.falloff_exp)))

            self.xmlnode = E.light(
                E.technique_common(pnode), id=self.id, name=self.id)

    def save(self):
        """Saves the light's properties back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        pnode = self.xmlnode.find(f"{tag('technique_common')}/{tag('spot')}")
        colornode = pnode.find(tag('color'))
        colornode.text = ' '.join(map(str, self.color))
        _correctValInNode(pnode, 'constant_attenuation', self.constant_att)
        _correctValInNode(pnode, 'linear_attenuation', self.linear_att)
        _correctValInNode(pnode, 'quadratic_attenuation', self.quad_att)
        _correctValInNode(pnode, 'falloff_angle', self.falloff_ang)
        _correctValInNode(pnode, 'falloff_exponent', self.falloff_exp)

    @staticmethod
    def load(collada, localscope, node):
        pnode = node.find(f"{collada.tag('technique_common')}/{collada.tag('spot')}")
        colornode = pnode.find(collada.tag('color'))
        if colornode is None:
            raise DaeIncompleteError('Missing color for spot light')
        try:
            color = tuple(float(v) for v in colornode.text.split())
        except ValueError:
            raise DaeMalformedError('Corrupted color values in spot light definition')
        constant_att = linear_att = quad_att = falloff_ang = falloff_exp = None
        cattnode = pnode.find(collada.tag('constant_attenuation'))
        lattnode = pnode.find(collada.tag('linear_attenuation'))
        qattnode = pnode.find(collada.tag('quadratic_attenuation'))
        fangnode = pnode.find(collada.tag('falloff_angle'))
        fexpnode = pnode.find(collada.tag('falloff_exponent'))
        try:
            if cattnode is not None:
                constant_att = float(cattnode.text)
            if lattnode is not None:
                linear_att = float(lattnode.text)
            if qattnode is not None:
                quad_att = float(qattnode.text)
            if fangnode is not None:
                falloff_ang = float(fangnode.text)
            if fexpnode is not None:
                falloff_exp = float(fexpnode.text)
        except ValueError:
            raise DaeMalformedError('Corrupted values in spot light definition')
        return SpotLight(node.get('id'), color, constant_att, linear_att,
                         quad_att, falloff_ang, falloff_exp, xmlnode=node)

    def bind(self, matrix):
        """Binds this light to a transform matrix.

        :param numpy.array matrix:
          A 4x4 numpy float matrix

        :rtype: :class:`collada.light.BoundSpotLight`

        """
        return BoundSpotLight(self, matrix)

    def __str__(self):
        return '<SpotLight id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class BoundLight(object):
    """Base class for bound lights"""


class BoundPointLight(BoundLight):
    """Point light bound to a scene with transformation. This gets created when a
        light is instantiated in a scene. Do not create this manually."""

    def __init__(self, plight, matrix):
        self.position = numpy.dot(matrix[:3, :3], plight.position) + matrix[:3, 3]
        """Numpy array of length 3 representing the position of the light in the scene"""
        self.color = plight.color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.constant_att = plight.constant_att
        if self.constant_att is None:
            self.constant_att = 1.0
        """Constant attenuation factor."""
        self.linear_att = plight.linear_att
        if self.linear_att is None:
            self.linear_att = 0.0
        """Linear attenuation factor."""
        self.quad_att = plight.quad_att
        if self.quad_att is None:
            self.quad_att = 0.0
        """Quadratic attenuation factor."""
        self.zfar = plight.zfar
        """Distance to the far clipping plane"""
        self.original = plight
        """The original :class:`collada.light.PointLight` this is bound to"""

    def __str__(self):
        return '<BoundPointLight bound to id=%s>' % str(self.original.id)

    def __repr__(self):
        return str(self)


class BoundSpotLight(BoundLight):
    """Spot light bound to a scene with transformation. This gets created when a
        light is instantiated in a scene. Do not create this manually."""

    def __init__(self, slight, matrix):
        self.position = matrix[:3, 3]
        """Numpy array of length 3 representing the position of the light in the scene"""
        self.direction = -matrix[:3, 2]
        """Direction of the spot light"""
        self.up = matrix[:3, 1]
        """Up vector of the spot light"""
        self.matrix = matrix
        """Transform matrix for the bound light"""
        self.color = slight.color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.constant_att = slight.constant_att
        if self.constant_att is None:
            self.constant_att = 1.0
        """Constant attenuation factor."""
        self.linear_att = slight.linear_att
        if self.linear_att is None:
            self.linear_att = 0.0
        """Linear attenuation factor."""
        self.quad_att = slight.quad_att
        if self.quad_att is None:
            self.quad_att = 0.0
        """Quadratic attenuation factor."""
        self.falloff_ang = slight.falloff_ang
        if self.falloff_ang is None:
            self.falloff_ang = 180.0
        """Falloff angle"""
        self.falloff_exp = slight.falloff_exp
        if self.falloff_exp is None:
            self.falloff_exp = 0.0
        """Falloff exponent"""
        self.original = slight
        """The original :class:`collada.light.SpotLight` this is bound to"""

    def __str__(self):
        return '<BoundSpotLight bound to id=%s>' % str(self.original.id)

    def __repr__(self):
        return str(self)


class BoundDirectionalLight(BoundLight):
    """Directional light bound to a scene with transformation. This gets created when a
        light is instantiated in a scene. Do not create this manually."""

    def __init__(self, dlight, matrix):
        self.direction = numpy.dot(matrix[:3, :3], dlight.direction)
        """Numpy array of length 3 representing the direction of the light in the scene"""
        self.color = dlight.color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.original = dlight
        """The original :class:`collada.light.DirectionalLight` this is bound to"""

    def __str__(self):
        return '<BoundDirectionalLight bound to id=%s>' % str(self.original.id)

    def __repr__(self):
        return str(self)


class BoundAmbientLight(BoundLight):
    """Ambient light bound to a scene with transformation. This gets created when a
        light is instantiated in a scene. Do not create this manually."""

    def __init__(self, alight, matrix):
        self.color = alight.color
        """Either a tuple of size 3 containing the RGB color value
          of the light or a tuple of size 4 containing the RGBA
          color value of the light"""
        self.original = alight
        """The original :class:`collada.light.AmbientLight` this is bound to"""

    def __str__(self):
        return '<BoundAmbientLight bound to id=%s>' % str(self.original.id)

    def __repr__(self):
        return str(self)
