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

"""Module for material, effect and image loading

This module contains all the functionality to load and manage:
- Images in the image library
- Surfaces and samplers2D in effects
- Effects (that are now used as materials)

"""

import copy
import io
import numpy

from collada.common import DaeObject, E, tag
from collada.common import DaeIncompleteError, DaeBrokenRefError, \
    DaeMalformedError, DaeUnsupportedError
from collada.util import falmostEqual

try:
    from PIL import Image as pil
except BaseException:
    pil = None

# internally used constant
_FAILED = 'failed'


class DaeMissingSampler2D(Exception):
    """Raised when a <texture> tag references a texture without a sampler."""


class CImage(DaeObject):
    """Class containing data coming from a <image> tag.

    Basically is just the path to the file, but we give an extended
    functionality if PIL is available. You can in that case get the
    image object or numpy arrays in both int and float format. We
    named it CImage to avoid confusion with PIL's Image class.

    """

    def __init__(self, id, path, collada=None, xmlnode=None):
        """Create an image object.

        :param str id:
          A unique string identifier for the image
        :param str path:
          Path relative to the collada document where the image is located
        :param collada.Collada collada:
          The collada object this image belongs to
        :param xmlnode:
          If loaded from xml, the node this data comes from

        """
        self.id = id
        """The unique string identifier for the image"""
        self.path = path
        """Path relative to the collada document where the image is located"""

        self.collada = collada
        self._data = None
        self._pilimage = None
        self._uintarray = None
        self._floatarray = None
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the image."""
        else:
            self.xmlnode = E.image(
                E.init_from(path), id=self.id, name=self.id)

    def getData(self):
        if self._data is None:
            try:
                self._data = self.collada.getFileData(self.path)
            except DaeBrokenRefError as ex:
                self._data = ''
                self.collada.handleError(ex)
        return self._data

    def getImage(self):
        if pil is None or self._pilimage is _FAILED:
            return None
        if self._pilimage:
            return self._pilimage
        else:
            data = self.getData()
            if not data:
                self._pilimage = _FAILED
                return None
            try:
                self._pilimage = pil.open(io.BytesIO(data))
                self._pilimage.load()
            except IOError:
                self._pilimage = _FAILED
                return None
            return self._pilimage

    def getUintArray(self):
        if self._uintarray is _FAILED:
            return None
        if self._uintarray is not None:
            return self._uintarray
        img = self.getImage()
        if not img:
            self._uintarray = _FAILED
            return None
        nchan = len(img.mode)
        self._uintarray = numpy.frombuffer(img.tobytes(), dtype=numpy.uint8)
        self._uintarray.shape = (img.size[1], img.size[0], nchan)
        return self._uintarray

    def getFloatArray(self):
        if self._floatarray is _FAILED:
            return None
        if self._floatarray is not None:
            return self._floatarray
        array = self.getUintArray()
        if array is None:
            self._floatarray = _FAILED
            return None
        self._floatarray = numpy.asarray(array, dtype=numpy.float32)
        self._floatarray *= 1.0 / 255.0
        return self._floatarray

    def setData(self, data):
        self._data = data
        self._floatarray = None
        self._uintarray = None
        self._pilimage = None

    data = property(getData, setData)
    """Raw binary image file data if the file is readable. If `aux_file_loader` was passed to
    :func:`collada.Collada.__init__`, this function will be called to retrieve the data.
    Otherwise, if the file came from the local disk, the path will be interpreted from
    the local file system. If the file was a zip archive, the archive will be searched."""
    pilimage = property(getImage)
    """PIL Image object if PIL is available and the file is readable."""
    uintarray = property(getUintArray)
    """Numpy array (height, width, nchannels) in integer format."""
    floatarray = property(getFloatArray)
    """Numpy float array (height, width, nchannels) with the image data normalized to 1.0."""

    @staticmethod
    def load(collada, localspace, node):
        id = node.get('id')
        initnode = node.find(collada.tag('init_from'))
        if initnode is None:
            raise DaeIncompleteError('Image has no file path')
        path = initnode.text
        return CImage(id, path, collada, xmlnode=node)

    def save(self):
        """Saves the image back to :attr:`xmlnode`. Only the :attr:`id` attribute is saved.
        The image itself will have to be saved to its original source to make modifications."""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        initnode = self.xmlnode.find(tag('init_from'))
        initnode.text = self.path

    def __str__(self):
        return '<CImage id=%s path=%s>' % (self.id, self.path)

    def __repr__(self):
        return str(self)


class Surface(DaeObject):
    """Class containing data coming from a <surface> tag.

    Collada materials use this to access to the <image> tag.
    The only extra information we store right now is the
    image format. In theory, this enables many more features
    according to the collada spec, but no one seems to actually
    use them in the wild, so for now, it's unimplemented.

    """

    def __init__(self, id, img, format=None, xmlnode=None):
        """Creates a surface.

        :param str id:
          A string identifier for the surface within the local scope of the material
        :param collada.material.CImage img:
          The image object
        :param str format:
          The format of the image
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The string identifier for the surface within the local scope of the material"""
        self.image = img
        """:class:`collada.material.CImage` object from the image library."""
        self.format = format if format is not None else "A8R8G8B8"
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the surface."""
        else:
            self.xmlnode = E.newparam(
                E.surface(
                    E.init_from(self.image.id),
                    E.format(self.format), type="2D"), sid=self.id)

    @staticmethod
    def load(collada, localscope, node):
        surfacenode = node.find(collada.tag('surface'))
        if surfacenode is None:
            raise DaeIncompleteError('No surface found in newparam')
        if surfacenode.get('type') != '2D':
            raise DaeMalformedError('Hard to imagine a non-2D surface, isn\'t it?')
        initnode = surfacenode.find(collada.tag('init_from'))
        if initnode is None:
            raise DaeIncompleteError('No init image found in surface')
        formatnode = surfacenode.find(collada.tag('format'))
        if formatnode is None:
            format = None
        else:
            format = formatnode.text
        imgid = initnode.text
        id = node.get('sid')
        if imgid in localscope:
            img = localscope[imgid]
        else:
            img = collada.images.get(imgid)
        if img is None:
            raise DaeBrokenRefError("Missing image '%s' in surface '%s'" % (imgid, id))
        return Surface(id, img, format, xmlnode=node)

    def save(self):
        """Saves the surface data back to :attr:`xmlnode`"""
        surfacenode = self.xmlnode.find(tag('surface'))
        initnode = surfacenode.find(tag('init_from'))
        if self.format:
            formatnode = surfacenode.find(tag('format'))
            if formatnode is None:
                surfacenode.append(E.format(self.format))
            else:
                formatnode.text = self.format
        initnode.text = self.image.id
        self.xmlnode.set('sid', self.id)

    def __str__(self):
        return '<Surface id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class Sampler2D(DaeObject):
    """Class containing data coming from <sampler2D> tag in material.

    Collada uses the <sampler2D> tag to map to a <surface>. The only
    information we store about the sampler right now is minfilter and
    magfilter. Theoretically, the collada spec has many more parameters
    here, but no one seems to be using them in the wild, so they are
    currently unimplemented.

    """

    def __init__(self, id, surface, minfilter=None, magfilter=None, xmlnode=None):
        """Create a Sampler2D object.

        :param str id:
          A string identifier for the sampler within the local scope of the material
        :param collada.material.Surface surface:
          Surface instance that this object samples from
        :param str minfilter:
          Minification filter string id, see collada spec for details
        :param str magfilter:
          Maximization filter string id, see collada spec for details
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The string identifier for the sampler within the local scope of the material"""
        self.surface = surface
        """Surface instance that this object samples from"""
        self.minfilter = minfilter
        """Minification filter string id, see collada spec for details"""
        self.magfilter = magfilter
        """Maximization filter string id, see collada spec for details"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the sampler."""
        else:
            sampler_node = E.sampler2D(E.source(self.surface.id))
            if minfilter:
                sampler_node.append(E.minfilter(self.minfilter))
            if magfilter:
                sampler_node.append(E.magfilter(self.magfilter))

            self.xmlnode = E.newparam(sampler_node, sid=self.id)

    @staticmethod
    def load(collada, localscope, node):
        samplernode = node.find(collada.tag('sampler2D'))
        if samplernode is None:
            raise DaeIncompleteError('No sampler found in newparam')
        sourcenode = samplernode.find(collada.tag('source'))
        if sourcenode is None:
            raise DaeIncompleteError('No source found in sampler')
        minnode = samplernode.find(collada.tag('minfilter'))
        if minnode is None:
            minfilter = None
        else:
            minfilter = minnode.text
        magnode = samplernode.find(collada.tag('magfilter'))
        if magnode is None:
            magfilter = None
        else:
            magfilter = magnode.text

        surfaceid = sourcenode.text
        id = node.get('sid')
        surface = localscope.get(surfaceid)
        if surface is None or not isinstance(surface, Surface):
            raise DaeBrokenRefError('Missing surface ' + surfaceid)
        return Sampler2D(id, surface, minfilter, magfilter, xmlnode=node)

    def save(self):
        """Saves the sampler data back to :attr:`xmlnode`"""
        samplernode = self.xmlnode.find(tag('sampler2D'))
        sourcenode = samplernode.find(tag('source'))
        if self.minfilter:
            minnode = samplernode.find(tag('minfilter'))
            minnode.text = self.minfilter
        if self.magfilter:
            maxnode = samplernode.find(tag('magfilter'))
            maxnode.text = self.magfilter
        sourcenode.text = self.surface.id
        self.xmlnode.set('sid', self.id)

    def __str__(self):
        return '<Sampler2D id=%s>' % (self.id,)

    def __repr__(self):
        return str(self)


class Map(DaeObject):
    """Class containing data coming from <texture> tag inside material.

    When a material defines its properties like `diffuse`, it can give you
    a color or a texture. In the latter, the texture is mapped with a
    sampler and a texture coordinate channel. If a material defined a texture
    for one of its properties, you'll find an object of this class in the
    corresponding attribute.

    """

    def __init__(self, sampler, texcoord, xmlnode=None):
        """Create a map instance to a sampler using a texcoord channel.

        :param collada.material.Sampler2D sampler:
          A sampler object to map
        :param str texcoord:
          Texture coordinate channel symbol to use
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.sampler = sampler
        """:class:`collada.material.Sampler2D` object to map"""
        self.texcoord = texcoord
        """Texture coordinate channel symbol to use"""
        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the map"""
        else:
            self.xmlnode = E.texture(texture=self.sampler.id, texcoord=self.texcoord)

    @staticmethod
    def load(collada, localscope, node):
        samplerid = node.get('texture')
        texcoord = node.get('texcoord')
        sampler = localscope.get(samplerid)
        # Check for the sampler ID as the texture ID because some exporters suck
        if sampler is None:
            for s2d in localscope.values():
                if isinstance(s2d, Sampler2D):
                    if s2d.surface.image.id == samplerid:
                        sampler = s2d
        if sampler is None or not isinstance(sampler, Sampler2D):
            err = DaeMissingSampler2D('Missing sampler ' + samplerid + ' in node ' + node.tag)
            err.samplerid = samplerid
            raise err
        return Map(sampler, texcoord, xmlnode=node)

    def save(self):
        """Saves the map back to :attr:`xmlnode`"""
        self.xmlnode.set('texture', self.sampler.id)
        self.xmlnode.set('texcoord', self.texcoord)

    def __str__(self):
        return '<Map sampler=%s texcoord=%s>' % (self.sampler.id, self.texcoord)

    def __repr__(self):
        return str(self)


class OPAQUE_MODE:
    """The opaque mode of an effect."""
    A_ONE = 'A_ONE'
    """Takes the transparency information from the color's alpha channel, where the value 1.0 is opaque (default)."""
    RGB_ZERO = 'RGB_ZERO'
    """Takes the transparency information from the color's red, green, and blue
    channels, where the value 0.0 is opaque, with each channel modulated
    independently."""


class Effect(DaeObject):
    """Class containing data coming from an <effect> tag.
    """
    supported = ['emission', 'ambient', 'diffuse', 'specular',
                 'shininess', 'reflective', 'reflectivity',
                 'transparent', 'transparency', 'index_of_refraction']
    """Supported material properties list."""
    shaders = ['phong', 'lambert', 'blinn', 'constant']
    """Supported shader list."""

    def __init__(self, id, params, shadingtype, bumpmap=None, double_sided=False,
                 emission=(0.0, 0.0, 0.0, 1.0),
                 ambient=(0.0, 0.0, 0.0, 1.0),
                 diffuse=(0.0, 0.0, 0.0, 1.0),
                 specular=(0.0, 0.0, 0.0, 1.0),
                 shininess=0.0,
                 reflective=(0.0, 0.0, 0.0, 1.0),
                 reflectivity=0.0,
                 transparent=(0.0, 0.0, 0.0, 1.0),
                 transparency=None,
                 index_of_refraction=None,
                 opaque_mode=None,
                 xmlnode=None):
        """Create an effect instance out of properties.

        :param str id:
          A string identifier for the effect
        :param list params:
          A list containing elements of type :class:`collada.material.Sampler2D`
          and :class:`collada.material.Surface`
        :param str shadingtype:
          The type of shader to be used for this effect. Right now, we
          only supper the shaders listed in :attr:`shaders`
        :param `collada.material.Map` bumpmap:
          The bump map for this effect, or None if there isn't one
        :param bool double_sided:
          Whether or not the material should be rendered double sided
        :param emission:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param ambient:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param diffuse:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param specular:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param shininess:
          Either a single float or an instance of :class:`collada.material.Map`
        :param reflective:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param reflectivity:
          Either a single float or an instance of :class:`collada.material.Map`
        :param tuple transparent:
          Either an RGBA-format tuple of four floats or an instance
          of :class:`collada.material.Map`
        :param transparency:
          Either a single float or an instance of :class:`collada.material.Map`
        :param float index_of_refraction:
          A single float indicating the index of refraction for perfectly
          refracted light
        :param `collada.material.OPAQUE_MODE` opaque_mode:
          The opaque mode for the effect. If not specified, defaults to A_ONE.
        :param xmlnode:
          If loaded from xml, the xml node

        """
        self.id = id
        """The string identifier for the effect"""
        self.params = params
        """A list containing elements of type :class:`collada.material.Sampler2D`
          and :class:`collada.material.Surface`"""
        self.shadingtype = shadingtype
        """String with the type of the shading."""
        self.bumpmap = bumpmap
        """Either the bump map of the effect of type :class:`collada.material.Map`
        or None if there is none."""
        self.double_sided = double_sided
        """A boolean indicating whether or not the material should be rendered double sided"""
        self.emission = emission
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.ambient = ambient
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.diffuse = diffuse
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.specular = specular
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.shininess = shininess
        """Either a single float or an instance of :class:`collada.material.Map`"""
        self.reflective = reflective
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.reflectivity = reflectivity
        """Either a single float or an instance of :class:`collada.material.Map`"""
        self.transparent = transparent
        """Either an RGB-format tuple of three floats or an instance
          of :class:`collada.material.Map`"""
        self.transparency = transparency
        """Either a single float or an instance of :class:`collada.material.Map`"""
        self.index_of_refraction = index_of_refraction
        """A single float indicating the index of refraction for perfectly
          refracted light"""
        self.opaque_mode = OPAQUE_MODE.A_ONE if opaque_mode is None else opaque_mode
        """The opaque mode for the effect. An instance of :class:`collada.material.OPAQUE_MODE`."""

        if self.transparency is None:
            if self.opaque_mode == OPAQUE_MODE.A_ONE:
                self.transparency = 1.0
            else:
                self.transparency = 0.0

        self._fixColorValues()

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the effect"""
        else:
            shadnode = E(self.shadingtype)

            for prop in self.supported:
                value = getattr(self, prop)
                if value is None:
                    continue
                propnode = E(prop)
                if prop == 'transparent' and self.opaque_mode == OPAQUE_MODE.RGB_ZERO:
                    propnode.set('opaque', OPAQUE_MODE.RGB_ZERO)
                shadnode.append(propnode)
                if isinstance(value, Map):
                    propnode.append(value.xmlnode)
                elif isinstance(value, float):
                    propnode.append(E.float(str(value)))
                else:
                    propnode.append(E.color(' '.join(map(str, value))))

            effect_nodes = [param.xmlnode for param in self.params]
            effect_nodes.append(E.technique(shadnode, sid='common'))
            self.xmlnode = E.effect(
                E.profile_COMMON(*effect_nodes), id=self.id, name=self.id)

    @staticmethod
    def getEffectParameters(collada, parentnode, localscope, params):

        for paramnode in parentnode.findall(collada.tag('newparam')):
            if paramnode.find(collada.tag('surface')) is not None:
                param = Surface.load(collada, localscope, paramnode)
                params.append(param)
                localscope[param.id] = param
            elif paramnode.find(collada.tag('sampler2D')) is not None:
                param = Sampler2D.load(collada, localscope, paramnode)
                params.append(param)
                localscope[param.id] = param
            else:
                # Try float variants in order
                floatnode = None
                for float_type in ('float', 'float2', 'float3', 'float4'):
                    floatnode = paramnode.find(collada.tag(float_type))
                    if floatnode is not None:
                        break
                paramid = paramnode.get('sid')
                if floatnode is not None and paramid and len(paramid) > 0 and floatnode.text is not None:
                    localscope[paramid] = [float(v) for v in floatnode.text.split()]

    @staticmethod
    def load(collada, localscope, node):
        localscope = {}  # we have our own scope, shadow it
        params = []
        id = node.get('id')
        profilenode = node.find(collada.tag('profile_COMMON'))
        if profilenode is None:
            raise DaeUnsupportedError('Found effect with profile other than profile_COMMON')

        # <image> can be local to a material instead of global in <library_images>
        for imgnode in profilenode.findall(collada.tag('image')):
            local_image = CImage.load(collada, localscope, imgnode)
            localscope[local_image.id] = local_image

            global_image_id = local_image.id
            uniquenum = 2
            while global_image_id in collada.images:
                global_image_id = local_image.id + "-" + uniquenum
                uniquenum += 1
            collada.images.append(local_image)

        Effect.getEffectParameters(collada, profilenode, localscope, params)

        tecnode = profilenode.find(collada.tag('technique'))

        Effect.getEffectParameters(collada, tecnode, localscope, params)

        shadnode = None
        for shad in Effect.shaders:
            shadnode = tecnode.find(collada.tag(shad))
            shadingtype = shad
            if shadnode is not None:
                break
        if shadnode is None:
            raise DaeIncompleteError('No material properties found in effect')
        props = {}
        for key in Effect.supported:
            pnode = shadnode.find(collada.tag(key))
            if pnode is None:
                props[key] = None
            else:
                try:
                    props[key] = Effect._loadShadingParam(collada, localscope, pnode)
                except DaeMissingSampler2D as ex:
                    if ex.samplerid in collada.images:
                        # Whoever exported this collada file didn't include the proper references so we will create them
                        surf = Surface(ex.samplerid + '-surface', collada.images[ex.samplerid], 'A8R8G8B8')
                        sampler = Sampler2D(ex.samplerid, surf, None, None)
                        params.append(surf)
                        params.append(sampler)
                        localscope[surf.id] = surf
                        localscope[sampler.id] = sampler
                        try:
                            props[key] = Effect._loadShadingParam(
                                collada, localscope, pnode)
                        except DaeUnsupportedError as ex:
                            props[key] = None
                            collada.handleError(ex)
                except DaeUnsupportedError as ex:
                    props[key] = None
                    collada.handleError(ex)  # Give the chance to ignore error and load the rest

                if key == 'transparent' and key in props and props[key] is not None:
                    opaque_mode = pnode.get('opaque')
                    if opaque_mode is not None and opaque_mode == OPAQUE_MODE.RGB_ZERO:
                        props['opaque_mode'] = OPAQUE_MODE.RGB_ZERO
        props['xmlnode'] = node

        bumpnode = node.find(f".//{collada.tag('extra')}//{collada.tag('texture')}")
        if bumpnode is not None:
            bumpmap = Map.load(collada, localscope, bumpnode)
        else:
            bumpmap = None

        double_sided_node = node.find(f".//{collada.tag('extra')}//{collada.tag('double_sided')}")
        double_sided = False
        if double_sided_node is not None and double_sided_node.text is not None:
            try:
                val = int(double_sided_node.text)
                if val == 1:
                    double_sided = True
            except ValueError:
                pass
        return Effect(id, params, shadingtype, bumpmap, double_sided, **props)

    @staticmethod
    def _loadShadingParam(collada, localscope, node):
        """Load from the node a definition for a material property."""
        children = list(node)
        if not children:
            raise DaeIncompleteError('Incorrect effect shading parameter ' + node.tag)
        vnode = children[0]
        if vnode.tag == collada.tag('color'):
            try:
                value = tuple(float(v) for v in vnode.text.split())
            except ValueError:
                raise DaeMalformedError('Corrupted color definition in effect `{}`'.format(id))
            except IndexError:
                raise DaeMalformedError('Corrupted color definition in effect `{}`'.format(id))
        elif vnode.tag == collada.tag('float'):
            try:
                value = float(vnode.text)
            except ValueError:
                raise DaeMalformedError('Corrupted float definition in effect ' + id)
        elif vnode.tag == collada.tag('texture'):
            value = Map.load(collada, localscope, vnode)
        elif vnode.tag == collada.tag('param'):
            refid = vnode.get('ref')
            if refid is not None and refid in localscope:
                value = localscope[refid]
            else:
                return None
        else:
            raise DaeUnsupportedError('Unknown shading param definition ' +
                                      str(vnode.tag))
        return value

    def _fixColorValues(self):
        for prop in self.supported:
            propval = getattr(self, prop)
            if isinstance(propval, tuple):
                if len(propval) < 4:
                    propval = list(propval)
                    while len(propval) < 3:
                        propval.append(0.0)
                    while len(propval) < 4:
                        propval.append(1.0)
                    setattr(self, prop, tuple(propval))

    def save(self):
        """Saves the effect back to :attr:`xmlnode`"""
        self.xmlnode.set('id', self.id)
        self.xmlnode.set('name', self.id)
        profilenode = self.xmlnode.find(tag('profile_COMMON'))
        tecnode = profilenode.find(tag('technique'))
        tecnode.set('sid', 'common')

        self._fixColorValues()

        for param in self.params:
            param.save()
            if param.xmlnode not in profilenode:
                profilenode.insert(list(profilenode).index(tecnode),
                                   param.xmlnode)

        deletenodes = []
        for oldparam in profilenode.findall(tag('newparam')):
            if oldparam not in [param.xmlnode for param in self.params]:
                deletenodes.append(oldparam)
        for d in deletenodes:
            profilenode.remove(d)

        for shader in self.shaders:
            shadnode = tecnode.find(tag(shader))
            if shadnode is not None and shader != self.shadingtype:
                tecnode.remove(shadnode)

        def getPropNode(prop, value):
            propnode = E(prop)
            if prop == 'transparent' and self.opaque_mode == OPAQUE_MODE.RGB_ZERO:
                propnode.set('opaque', OPAQUE_MODE.RGB_ZERO)
            if isinstance(value, Map):
                propnode.append(copy.deepcopy(value.xmlnode))
            elif isinstance(value, float):
                propnode.append(E.float(str(value)))
            else:
                propnode.append(E.color(' '.join(map(str, value))))
            return propnode

        shadnode = tecnode.find(tag(self.shadingtype))
        if shadnode is None:
            shadnode = E(self.shadingtype)
            for prop in self.supported:
                value = getattr(self, prop)
                if value is None:
                    continue
                shadnode.append(getPropNode(prop, value))
            tecnode.append(shadnode)
        else:
            for prop in self.supported:
                value = getattr(self, prop)
                propnode = shadnode.find(tag(prop))
                if propnode is not None:
                    shadnode.remove(propnode)
                if value is not None:
                    shadnode.append(getPropNode(prop, value))

        double_sided_node = profilenode.find(f".//{tag('extra')}//{tag('double_sided')}")
        if double_sided_node is None or double_sided_node.text is None:
            extranode = profilenode.find(tag('extra'))
            if extranode is None:
                extranode = E.extra()
                profilenode.append(extranode)

            teqnodes = extranode.findall(tag('technique'))
            goognode = None
            for teqnode in teqnodes:
                if teqnode.get('profile') == 'GOOGLEEARTH':
                    goognode = teqnode
                    break
            if goognode is None:
                goognode = E.technique(profile='GOOGLEEARTH')
                extranode.append(goognode)
            double_sided_node = goognode.find(tag('double_sided'))
            if double_sided_node is None:
                double_sided_node = E.double_sided()
                goognode.append(double_sided_node)

        double_sided_node.text = "1" if self.double_sided else "0"

    def __str__(self):
        return '<Effect id=%s type=%s>' % (self.id, self.shadingtype)

    def __repr__(self):
        return str(self)

    def almostEqual(self, other):
        """Checks if this effect is almost equal (within float precision)
        to the given effect.

        :param collada.material.Effect other:
          Effect to compare to

        :rtype: bool

        """
        if self.shadingtype != other.shadingtype:
            return False
        if self.double_sided != other.double_sided:
            return False
        for prop in self.supported:
            thisprop = getattr(self, prop)
            otherprop = getattr(other, prop)
            if not isinstance(thisprop, type(otherprop)):
                return False
            elif isinstance(thisprop, float):
                if not falmostEqual(thisprop, otherprop):
                    return False
            elif isinstance(thisprop, Map):
                if thisprop.sampler.surface.image.id != otherprop.sampler.surface.image.id or thisprop.texcoord != otherprop.texcoord:
                    return False
            elif isinstance(thisprop, tuple):
                if len(thisprop) != len(otherprop):
                    return False
                for valthis, valother in zip(thisprop, otherprop):
                    if not falmostEqual(valthis, valother):
                        return False
        return True


class Material(DaeObject):
    """Class containing data coming from a <material> tag.

    Right now, this just stores a reference to the effect
    which is instantiated in the material. The effect instance
    can have parameters, but this is rarely used in the wild,
    so it is not yet implemented.

    """

    def __init__(self, id, name, effect, xmlnode=None):
        """Creates a material.

        :param str id:
          A unique string identifier for the material
        :param str name:
          A name for the material
        :param collada.material.Effect effect:
          The effect instantiated in this material
        :param xmlnode:
          If loaded from xml, the xml node

        """

        self.id = id
        """The unique string identifier for the material"""
        self.name = name
        """The name for the material"""
        self.effect = effect
        """The :class:`collada.material.Effect` instantiated in this material"""

        if xmlnode is not None:
            self.xmlnode = xmlnode
            """ElementTree representation of the surface."""
        else:
            self.xmlnode = E.material(
                E.instance_effect(url="#%s" % self.effect.id), id=str(self.id), name=str(self.name))

    @staticmethod
    def load(collada, localscope, node):
        matid = node.get('id')
        matname = node.get('name')

        effnode = node.find(collada.tag('instance_effect'))
        if effnode is None:
            raise DaeIncompleteError('No effect inside material')
        effectid = effnode.get('url')

        if not effectid.startswith('#'):
            raise DaeMalformedError('Corrupted effect reference in material %s' % effectid)

        effect = collada.effects.get(effectid[1:])
        if not effect:
            raise DaeBrokenRefError('Effect not found: ' + effectid)

        return Material(matid, matname, effect, xmlnode=node)

    def save(self):
        """Saves the material data back to :attr:`xmlnode`"""
        self.xmlnode.set('id', str(self.id))
        self.xmlnode.set('name', str(self.name))
        effnode = self.xmlnode.find(tag('instance_effect'))
        effnode.set('url', '#%s' % self.effect.id)

    def __str__(self):
        return '<Material id=%s effect=%s>' % (self.id, self.effect.id)

    def __repr__(self):
        return str(self)
