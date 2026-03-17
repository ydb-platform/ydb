"""
color.py
-------------

Hold and deal with visual information about meshes.

There are lots of ways to encode visual information, and the goal of this
architecture is to make it possible to define one, and then transparently
get the others. The two general categories are:

1) colors, defined for a face, vertex, or material
2) textures, defined as an image and UV coordinates for each vertex

This module only implements diffuse colors at the moment.

Goals
----------
1) If nothing is defined sane defaults should be returned
2) If a user alters or sets a value, that is considered user data
   and should be saved and treated as such.
3) Only one 'mode' of visual (vertex or face) is allowed at a time
   and setting or altering a value should automatically change the mode.
"""

import copy

import numpy as np

from .. import caching, util
from ..constants import tol
from ..grouping import unique_rows
from ..resources import get_json
from ..typed import (
    Any,
    ArrayLike,
    Callable,
    ColorMapType,
    DTypeLike,
    Integer,
    Iterable,
    NDArray,
    Optional,
    Union,
)
from .base import Visuals

# Save a lookup table for an integer to match the
# cases for HSV conversion specified on the wikipedia article
# Where indexes 0=C, 1=X, 2=0.0
_HSV_LOOKUP = np.array(
    [[0, 1, 2], [1, 0, 2], [2, 0, 1], [2, 1, 0], [1, 2, 0], [0, 2, 1]], dtype=np.int64
)
_HSV_LOOKUP.flags.writeable = False


class ColorVisuals(Visuals):
    """
    Store color information about a mesh.
    """

    def __init__(
        self,
        mesh=None,
        face_colors: Optional[ArrayLike] = None,
        vertex_colors: Optional[ArrayLike] = None,
    ):
        """
        Store color information about a mesh.

        Parameters
        ----------
        mesh : Trimesh
          Object that these visual properties
          are associated with
        face_ colors :  (n,3|4) or (3,) or (4,) uint8
          Colors per-face
        vertex_colors : (n,3|4) or (3,) or (4,) uint8
          Colors per-vertex
        """
        self.mesh = mesh
        self._data = caching.DataStore()
        self._cache = caching.Cache(id_function=self._data.__hash__)

        try:
            if face_colors is not None:
                self.face_colors = face_colors
            if vertex_colors is not None:
                self.vertex_colors = vertex_colors
        except ValueError:
            util.log.warning("unable to convert colors!")

    @caching.cache_decorator
    def transparency(self) -> bool:
        """
        Does the current object contain any transparency.

        Returns
        ----------
        transparency: bool, does the current visual contain transparency
        """
        if "vertex_colors" in self._data:
            a_min = self._data["vertex_colors"][:, 3].min()
        elif "face_colors" in self._data:
            a_min = self._data["face_colors"][:, 3].min()
        else:
            return False

        return bool(a_min < 255)

    @property
    def defined(self) -> bool:
        """
        Are any colors defined for the current mesh.

        Returns
        ---------
        defined : bool
          Are colors defined or not.
        """
        return self.kind is not None

    @property
    def kind(self) -> Optional[str]:
        """
        What color mode has been set.

        Returns
        ----------
        mode : str or None
          One of ('face', 'vertex', None)
        """
        # if nothing is stored anywhere it's a safe bet mode is None
        if not (len(self._cache.cache) > 0 or len(self._data.data) > 0):
            return None

        self._verify_hash()

        # check modes in data
        if "vertex_colors" in self._data:
            return "vertex"
        elif "face_colors" in self._data:
            return "face"

        return None

    def __hash__(self):
        return self._data.__hash__()

    def copy(self) -> "ColorVisuals":
        """
        Return a copy of the current ColorVisuals object.


        Returns
        ----------
        copied : ColorVisuals
          Contains the same information as self
        """
        copied = ColorVisuals()
        # call the literally insane generators to validate
        self.face_colors  # noqa
        self.vertex_colors  # noqa
        # copy anything that's actually data
        copied._data.data = copy.deepcopy(self._data.data)

        return copied

    @property
    def face_colors(self) -> NDArray[np.uint8]:
        """
        Colors defined for each face of a mesh.

        If no colors are defined, defaults are returned.

        Returns
        ----------
        colors : (len(mesh.faces), 4) uint8
          RGBA color for each face
        """
        return self._get_colors(name="face")

    @face_colors.setter
    def face_colors(self, values: ArrayLike):
        """
        Set the colors for each face of a mesh.

        This will apply these colors and delete any previously specified
        color information.

        Parameters
        ------------
        colors : (len(mesh.faces), 3), set each face to the specified color
                 (len(mesh.faces), 4), set each face to the specified color
                 (3,) int, set the whole mesh this color
                 (4,) int, set the whole mesh this color
        """
        if values is None:
            if "face_colors" in self._data:
                self._data.data.pop("face_colors")
            return

        colors = to_rgba(values)

        if self.mesh is not None and colors.shape == (4,):
            count = len(self.mesh.faces)
            colors = np.tile(colors, (count, 1))

        # if we set any color information, clear the others
        self._data.clear()
        self._data["face_colors"] = colors
        self._cache.verify()

    @property
    def vertex_colors(self) -> NDArray[np.uint8]:
        """
        Return the colors for each vertex of a mesh

        Returns
        ------------
        colors: (len(mesh.vertices), 4) uint8, color for each vertex
        """
        return self._get_colors(name="vertex")

    @vertex_colors.setter
    def vertex_colors(self, values: ArrayLike):
        """
        Set the colors for each vertex of a mesh

        This will apply these colors and delete any previously specified
        color information.

        Parameters
        ------------
        colors : (len(mesh.vertices), 3), set each face to the color
                 (len(mesh.vertices), 4), set each face to the color
                 (3,) int, set the whole mesh this color
                 (4,) int, set the whole mesh this color
        """
        if values is None:
            if "vertex_colors" in self._data:
                self._data.data.pop("vertex_colors")
            return

        # make sure passed values are numpy array
        values = np.asanyarray(values)
        # Ensure the color shape is sane
        if self.mesh is not None and not (
            values.shape == (len(self.mesh.vertices), 3)
            or values.shape == (len(self.mesh.vertices), 4)
            or values.shape == (3,)
            or values.shape == (4,)
        ):
            return

        colors = to_rgba(values)
        if self.mesh is not None and colors.shape == (4,):
            count = len(self.mesh.vertices)
            colors = np.tile(colors, (count, 1))

        # if we set any color information, clear the others
        self._data.clear()
        self._data["vertex_colors"] = colors
        self._cache.verify()

    def _get_colors(self, name):
        """
        A magical function which maintains the sanity of vertex and face colors.

        * If colors have been explicitly stored or changed, they are considered
        user data, stored in self._data (DataStore), and are returned immediately
        when requested.
        * If colors have never been set, a (count,4) tiled copy of the default diffuse
        color will be stored in the cache
        ** the hash on creation for these cached default colors will also be stored
        ** if the cached color array is altered (different hash than when it was
        created) we consider that now to be user data and the array is moved from
        the cache to the DataStore.

        Parameters
        -----------
        name : str
          Values 'face' or 'vertex'

        Returns
        -----------
        colors : (count, 4) uint8
          RGBA colors
        """

        count = None
        try:
            if name == "face":
                count = len(self.mesh.faces)
            elif name == "vertex":
                count = len(self.mesh.vertices)
        except BaseException:
            pass

        # the face or vertex colors
        key_colors = str(name) + "_colors"
        # the initial hash of the colors
        key_hash = key_colors + "_hash"

        if key_colors in self._data:
            # if a user has explicitly stored or changed the color it
            # will be in data
            return self._data[key_colors]

        elif key_colors in self._cache:
            # if the colors have been autogenerated already they
            # will be in the cache
            colors = self._cache[key_colors]
            # if the cached colors have been changed since creation we move
            # them to data
            if hash(colors) != self._cache[key_hash]:
                # call the setter on the property using exec
                # this avoids having to pass a setter to this function
                if name == "face":
                    self.face_colors = colors
                elif name == "vertex":
                    self.vertex_colors = colors
                else:
                    raise ValueError("unsupported name!!!")
                self._cache.verify()
                # return the stored copy of the colors
                return self._data[key_colors]
        else:
            # colors have never been accessed
            if self.kind is None:
                # no colors are defined, so create a (count, 4) tiled
                # copy of the default color
                colors = np.tile(DEFAULT_MAT["material_diffuse"], (count, 1))
            elif self.kind == "vertex" and name == "face":
                colors = vertex_to_face_color(
                    vertex_colors=self.vertex_colors, faces=self.mesh.faces
                )
            elif self.kind == "face" and name == "vertex":
                colors = face_to_vertex_color(
                    mesh=self.mesh, face_colors=self.face_colors
                )
            else:
                raise ValueError("self.kind not accepted values!!")

        if count is not None and colors.shape != (count, 4):
            raise ValueError("face colors incorrect shape!")

        # subclass the array to track for changes using a hash
        colors = caching.tracked_array(colors)
        # put the generated colors and their initial checksum into cache
        self._cache[key_colors] = colors
        self._cache[key_hash] = hash(colors)

        return colors

    def _verify_hash(self):
        """
        Verify the checksums of cached face and vertex color, to verify
        that a user hasn't altered them since they were generated from
        defaults.

        If the colors have been altered since creation, move them into
        the DataStore at self._data since the user action has made them
        user data.
        """
        if not hasattr(self, "_cache") or len(self._cache) == 0:
            return

        for name in ["face", "vertex"]:
            # the face or vertex colors
            key_colors = str(name) + "_colors"
            # the initial hash of the colors
            key_hash = key_colors + "_hash"

            if key_colors not in self._cache:
                continue

            colors = self._cache[key_colors]
            # if the cached colors have been changed since creation
            # move them to data
            if hash(colors) != self._cache[key_hash]:
                if name == "face":
                    self.face_colors = colors
                elif name == "vertex":
                    self.vertex_colors = colors
                else:
                    raise ValueError("unsupported name!!!")
                self._cache.verify()

    def update_vertices(self, mask: ArrayLike):
        """
        Apply a mask to remove or duplicate vertex properties.
        """
        self._update_key(mask, "vertex_colors")

    def update_faces(self, mask: ArrayLike):
        """
        Apply a mask to remove or duplicate face properties
        """
        self._update_key(mask, "face_colors")

    def face_subset(self, face_index: ArrayLike):
        """
        Given a mask of face indices, return a sliced version.

        Parameters
        ----------
        face_index: (n,) int, mask for faces
                    (n,) bool, mask for faces

        Returns
        ----------
        visual: ColorVisuals object containing a subset of faces.
        """
        kwargs = {}
        if self.defined:
            if self.face_colors is not None:
                kwargs.update(face_colors=self.face_colors[face_index])

            if self.vertex_colors is not None:
                indices = np.unique(self.mesh.faces[face_index].flatten())
                vertex_colors = self.vertex_colors[indices]
                kwargs.update(vertex_colors=vertex_colors)

        result = ColorVisuals(**kwargs)

        return result

    @property
    def main_color(self) -> NDArray[np.uint8]:
        """
        What is the most commonly occurring color.

        Returns
        ------------
        color: (4,) uint8, most common color
        """
        if self.kind is None:
            return DEFAULT_COLOR
        elif self.kind == "face":
            colors = self.face_colors
        elif self.kind == "vertex":
            colors = self.vertex_colors
        else:
            raise ValueError("color kind incorrect!")

        # find the unique colors
        unique, inverse = unique_rows(colors)
        # the most commonly occurring color, or mode
        # this will be an index of inverse, not colors
        mode_index = np.bincount(inverse).argmax()
        color = colors[unique[mode_index]]

        return color

    def to_texture(self):
        """
        Convert the current ColorVisuals object to a texture
        with a `SimpleMaterial` defined.

        Returns
        ------------
        visual : trimesh.visual.TextureVisuals
          Copy of the current visuals as a texture.
        """
        from .texture import TextureVisuals

        mat, uv = color_to_uv(vertex_colors=self.vertex_colors)
        return TextureVisuals(material=mat, uv=uv)

    def concatenate(self, other: Union[Iterable[Visuals], Visuals, ArrayLike], *args):
        """
        Concatenate two or more ColorVisuals objects
        into a single object.

        Parameters
        -----------
        other : ColorVisuals
          Object to append
        *args: ColorVisuals objects

        Returns
        -----------
        result : ColorVisuals
          Containing information from current
          object and others in the order it was passed.
        """
        # avoid a circular import
        from . import objects

        result = objects.concatenate(self, other, *args)
        return result

    def _update_key(self, mask, key):
        """
        Mask the value contained in the DataStore at a specified key.

        Parameters
        -----------
        mask: (n,) int
              (n,) bool
        key: hashable object, in self._data
        """
        mask = np.asanyarray(mask)
        if key in self._data:
            self._data[key] = self._data[key][mask]


class VertexColor(Visuals):
    """
    Create a simple visual object to hold just vertex colors
    for objects such as PointClouds.
    """

    def __init__(self, colors=None, obj=None):
        """
        Create a vertex color visual
        """
        self.obj = obj
        self.vertex_colors = colors

    @property
    def kind(self):
        return "vertex"

    def update_vertices(self, mask):
        if self._colors is not None:
            self._colors = self._colors[mask]

    def update_faces(self, mask):
        pass

    @property
    def vertex_colors(self):
        return self._colors

    @vertex_colors.setter
    def vertex_colors(self, data):
        if data is None:
            self._colors = caching.tracked_array(None)
        else:
            # tile single color into color array
            data = np.asanyarray(data)
            if data.shape in [(3,), (4,)]:
                data = np.tile(data, (len(self.obj.vertices), 1))
            # track changes in colors and convert to RGBA
            self._colors = caching.tracked_array(to_rgba(data))

    def copy(self):
        """
        Return a copy of the current visuals
        """
        return copy.deepcopy(self)

    def concatenate(self, other):
        """
        Concatenate this visual object with another
        VertexVisuals.

        Parameters
        -----------
        other : VertexColors or ColorVisuals
          Other object to concatenate

        Returns
        ------------
        concate : VertexColor
          Object with both colors
        """
        return VertexColor(colors=np.vstack(self.vertex_colors, other.vertex_colors))

    def __hash__(self):
        return self._colors.__hash__()


def to_rgba(colors: Any, dtype: DTypeLike = np.uint8) -> NDArray:
    """
    Convert a single or multiple RGB colors to RGBA colors.

    Parameters
    ----------
    colors : (n, 3) or (n, 4) array
      RGB or RGBA colors or None

    Returns
    ----------
    colors : (n, 4) list of RGBA colors
             (4,)  single RGBA color
    """
    if colors is None:
        return DEFAULT_COLOR
    # if MTL uses 0 as None
    if isinstance(colors, (int, float)) and colors == 0:
        return DEFAULT_COLOR

    # colors as numpy array
    colors = np.asanyarray(colors)
    dtype = np.dtype(dtype)

    # what is the output dtype opaque value
    if dtype.kind in "iu":
        opaque = np.iinfo(dtype).max
    elif dtype.kind == "f":
        opaque = 1.0
    else:
        raise ValueError(f"Unknown dtype: `{dtype}`")

    if colors.dtype.kind == "f":
        # replace any `nan` or `inf` values with zero
        colors[~np.isfinite(colors)] = 0.0

        # multiple the 0.0 - 1.0 colors by the opaque value
        # to scale them to the output data type's proper range
        colors = np.clip(colors * opaque, 0.0, opaque)

        # if the requested output type is integer-like
        # make sure to round the multiplied floats
        # before the `astype` on the return
        if dtype.kind in "iu":
            colors = colors.round()

    if util.is_shape(colors, (-1, 3)):
        # add an opaque alpha for RGB colors
        colors = np.column_stack((colors, opaque * np.ones(len(colors))))
    elif util.is_shape(colors, (3,)):
        # if passed a single RGB color add an alpha
        colors = np.append(colors, opaque)
    if not (util.is_shape(colors, (4,)) or util.is_shape(colors, (-1, 4))):
        raise ValueError("Colors not of appropriate shape!")

    return colors.astype(dtype)


def to_float(colors: ArrayLike) -> NDArray[np.float64]:
    """
    Convert integer colors to 0.0-1.0 floating point colors

    Parameters
    -------------
    colors : (n, d) int
      Integer colors

    Returns
    -------------
    as_float : (n, d) float
      Float colors 0.0 - 1.0
    """

    # colors as numpy array
    colors = np.asanyarray(colors)
    if colors.dtype.kind == "f":
        return colors.astype(np.float64)
    elif colors.dtype.kind in "iu":
        # integer value for opaque alpha given our datatype
        opaque = np.iinfo(colors.dtype).max
        return colors.astype(np.float64) / opaque
    else:
        raise ValueError("only works on int or float colors!")


def hex_to_rgba(color: str) -> NDArray[np.uint8]:
    """
    Turn a string hex color to a (4,) RGBA color.

    Parameters
    -----------
    color: str, hex color

    Returns
    -----------
    rgba: (4,) np.uint8, RGBA color
    """
    value = str(color).lstrip("#").strip()
    if len(value) == 6:
        rgb = [int(value[i : i + 2], 16) for i in (0, 2, 4)]
        rgba = np.append(rgb, 255).astype(np.uint8)
    else:
        raise ValueError("Only RGB supported")

    return rgba


def hsv_to_rgba(hsv: ArrayLike, dtype: DTypeLike = np.uint8) -> NDArray:
    """
    Convert an (n, 3) array of 0.0-1.0 HSV colors into an
    array of RGBA colors.

    A vectorized implementation that matches `colorsys.hsv_to_rgb`.

    Parameters
    -----------
    hsv
      Should be `(n, 3)` array of 0.0-1.0 values.

    Returns
    ------------
    rgba
      An (n, 4) array of RGBA colors.
    """

    hsv = np.asanyarray(hsv, dtype=np.float64)
    if len(hsv.shape) != 2 or hsv.shape[1] != 3:
        raise ValueError("(n, 3) values of HSV are required")
    # clip values in-place to 0.0-1.0 range
    np.clip(hsv, a_min=0.0, a_max=1.0, out=hsv)

    # expand into flat arrays for each of
    # hue, saturation, and value
    H, S, V = hsv.T

    # chroma and other values for the equation
    C = S * V
    Hi = H * 6.0
    X = C * (1.0 - np.abs((Hi % 2.0) - 1.0))

    # stack values we need so we can access them with the lookup table
    stacked = np.column_stack((C, X, np.zeros_like(X)))
    # get the indexes per-row and then increment them so we can use them on the stack
    indexes = _HSV_LOOKUP[Hi.astype(np.int64)] + (np.arange(len(H)) * 3).reshape((-1, 1))

    # get the intermediate value, described by wikipedia as
    # the point along the bottom three faces of the RGB cube
    RGBi = stacked.ravel()[indexes]

    # stack it into the final RGBA array
    RGBA = np.column_stack((RGBi + (V - C).reshape((-1, 1)), np.ones(len(H))))

    # now return the correct type of color
    dtype = np.dtype(dtype)
    if dtype.kind == "f":
        return RGBA.astype(dtype)
    elif dtype.kind in "iu":
        return (RGBA * np.iinfo(dtype).max).round().astype(dtype)

    raise ValueError(f"dtype `{dtype}` not supported")


def linear_to_srgb(linear: ArrayLike) -> NDArray[np.float64]:
    """
    Converts linear color values to sRGB color values.

    See: https://entropymine.com/imageworsener/srgbformula/

    Parameters
    ----------
    linear
      Linear color values of any shape since this
      is a per-element transformation

    Returns
    ---------
    srgb
      Values scaled to an sRGB scale.
    """
    linear = to_float(linear)

    mask = linear > 0.00313066844250063
    srgb = np.zeros(linear.shape, dtype=np.float64)
    srgb[mask] = 1.055 * np.power(linear[mask], (1.0 / 2.4)) - 0.055
    srgb[~mask] = 12.92 * linear[~mask]

    return srgb


def srgb_to_linear(srgb: ArrayLike) -> NDArray[np.float64]:
    """
    Converts sRGB color values to linear color values.
    See: https://entropymine.com/imageworsener/srgbformula/
    """

    # make sure the color values are floating point scaled
    srgb = to_float(srgb)

    mask = srgb <= 0.0404482362771082
    linear = np.zeros(srgb.shape, dtype=np.float64)
    linear[mask] = srgb[mask] / 12.92
    linear[~mask] = np.power(((srgb[~mask] + 0.055) / 1.055), 2.4)

    return linear


def random_color(dtype: DTypeLike = np.uint8, count: Optional[Integer] = None) -> NDArray:
    """
    Return a random RGB color using datatype specified.

    Parameters
    ----------
    dtype
      Color type of result.
    count
      If passed return (count, 4) colors instead of
      a single (4,) color.

    Returns
    ----------
    color : (4,) or (count, 4)
      Random color or colors that look "OK"
    """
    # generate a random hue
    hue = (np.random.random(count or 1) + 0.61803) % 1.0

    # saturation and "value" as constant
    sv = np.ones_like(hue) * 0.99
    # convert our random hue to RGBA
    colors = hsv_to_rgba(np.column_stack((hue, sv, sv)))

    # unspecified count is a single color
    if count is None:
        return colors[0]
    return colors


def vertex_to_face_color(vertex_colors: ArrayLike, faces: ArrayLike) -> NDArray[np.uint8]:
    """
    Convert a list of vertex colors to face colors.

    Parameters
    ----------
    vertex_colors: (n,(3,4)),  colors
    faces:         (m,3) int, face indexes

    Returns
    -----------
    face_colors: (m,4) colors
    """
    vertex_colors = to_rgba(vertex_colors)
    face_colors = vertex_colors[faces].mean(axis=1)
    return face_colors.astype(np.uint8)


def face_to_vertex_color(
    mesh, face_colors: ArrayLike, dtype: DTypeLike = np.uint8
) -> NDArray:
    """
    Convert face colors into vertex colors.

    Parameters
    -----------
    mesh : trimesh.Trimesh
      Mesh to convert colors for
    face_colors : `(len(mesh.faces), (3 | 4))` int
      The colors for each face of the mesh
    dtype
      What should colors be returned in.

    Returns
    -----------
    vertex_colors : `(len(mesh.vertices), 4)`
      Color for each vertex
    """
    rgba = to_rgba(face_colors)
    vertex = mesh.faces_sparse.dot(rgba.astype(np.float64))
    degree = mesh.vertex_degree

    # normalize color by the number of faces including
    # the vertex (i.e. the vertex degree)
    nonzero = degree > 0
    vertex[nonzero] /= degree[nonzero].reshape((-1, 1))

    assert vertex.shape == (len(mesh.vertices), 4)

    return vertex.astype(dtype)


def colors_to_materials(colors: ArrayLike, count: Optional[Integer] = None):
    """
    Convert a list of colors into a list of unique materials
    and material indexes.

    Parameters
    -----------
    colors : (n, 3) or (n, 4) float
      RGB or RGBA colors
    count : int
      Number of entities to apply color to

    Returns
    -----------
    diffuse : (m, 4) int
      Colors
    index : (count,) int
      Index of each color
    """

    # convert RGB to RGBA
    rgba = to_rgba(colors)

    # if we were only passed a single color
    if util.is_shape(rgba, (4,)) and count is not None:
        diffuse = rgba.reshape((-1, 4))
        index = np.zeros(count, dtype=np.int64)
    elif util.is_shape(rgba, (-1, 4)):
        # we were passed multiple colors
        # find the unique colors in the list to save as materials
        unique, index = unique_rows(rgba)
        diffuse = rgba[unique]
    else:
        raise ValueError("Colors not convertible!")

    return diffuse, index


def linear_color_map(
    values: ArrayLike, color_range: Optional[ArrayLike] = None
) -> NDArray:
    """
    Linearly interpolate a color lookup table from normalized
    values.

    For example if `color_range` has two values [`a`, `b`]
    `values` is `[0.0, 0.5, 1.0]`, this function will return
    [`a`, `(a+b)/2`, `b`].

    The default value for `color_range` is red-green, or you
    can pass in a full lookup table for a color map, i.e. a
    `(256, 3) float64` array of RGB colors such as our defaults:
    `trimesh.resources.get_json('color_map.json.gzip')['viridis']`

    Parameters
    --------------
    values : (n, ) float
      Normalized to 0.0-1.0 values to interpolate
    color_range : None or (n, 3|4)
      Evenly spaced colors to interpolate through
      where `n >= 2`.

    Returns
    ---------------
    colors : (n, 4) color_range.dtype
      RGBA colors for interpolated values
    """

    if color_range is None:
        # do a very unimaginative "red to green" linear scale
        color_range = np.array([[255, 0, 0, 255], [0, 255, 0, 255]], dtype=np.uint8)
    else:
        # make sure we have a numpy array
        color_range = np.asanyarray(color_range)

    # do simple checks on the color range shape
    if color_range.shape[0] < 2 or color_range.shape[1] < 3:
        raise ValueError(
            "color_range must be RGBA convertible and have more than 2 values!"
        )

    # float 1D array clamped to 0.0 - 1.0
    values = np.clip(np.asanyarray(values, dtype=np.float64).ravel(), 0.0, 1.0).reshape(
        (-1, 1)
    )

    # what is the maximum index of our colors
    max_index = len(color_range) - 1
    # convert our normalized values into a fractional index
    index = values.ravel() * max_index

    # get the left and right indexes
    # clipping should be a no-op based on above normalization but
    # be extra sure ceil isn't pushing us out of our array range
    bounds = np.clip(
        np.column_stack((np.floor(index), np.ceil(index))), 0.0, max_index
    ).astype(np.int64)

    # get the factor of how far each point is between `bounds` pair
    factor = index - bounds[:, 0]

    # reshape the factor into an interpolation
    multiplier = np.column_stack((1.0 - factor, factor)).reshape((-1, 2, 1))

    # get both colors, multiply them by the interpolation multiplier, and sum
    interpolated = (color_range.astype(np.float64)[bounds] * multiplier).sum(axis=1)

    # if we're returning integers make sure to round first
    if color_range.dtype.kind in "iu":
        return interpolated.round().astype(color_range.dtype)

    return interpolated.astype(color_range.dtype)


def interpolate(
    values: ArrayLike,
    color_map: Union[None, ColorMapType, Callable] = None,
    dtype: DTypeLike = np.uint8,
) -> NDArray:
    """
    Given a 1D list of values, return interpolated colors
    for the range.

    Parameters
    ---------------
    values : (n, ) float
      Values to be interpolated over
    color_map
      One of the four included color maps:
      ("viridis", "inferno", "plasma", "magma")
      Or a function, `matplotlib.pyplot.get_cmap


    Returns
    -------------
    interpolated : (n, 4) dtype
      Interpolated RGBA colors
    """

    # make `viridis` the default just like everyone else
    if color_map is None:
        color_map = "viridis"

    if callable(color_map):
        # should be a `matplotlib.pyplot.get_cmap` callable
        cmap = color_map
    elif isinstance(color_map, str):
        # color map is a named key in our packaged color maps
        available = get_json("color_map.json.gzip")
        if color_map not in available:
            # we could have added a fallback to matplotlib:
            # `from matplotlib.pyplot import get_cmap; cmap = get_cmap(name)`
            # but we don't want trimesh to depend on matplotlib as it is quite heavy
            raise ValueError(
                f"Included color maps are: {available.keys()}.\n\n"
                + "If you want to use a `matplotlib` color map you can "
                + "pass it as `color_map=matplotlib.pyplot.get_cmap(name)`"
            )

        # pass in the retrieved color map values to linear_color_map
        def cmap(x):
            return linear_color_map(x, np.array(available[color_map]))
    else:
        raise TypeError(f"Unknown color map: `{type(color_map)}`")

    # make input always float
    values = np.asanyarray(values, dtype=np.float64).ravel()

    # get both minumium and maximum values for range normalization
    v_min, v_max = values.min(), values.max()
    # offset to zero
    values -= v_min
    # normalize to the 0.0 - 1.0 range
    if v_min != v_max:
        values /= v_max - v_min

    # scale values to 0.0 - 1.0 and get colors
    colors = cmap(values)

    # convert to 0-255 RGBA
    rgba = to_rgba(colors, dtype=dtype)

    return rgba


def uv_to_color(uv, image) -> NDArray[np.uint8]:
    """
    Get the color in a texture image.

    Parameters
    -------------
    uv : (n, 2) float
      UV coordinates on texture image
    image : PIL.Image
      Texture image

    Returns
    ----------
    colors : (n, 4) uint4
      RGBA color at each of the UV coordinates
    """
    if image is None or uv is None:
        return None

    # UV coordinates should be (n, 2) float
    uv = np.asanyarray(uv, dtype=np.float64)

    # get texture image pixel positions of UV coordinates
    x = (uv[:, 0] * (image.width - 1)) % image.width
    y = ((1 - uv[:, 1]) * (image.height - 1)) % image.height

    # access colors from pixel locations
    # make sure image is RGBA before getting values
    colors = np.asanyarray(image.convert("RGBA"))[
        y.round().astype(np.int64) % image.height,
        x.round().astype(np.int64) % image.width,
    ]

    # conversion to RGBA should have corrected shape
    assert colors.ndim == 2 and colors.shape[1] == 4
    assert colors.dtype == np.uint8

    return colors


def uv_to_interpolated_color(uv: ArrayLike, image) -> NDArray[np.uint8]:
    """
    Get the color from texture image using bilinear sampling.

    Parameters
    -------------
    uv : (n, 2) float
      UV coordinates on texture image
    image : PIL.Image
      Texture image

    Returns
    ----------
    colors : (n, 4) uint8
      RGBA color at each of the UV coordinates.
    """
    if image is None or uv is None:
        return None

    # UV coordinates should be (n, 2) float
    uv = np.asanyarray(uv, dtype=np.float64)

    # get texture image pixel positions of UV coordinates
    x = uv[:, 0] * (image.width - 1)
    y = (1 - uv[:, 1]) * (image.height - 1)

    x_floor = np.floor(x).astype(np.int64) % image.width
    y_floor = np.floor(y).astype(np.int64) % image.height

    x_ceil = np.ceil(x).astype(np.int64) % image.width
    y_ceil = np.ceil(y).astype(np.int64) % image.height

    dx = x % image.width - x_floor
    dy = y % image.height - y_floor

    img = np.asanyarray(image.convert("RGBA"))

    colors00 = img[y_floor, x_floor]
    colors01 = img[y_floor, x_ceil]
    colors10 = img[y_ceil, x_floor]
    colors11 = img[y_ceil, x_ceil]

    a00 = (1 - dx) * (1 - dy)
    a01 = dx * (1 - dy)
    a10 = (1 - dx) * dy
    a11 = dx * dy

    a00 = np.repeat(a00[:, None], 4, axis=1)
    a01 = np.repeat(a01[:, None], 4, axis=1)
    a10 = np.repeat(a10[:, None], 4, axis=1)
    a11 = np.repeat(a11[:, None], 4, axis=1)

    # interpolated colors as floating point then convert back to uint8
    colors = (
        (a00 * colors00 + a01 * colors01 + a10 * colors10 + a11 * colors11)
        .round()
        .astype(np.uint8)
    )

    # conversion to RGBA should have corrected shape
    assert colors.ndim == 2 and colors.shape[1] == 4
    assert colors.dtype == np.uint8

    return colors


def color_to_uv(vertex_colors: ArrayLike):
    """
    Pack vertex colors into UV coordinates and a simple image material

    Parameters
    ------------
    vertex_colors : (n, 4) float
      Array of vertex colors.

    Returns
    ------------
    material : SimpleMaterial
      Material containing color information.
    uv : (n, 2) float
      Normalized UV coordinates
    """
    from .material import SimpleMaterial, empty_material

    # deduplicate the vertex colors
    unique, inverse = unique_rows(vertex_colors)

    # if there is only one color return a
    if len(unique) == 1:
        # return a simple single-pixel material
        material = empty_material(color=vertex_colors[unique[0]])
        uvs = np.zeros((len(vertex_colors), 2)) + 0.5
        return material, uvs

    from PIL import Image

    # return a square image of (size, size)
    size = int(np.ceil(np.sqrt(len(unique))))
    ctype = vertex_colors.shape[1]

    colors = np.zeros((size**2, ctype), dtype=vertex_colors.dtype)
    colors[: len(unique)] = vertex_colors[unique]

    # PIL has reversed x-y coordinates
    image = Image.fromarray(colors.reshape((size, size, ctype))[::-1])

    pos = np.arange(len(unique))
    # create tiled coordinates for the color pixels
    coords = np.column_stack((pos % size, np.floor(pos / size)))

    # normalize the index coords into 0.0 - 1.0
    # and offset them to be centered on the pixel
    coords = (coords / size) + (1.0 / (size * 2.0))
    uvs = coords[inverse]

    if tol.strict:
        # check the packed colors against the image
        check = uv_to_color(image=image, uv=uvs)
        assert np.all(check == vertex_colors)

    return SimpleMaterial(image=image), uvs


# set an arbitrary grey as the default color
DEFAULT_COLOR = np.array([102, 102, 102, 255], dtype=np.uint8)
DEFAULT_MAT = {
    "material_diffuse": np.array([102, 102, 102, 255], dtype=np.uint8),
    "material_ambient": np.array([64, 64, 64, 255], dtype=np.uint8),
    "material_specular": np.array([197, 197, 197, 255], dtype=np.uint8),
    "material_shine": 77.0,
}
