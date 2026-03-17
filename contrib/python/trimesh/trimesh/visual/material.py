"""
material.py
-------------

Store visual materials as objects.
"""

import abc
import copy

import numpy as np

from .. import exceptions, util
from ..constants import tol
from ..typed import NDArray, Optional
from . import color

try:
    from PIL import Image
except BaseException as E:
    Image = exceptions.ExceptionWrapper(E)

# epsilon for comparing floating point
_eps = 1e-5


class Material(util.ABC):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("must be subclassed!")

    @abc.abstractmethod
    def __hash__(self):
        raise NotImplementedError("must be subclassed!")

    @property
    @abc.abstractmethod
    def main_color(self):
        """
        The "average" color of this material.

        Returns
        ---------
        color : (4,) uint8
          Average color of this material.
        """

    @property
    def name(self):
        if hasattr(self, "_name"):
            return self._name
        return "material_0"

    @name.setter
    def name(self, value):
        if value is not None:
            self._name = value

    def copy(self):
        return copy.deepcopy(self)


class SimpleMaterial(Material):
    """
    Hold a single image texture.
    """

    def __init__(
        self,
        image=None,
        diffuse=None,
        ambient=None,
        specular=None,
        glossiness=None,
        name=None,
        **kwargs,
    ):
        # save image
        self.image = image
        self.name = name
        # save material colors as RGBA
        self.ambient = color.to_rgba(ambient)
        self.diffuse = color.to_rgba(diffuse)
        self.specular = color.to_rgba(specular)

        # save Ns
        self.glossiness = glossiness

        # save other keyword arguments
        self.kwargs = kwargs

    def to_color(self, uv):
        return color.uv_to_color(uv, self.image)

    def to_obj(self, name=None):
        """
        Convert the current material to an OBJ format
        material.

        Parameters
        -----------
        name : str or None
          Name to apply to the material

        Returns
        -----------
        tex_name : str
          Name of material
        mtl_name : str
          Name of mtl file in files
        files : dict
          Data as {file name : bytes}
        """
        # material parameters as 0.0-1.0 RGB
        Ka = color.to_float(self.ambient)[:3]
        Kd = color.to_float(self.diffuse)[:3]
        Ks = color.to_float(self.specular)[:3]

        if name is None:
            name = self.name

        # create an MTL file
        mtl = [
            f"newmtl {name}",
            "Ka {:0.8f} {:0.8f} {:0.8f}".format(*Ka),
            "Kd {:0.8f} {:0.8f} {:0.8f}".format(*Kd),
            "Ks {:0.8f} {:0.8f} {:0.8f}".format(*Ks),
            f"Ns {self.glossiness:0.8f}",
        ]

        # collect the OBJ data into files
        data = {}

        if self.image is not None:
            image_type = self.image.format
            # what is the name of the export image to save
            if image_type is None:
                image_type = "png"
            image_name = f"{name}.{image_type.lower()}"
            # save the reference to the image
            mtl.append(f"map_Kd {image_name}")

            # save the image texture as bytes in the original format
            f_obj = util.BytesIO()
            self.image.save(fp=f_obj, format=image_type)
            f_obj.seek(0)
            data[image_name] = f_obj.read()

        data[f"{name}.mtl"] = "\n".join(mtl).encode("utf-8")

        return data, name

    def __hash__(self):
        """
        Provide a hash of the material so we can detect
        duplicates.

        Returns
        ------------
        hash : int
          Hash of image and parameters
        """
        if hasattr(self.image, "tobytes"):
            # start with hash of raw image bytes
            hashed = hash(self.image.tobytes())
        else:
            # otherwise start with zero
            hashed = 0
        # we will add additional parameters with
        # an in-place xor of the additional value
        # if stored as numpy arrays add parameters
        if hasattr(self.ambient, "tobytes"):
            hashed ^= hash(self.ambient.tobytes())
        if hasattr(self.diffuse, "tobytes"):
            hashed ^= hash(self.diffuse.tobytes())
        if hasattr(self.specular, "tobytes"):
            hashed ^= hash(self.specular.tobytes())
        if isinstance(self.glossiness, float):
            hashed ^= hash(int(self.glossiness * 1000))
        return hashed

    @property
    def main_color(self):
        """
        Return the most prominent color.
        """
        return self.diffuse

    @property
    def glossiness(self):
        if hasattr(self, "_glossiness"):
            return self._glossiness
        return 1.0

    @glossiness.setter
    def glossiness(self, value):
        if value is None:
            return
        self._glossiness = float(value)

    def to_pbr(self):
        """
        Convert the current simple material to a
        PBR material.

        Returns
        ------------
        pbr : PBRMaterial
          Contains material information in PBR format.
        """
        # convert specular exponent to roughness
        roughness = (2 / (self.glossiness + 2)) ** (1.0 / 4.0)

        return PBRMaterial(
            roughnessFactor=roughness,
            baseColorTexture=self.image,
            baseColorFactor=self.diffuse,
        )


class MultiMaterial(Material):
    def __init__(self, materials=None, **kwargs):
        """
        Wrapper for a list of Materials.

        Parameters
        ----------
        materials : Optional[List[Material]]
            List of materials with which the container to be initialized.
        """
        if materials is None:
            self.materials = []
        else:
            self.materials = materials

    def to_pbr(self):
        """
        TODO : IMPLEMENT
        """
        pbr = [m for m in self.materials if isinstance(m, PBRMaterial)]
        if len(pbr) == 0:
            return PBRMaterial()
        return pbr[0]

    def __hash__(self):
        """
        Provide a hash of the multi material so we can detect
        duplicates.

        Returns
        ------------
        hash : int
          Xor hash of the contained materials.
        """
        return int(np.bitwise_xor.reduce([hash(m) for m in self.materials]))

    def __iter__(self):
        return iter(self.materials)

    def __next__(self):
        return next(self.materials)

    def __len__(self):
        return len(self.materials)

    @property
    def main_color(self):
        """
        The "average" color of this material.

        Returns
        ---------
        color : (4,) uint8
          Average color of this material.
        """

    def add(self, material):
        """
        Adds new material to the container.

        Parameters
        ----------
        material : Material
            The material to be added.
        """
        self.materials.append(material)

    def get(self, idx):
        """
        Get material by index.

        Parameters
        ----------
        idx : int
            Index of the material to be retrieved.

        Returns
        -------
            The material on the given index.
        """
        return self.materials[idx]


class PBRMaterial(Material):
    """
    Create a material for physically based rendering as
    specified by GLTF 2.0:
    https://git.io/fhkPZ

    Parameters with `Texture` in them must be PIL.Image objects
    """

    def __init__(
        self,
        name=None,
        emissiveFactor=None,
        emissiveTexture=None,
        baseColorFactor=None,
        metallicFactor=None,
        roughnessFactor=None,
        normalTexture=None,
        occlusionTexture=None,
        baseColorTexture=None,
        metallicRoughnessTexture=None,
        doubleSided=False,
        alphaMode=None,
        alphaCutoff=None,
        **kwargs,
    ):
        # store values in an internal dict
        self._data = {}

        # (3,) float
        self.emissiveFactor = emissiveFactor
        # (3,) or (4,) float with RGBA colors
        self.baseColorFactor = baseColorFactor

        # float
        self.metallicFactor = metallicFactor
        self.roughnessFactor = roughnessFactor
        self.alphaCutoff = alphaCutoff

        # PIL image
        self.normalTexture = normalTexture
        self.emissiveTexture = emissiveTexture
        self.occlusionTexture = occlusionTexture
        self.baseColorTexture = baseColorTexture
        self.metallicRoughnessTexture = metallicRoughnessTexture

        # bool
        self.doubleSided = doubleSided

        # str
        self.name = name
        self.alphaMode = alphaMode

        if len(kwargs) > 0:
            util.log.debug(
                "unsupported material keys: {}".format(", ".join(kwargs.keys()))
            )

    @property
    def emissiveFactor(self):
        """
        The factors for the emissive color of the material.
        This value defines linear multipliers for the sampled
        texels of the emissive texture.

        Returns
        -----------
        emissiveFactor : (3,) float
           Ech element in the array MUST be greater than
           or equal to 0 and less than or equal to 1.
        """
        return self._data.get("emissiveFactor")

    @emissiveFactor.setter
    def emissiveFactor(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("emissiveFactor", None)
        else:
            # non-None values must be a floating point
            emissive = np.array(value, dtype=np.float64).reshape(3)
            if emissive.min() < -_eps or emissive.max() > (1 + _eps):
                raise ValueError("all factors must be between 0.0-1.0")
            self._data["emissiveFactor"] = emissive

    @property
    def alphaMode(self):
        """
        The material alpha rendering mode enumeration
        specifying the interpretation of the alpha value of
        the base color.

        Returns
        -----------
        alphaMode : str
          One of 'OPAQUE', 'MASK', 'BLEND'
        """
        return self._data.get("alphaMode")

    @alphaMode.setter
    def alphaMode(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("alphaMode", None)
        else:
            # non-None values must be one of three values
            value = str(value).upper().strip()
            if value not in ["OPAQUE", "MASK", "BLEND"]:
                raise ValueError("incorrect alphaMode: %s", value)
            self._data["alphaMode"] = value

    @property
    def alphaCutoff(self):
        """
        Specifies the cutoff threshold when in MASK alpha mode.
        If the alpha value is greater than or equal to this value
        then it is rendered as fully opaque, otherwise, it is rendered
        as fully transparent. A value greater than 1.0 will render
        the entire material as fully transparent. This value MUST be
        ignored for other alpha modes. When alphaMode is not defined,
        this value MUST NOT be defined.

        Returns
        -----------
        alphaCutoff : float
          Value of cutoff.
        """
        return self._data.get("alphaCutoff")

    @alphaCutoff.setter
    def alphaCutoff(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("alphaCutoff", None)
        else:
            self._data["alphaCutoff"] = float(value)

    @property
    def doubleSided(self):
        """
        Specifies whether the material is double sided.

        Returns
        -----------
        doubleSided : bool
          Specifies whether the material is double sided.
        """
        return self._data.get("doubleSided")

    @doubleSided.setter
    def doubleSided(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("doubleSided", None)
        else:
            self._data["doubleSided"] = bool(value)

    @property
    def metallicFactor(self):
        """
        The factor for the metalness of the material. This value
        defines a linear multiplier for the sampled metalness values
        of the metallic-roughness texture.


        Returns
        -----------
        metallicFactor : float
          How metally is the material
        """
        return self._data.get("metallicFactor")

    @metallicFactor.setter
    def metallicFactor(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("metallicFactor", None)
        else:
            self._data["metallicFactor"] = float(value)

    @property
    def roughnessFactor(self):
        """
        The factor for the roughness of the material. This value
        defines a linear multiplier for the sampled roughness values
        of the metallic-roughness texture.

        Returns
        -----------
        roughnessFactor : float
          Roughness of material.
        """
        return self._data.get("roughnessFactor")

    @roughnessFactor.setter
    def roughnessFactor(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("roughnessFactor", None)
        else:
            self._data["roughnessFactor"] = float(value)

    @property
    def baseColorFactor(self):
        """
        The factors for the base color of the material. This
        value defines linear multipliers for the sampled texels
        of the base color texture.

        Returns
        ---------
        color : (4,) uint8
          RGBA color
        """
        return self._data.get("baseColorFactor")

    @baseColorFactor.setter
    def baseColorFactor(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("baseColorFactor", None)
        else:
            # non-None values must be RGBA color
            self._data["baseColorFactor"] = color.to_rgba(value)

    @property
    def normalTexture(self):
        """
        The normal map texture.

        Returns
        ----------
        image : PIL.Image
          Normal texture.
        """
        return self._data.get("normalTexture")

    @normalTexture.setter
    def normalTexture(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("normalTexture", None)
        else:
            self._data["normalTexture"] = value

    @property
    def emissiveTexture(self):
        """
        The emissive texture.

        Returns
        ----------
        image : PIL.Image
          Emissive texture.
        """
        return self._data.get("emissiveTexture")

    @emissiveTexture.setter
    def emissiveTexture(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("emissiveTexture", None)
        else:
            self._data["emissiveTexture"] = value

    @property
    def occlusionTexture(self):
        """
        The occlusion texture.

        Returns
        ----------
        image : PIL.Image
          Occlusion texture.
        """
        return self._data.get("occlusionTexture")

    @occlusionTexture.setter
    def occlusionTexture(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("occlusionTexture", None)
        else:
            self._data["occlusionTexture"] = value

    @property
    def baseColorTexture(self):
        """
        The base color texture image.

        Returns
        ----------
        image : PIL.Image
          Color texture.
        """
        return self._data.get("baseColorTexture")

    @baseColorTexture.setter
    def baseColorTexture(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("baseColorTexture", None)
        else:
            # non-None values must be RGBA color
            self._data["baseColorTexture"] = value

    @property
    def metallicRoughnessTexture(self):
        """
        The metallic-roughness texture.

        Returns
        ----------
        image : PIL.Image
          Metallic-roughness texture.
        """
        return self._data.get("metallicRoughnessTexture")

    @metallicRoughnessTexture.setter
    def metallicRoughnessTexture(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("metallicRoughnessTexture", None)
        else:
            self._data["metallicRoughnessTexture"] = value

    @property
    def name(self):
        return self._data.get("name")

    @name.setter
    def name(self, value):
        if value is None:
            # passing none effectively removes value
            self._data.pop("name", None)
        else:
            self._data["name"] = value

    def copy(self):
        # doing a straight deepcopy fails due to PIL images
        kwargs = {}
        # collect stored values as kwargs
        for k, v in self._data.items():
            if v is None:
                continue
            if hasattr(v, "copy"):
                # use an objects explicit copy if available
                kwargs[k] = v.copy()
            else:
                # otherwise just hope deepcopy does something
                kwargs[k] = copy.deepcopy(v)
        return PBRMaterial(**kwargs)

    def to_color(self, uv):
        """
        Get the rough color at a list of specified UV
        coordinates.

        Parameters
        -------------
        uv : (n, 2) float
          UV coordinates on the material

        Returns
        -------------
        colors
        """
        colors = color.uv_to_color(uv=uv, image=self.baseColorTexture)
        if colors is None and self.baseColorFactor is not None:
            colors = self.baseColorFactor.copy()
        return colors

    def to_simple(self):
        """
        Get a copy of the current PBR material as
        a simple material.

        Returns
        ------------
        simple : SimpleMaterial
          Contains material information in a simple manner
        """
        # `self.baseColorFactor` is really a linear value
        # so the "right" thing to do here would probably be:
        #  `diffuse = color.to_rgba(color.linear_to_srgb(self.baseColorFactor))`
        # however that subtle transformation seems like it would confuse
        # the absolute heck out of people looking at this. If someone wants
        # this and has opinions happy to accept that change but otherwise
        # we'll just keep passing it through as "probably-RGBA-like"
        return SimpleMaterial(
            image=self.baseColorTexture,
            diffuse=self.baseColorFactor,
            name=self.name,
        )

    @property
    def main_color(self):
        # will return default color if None
        result = color.to_rgba(self.baseColorFactor)
        return result

    def __hash__(self):
        """
        Provide a hash of the material so we can detect
        duplicate materials.

        Returns
        ------------
        hash : int
          Hash of image and parameters
        """
        return hash(
            b"".join(
                np.asanyarray(v).tobytes() for v in self._data.values() if v is not None
            )
        )


def empty_material(color: Optional[NDArray[np.uint8]] = None) -> SimpleMaterial:
    """
    Return an empty material set to a single color

    Parameters
    -----------
    color : None or (3,) uint8
      RGB color

    Returns
    -------------
    material : SimpleMaterial
      Image is a a four pixel RGB
    """

    # create a one pixel RGB image
    return SimpleMaterial(image=color_image(color=color))


def color_image(color: Optional[NDArray[np.uint8]] = None):
    """
    Generate an image with one color.

    Parameters
    ----------
    color
      Optional uint8 color

    Returns
    ----------
    image
      A (2, 2) RGBA image with the specified color.
    """
    # only raise an error further down the line
    if isinstance(Image, exceptions.ExceptionWrapper):
        return Image
    # start with a single default RGBA color
    single = np.array([100, 100, 100, 255], dtype=np.uint8)
    if np.shape(color) in ((3,), (4,)):
        single[: len(color)] = color
    # tile into a (2, 2) image and return
    return Image.fromarray(np.tile(single, 4).reshape((2, 2, 4)).astype(np.uint8))


def pack(
    materials,
    uvs,
    deduplicate=True,
    padding: int = 2,
    max_tex_size_individual=8192,
    max_tex_size_fused=8192,
):
    """
    Pack multiple materials with texture into a single material.

    UV coordinates outside of the 0.0-1.0 range will be coerced
    into this range using a "wrap" behavior (i.e. modulus).

    Alpha blending and backface culling settings are not supported!
    Returns a material with alpha values set, but alpha blending disabled.

    Parameters
    -----------
    materials : (n,) Material
      List of multiple materials
    uvs : (n, m, 2) float
      Original UV coordinates
    padding : int
      Number of pixels to pad each image with.
    max_tex_size_individual : int
      Maximum size of each individual texture.
    max_tex_size_fused : int | None
      Maximum size of the combined texture.
      Individual texture size will be reduced to fit.
      Set to None to allow infinite size.

    Returns
    ------------
    material : SimpleMaterial
      Combined material.
    uv : (p, 2) float
      Combined UV coordinates in the 0.0-1.0 range.
    """

    import collections

    from PIL import Image

    from ..path import packing

    def multiply_factor(img, factor, mode):
        """
        Multiply an image by a factor.
        """
        if factor is None:
            return img.convert(mode)
        img = (
            (np.array(img.convert(mode), dtype=np.float64) * factor)
            .round()
            .astype(np.uint8)
        )
        return Image.fromarray(img)

    def get_base_color_texture(mat):
        """
        Logic for extracting a simple image from each material.
        """
        # extract an image for each material
        img = None
        if isinstance(mat, PBRMaterial):
            if mat.baseColorTexture is not None:
                img = multiply_factor(
                    mat.baseColorTexture, factor=mat.baseColorFactor, mode="RGBA"
                )
            elif mat.baseColorFactor is not None:
                # Per glTF 2.0 spec (https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html):
                # - baseColorFactor: "defines linear multipliers for the sampled texels"
                # - baseColorTexture: "RGB components MUST be encoded with the sRGB transfer function"
                #
                # Therefore when creating a texture from baseColorFactor values,
                # we need to convert from linear to sRGB space
                c_linear = color.to_float(mat.baseColorFactor).reshape(4)

                # Apply proper sRGB gamma correction to RGB channels
                c_srgb = np.concatenate(
                    [color.linear_to_srgb(c_linear[:3]), c_linear[3:4]]
                )

                # Convert to uint8
                c = np.round(c_srgb * 255).astype(np.uint8)
                assert c.shape == (4,)
                img = color_image(c)

            if img is not None and mat.alphaMode != "BLEND":
                # we can't handle alpha blending well, but we can bake alpha cutoff
                mode = img.mode
                img = np.array(img)
                if mat.alphaMode == "MASK":
                    img[..., 3] = np.where(img[..., 3] > mat.alphaCutoff * 255, 255, 0)
                elif mat.alphaMode == "OPAQUE" or mat.alphaMode is None:
                    if "A" in mode:
                        img[..., 3] = 255
                img = Image.fromarray(img)
        elif getattr(mat, "image", None) is not None:
            img = mat.image
        elif np.shape(getattr(mat, "diffuse", [])) == (4,):
            # return a one pixel image
            img = color_image(mat.diffuse)

        if img is None:
            # return a one pixel image
            img = color_image()
        # make sure we're always returning in RGBA mode
        return img.convert("RGBA")

    def get_metallic_roughness_texture(mat):
        """
        Logic for extracting a simple image from each material.
        """
        # extract an image for each material
        img = None
        if isinstance(mat, PBRMaterial):
            if mat.metallicRoughnessTexture is not None:
                if mat.metallicRoughnessTexture.format == "BGR":
                    img = np.array(mat.metallicRoughnessTexture.convert("RGB"))
                else:
                    img = np.array(mat.metallicRoughnessTexture)

                if len(img.shape) == 2 or img.shape[-1] == 1:
                    img = img.reshape(*img.shape[:2], 1)
                    img = np.concatenate(
                        [
                            img,
                            np.ones_like(img[..., :1]) * 255,
                            np.zeros_like(img[..., :1]),
                        ],
                        axis=-1,
                    )
                elif img.shape[-1] == 2:
                    img = np.concatenate([img, np.zeros_like(img[..., :1])], axis=-1)

                if mat.metallicFactor is not None:
                    img[..., 0] = np.round(
                        img[..., 0].astype(np.float64) * mat.metallicFactor
                    ).astype(np.uint8)
                if mat.roughnessFactor is not None:
                    img[..., 1] = np.round(
                        img[..., 1].astype(np.float64) * mat.roughnessFactor
                    ).astype(np.uint8)
                img = Image.fromarray(img)
            else:
                metallic = 0.0 if mat.metallicFactor is None else mat.metallicFactor
                roughness = 1.0 if mat.roughnessFactor is None else mat.roughnessFactor
                # glTF expects B=metallic, G=roughness, R=unused
                # https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html#metallic-roughness-material
                metallic_roughnesss = np.round(
                    np.array([0.0, roughness, metallic], dtype=np.float64) * 255
                )
                img = Image.fromarray(metallic_roughnesss[None, None].astype(np.uint8))
        return img

    def get_emissive_texture(mat):
        """
        Logic for extracting a simple image from each material.
        """
        # extract an image for each material
        img = None
        if isinstance(mat, PBRMaterial):
            if mat.emissiveTexture is not None:
                img = multiply_factor(mat.emissiveTexture, mat.emissiveFactor, "RGB")
            elif mat.emissiveFactor is not None:
                c = color.to_rgba(mat.emissiveFactor)
                img = Image.fromarray(c.reshape((1, 1, -1)))
            else:
                img = Image.fromarray(np.reshape([0, 0, 0], (1, 1, 3)).astype(np.uint8))
        # make sure we're always returning in RGBA mode
        return img.convert("RGB")

    def get_normal_texture(mat):
        # there is no default normal texture
        return getattr(mat, "normalTexture", None)

    def get_occlusion_texture(mat):
        occlusion_texture = getattr(mat, "occlusionTexture", None)
        if occlusion_texture is None:
            occlusion_texture = Image.fromarray(np.array([[255]], dtype=np.uint8))
        else:
            occlusion_texture = occlusion_texture.convert("L")
        return occlusion_texture

    def resize_images(images, sizes):
        resized = []
        for img, size in zip(images, sizes):
            if img is None:
                resized.append(None)
            else:
                img = img.resize(size)
                resized.append(img)
        return resized

    packed = {}

    def pack_images(images):
        # run image packing with our material-specific settings
        # Note: deduplication is disabled to ensure consistent packing
        # across different texture types (base color, metallic/roughness, etc)

        # see if we've already run this packing image
        key = hash(tuple(sorted([id(i) for i in images])))
        assert key not in packed
        if key in packed:
            return packed[key]

        # otherwise run packing now
        result = packing.images(
            images,
            deduplicate=False,  # Disabled to ensure consistent texture layouts
            power_resize=True,
            seed=42,
            iterations=10,
            spacing=int(padding),
        )
        packed[key] = result
        return result

    if deduplicate:
        # start by collecting a list of indexes for each material hash
        unique_idx = collections.defaultdict(list)
        [unique_idx[hash(m)].append(i) for i, m in enumerate(materials)]
        # now we only need the indexes and don't care about the hashes
        mat_idx = list(unique_idx.values())
    else:
        # otherwise just use all the indexes
        mat_idx = np.arange(len(materials)).reshape((-1, 1))

    if len(mat_idx) == 1:
        # if there is only one material we can just return it
        return materials[0], np.vstack(uvs)

    assert set(np.concatenate(mat_idx).ravel()) == set(range(len(uvs)))
    assert len(uvs) == len(materials)
    use_pbr = any(isinstance(m, PBRMaterial) for m in materials)

    # in some cases, the fused scene results in huge trimsheets
    # we can try to prevent this by downscaling the textures iteratively
    down_scale_iterations = 6
    while down_scale_iterations > 0:
        # collect the images from the materials
        images = [get_base_color_texture(materials[g[0]]) for g in mat_idx]

        if use_pbr:
            # if we have PBR materials, collect all possible textures and
            # determine the largest size per material
            metallic_roughness = [
                get_metallic_roughness_texture(materials[g[0]]) for g in mat_idx
            ]
            emissive = [get_emissive_texture(materials[g[0]]) for g in mat_idx]
            normals = [get_normal_texture(materials[g[0]]) for g in mat_idx]
            occlusion = [get_occlusion_texture(materials[g[0]]) for g in mat_idx]

            unpadded_sizes = []
            for textures in zip(images, metallic_roughness, emissive, normals, occlusion):
                # remove None textures
                textures = [tex for tex in textures if tex is not None]
                tex_sizes = np.stack([np.array(tex.size) for tex in textures])
                max_tex_size = tex_sizes.max(axis=0)
                if max_tex_size.max() > max_tex_size_individual:
                    scale = max_tex_size.max() / max_tex_size_individual
                    max_tex_size = np.round(max_tex_size / scale).astype(np.int64)

                unpadded_sizes.append(tuple(max_tex_size))

            # use the same size for all of them to ensure
            # that texture atlassing is identical
            images = resize_images(images, unpadded_sizes)
            metallic_roughness = resize_images(metallic_roughness, unpadded_sizes)
            emissive = resize_images(emissive, unpadded_sizes)
            normals = resize_images(normals, unpadded_sizes)
            occlusion = resize_images(occlusion, unpadded_sizes)
        else:
            # for non-pbr materials, just use the original image size
            unpadded_sizes = []
            for img in images:
                tex_size = np.array(img.size)
                if tex_size.max() > max_tex_size_individual:
                    scale = tex_size.max() / max_tex_size_individual
                    tex_size = np.round(tex_size / scale).astype(np.int64)
                unpadded_sizes.append(tex_size)

        # pack the multiple images into a single large image
        final, offsets = pack_images(images)

        # if the final image is too large, reduce the maximum texture size and repeat
        if (
            max_tex_size_fused is not None
            and final.size[0] * final.size[1] > max_tex_size_fused**2
        ):
            down_scale_iterations -= 1
            max_tex_size_individual //= 2
        else:
            break

    if use_pbr:
        # even if we only need the first two channels, store RGB, because
        # PIL 'LA' mode images are interpreted incorrectly in other 3D software
        final_metallic_roughness, _ = pack_images(metallic_roughness)

        if all(np.array(x).max() == 0 for x in emissive):
            # if all emissive textures are black, don't use emissive
            emissive = None
            final_emissive = None
        else:
            final_emissive, _ = pack_images(emissive)

        if all(n is not None for n in normals):
            # only use normal texture if all materials use them
            # how else would you handle missing normals?
            final_normals, _ = pack_images(normals)
        else:
            final_normals = None

        if any(np.array(o).min() < 255 for o in occlusion):
            # only use occlusion texture if any material actually has an occlusion value
            final_occlusion, _ = pack_images(occlusion)
        else:
            final_occlusion = None

    # the size of the final texture image
    final_size = np.array(final.size, dtype=np.float64)
    # collect scaled new UV coordinates by material index
    new_uv = {}
    for group, img, offset in zip(mat_idx, images, offsets):
        # how big was the original image
        uv_scale = (np.array(img.size) - 1) / final_size
        # the units of offset are *pixels of the final image*
        # thus to scale them to normalized UV coordinates we
        # what is the offset in fractions of final image
        uv_offset = offset / (final_size - 1)
        # scale and translate each of the new UV coordinates
        # also make sure they are in 0.0-1.0 using modulus (i.e. wrap)
        half = 0.5 / np.array(img.size)

        for g in group:
            # only wrap pixels that are outside of 0.0-1.0.
            # use a small leeway of half a pixel for floating point inaccuracies and
            # the case of uv==1.0
            uvg = uvs[g].copy()

            # now wrap anything more than half a pixel outside
            uvg[np.logical_or(uvg < -half, uvg > (1.0 + half))] %= 1.0
            # clamp to half a pixel
            uvg = np.clip(uvg, half, 1.0 - half)

            # apply the scale and offset
            moved = (uvg * uv_scale) + uv_offset

            if tol.strict:
                # the color from the original coordinates and image
                old = color.uv_to_interpolated_color(uvs[g], img)
                # the color from the packed image
                new = color.uv_to_interpolated_color(moved, final)
                assert np.allclose(old, new, atol=6)

            new_uv[g] = moved

    # stack the new UV coordinates in the original order
    stacked = np.vstack([new_uv[i] for i in range(len(uvs))])

    if use_pbr:
        return (
            PBRMaterial(
                baseColorTexture=final,
                metallicRoughnessTexture=final_metallic_roughness,
                emissiveTexture=final_emissive,
                emissiveFactor=[1.0, 1.0, 1.0] if final_emissive else None,
                alphaMode=None,  # unfortunately, we can't handle alpha blending well
                doubleSided=False,  # TODO how to handle this?
                normalTexture=final_normals,
                occlusionTexture=final_occlusion,
            ),
            stacked,
        )
    else:
        return SimpleMaterial(image=final), stacked
