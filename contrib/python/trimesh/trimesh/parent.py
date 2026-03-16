"""
parent.py
-------------

The base class for Trimesh, PointCloud, and Scene objects
"""

import abc
import os
from dataclasses import dataclass

import numpy as np

from . import bounds, caching
from . import transformations as tf
from .caching import cache_decorator
from .constants import tol
from .resolvers import ResolverLike
from .typed import Any, ArrayLike, Dict, NDArray, Optional, float64
from .util import ABC


@dataclass
class LoadSource:
    """
    Save information about where a particular object was loaded from.
    """

    # a file-like object that can be accessed
    file_obj: Optional[Any] = None

    # a cleaned file type string, i.e. "stl"
    file_type: Optional[str] = None

    # if this was originally loaded from a file path
    # save it here so we can check it later.
    file_path: Optional[str] = None

    # did we open `file_obj` ourselves?
    was_opened: bool = False

    # a resolver for loading assets next to the file
    resolver: Optional[ResolverLike] = None

    @property
    def file_name(self) -> Optional[str]:
        """
        Get just the file name from the path if available.

        Returns
        ---------
        file_name
          Just the file name, i.e. for file_path="/a/b/c.stl" -> "c.stl"
        """
        if self.file_path is None:
            return None
        return os.path.basename(self.file_path)

    def __getstate__(self) -> Dict:
        # this overrides the `pickle.dump` behavior for this class
        # we cannot pickle a file object so return `file_obj: None` for pickles
        return {k: v if k != "file_obj" else None for k, v in self.__dict__.items()}

    def __deepcopy__(self, *args):
        return LoadSource(**self.__getstate__())


class Geometry(ABC):
    """
    `Geometry` is the parent class for all geometry.

    By decorating a method with `abc.abstractmethod` it means
    the objects that inherit from `Geometry` MUST implement
    those methods.
    """

    # geometry should have a dict to store loose metadata
    metadata: Dict

    @property
    def source(self) -> LoadSource:
        """
        Where and what was this current geometry loaded from?

        Returns
        --------
        source
          If loaded from a file, has the path, type, etc.
        """
        # this should have been tacked on by the loader
        # but we want to *always* be able to access
        # a value like `mesh.source.file_type` so add a default
        current = getattr(self, "_source", None)
        if current is not None:
            return current
        self._source = LoadSource()
        return self._source

    @property
    @abc.abstractmethod
    def identifier_hash(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def bounds(self) -> NDArray[np.float64]:
        pass

    @property
    @abc.abstractmethod
    def extents(self) -> NDArray[np.float64]:
        pass

    @abc.abstractmethod
    def apply_transform(self, matrix: ArrayLike) -> Any:
        pass

    @property
    @abc.abstractmethod
    def is_empty(self) -> bool:
        pass

    def __hash__(self):
        """
        Get a hash of the current geometry.

        Returns
        ---------
        hash
          Hash of current graph and geometry.
        """
        return self._data.__hash__()  # type: ignore

    @abc.abstractmethod
    def copy(self):
        pass

    @abc.abstractmethod
    def show(self):
        pass

    @abc.abstractmethod
    def __add__(self, other):
        pass

    @abc.abstractmethod
    def export(self, file_obj, file_type=None):
        pass

    def __repr__(self) -> str:
        """
        Print quick summary of the current geometry without
        computing properties.

        Returns
        -----------
        repr : str
          Human readable quick look at the geometry.
        """
        elements = []
        if hasattr(self, "vertices"):
            # for Trimesh and PointCloud
            elements.append(f"vertices.shape={self.vertices.shape}")
        if hasattr(self, "faces"):
            # for Trimesh
            elements.append(f"faces.shape={self.faces.shape}")
        if hasattr(self, "geometry") and isinstance(self.geometry, dict):
            # for Scene
            elements.append(f"len(geometry)={len(self.geometry)}")
        if "Voxel" in type(self).__name__:
            # for VoxelGrid objects
            elements.append(str(self.shape)[1:-1])
        if "file_name" in self.metadata:
            display = self.metadata["file_name"]
            elements.append(f"name=`{display}`")
        return "<trimesh.{}({})>".format(type(self).__name__, ", ".join(elements))

    def apply_translation(self, translation: ArrayLike):
        """
        Translate the current mesh.

        Parameters
        ----------
        translation : (3,) float
          Translation in XYZ
        """
        translation = np.asanyarray(translation, dtype=np.float64)
        if translation.shape == (2,):
            # create a planar matrix if we were passed a 2D offset
            return self.apply_transform(tf.planar_matrix(offset=translation))
        elif translation.shape != (3,):
            raise ValueError("Translation must be (3,) or (2,)!")

        # manually create a translation matrix
        matrix = np.eye(4)
        matrix[:3, 3] = translation
        return self.apply_transform(matrix)

    def apply_scale(self, scaling):
        """
        Scale the mesh.

        Parameters
        ----------
        scaling : float or (3,) float
          Scale factor to apply to the mesh
        """
        matrix = tf.scale_and_translate(scale=scaling)
        # apply_transform will work nicely even on negative scales
        return self.apply_transform(matrix)

    def __radd__(self, other):
        """
        Concatenate the geometry allowing concatenation with
        built in `sum()` function:
          `sum(Iterable[trimesh.Trimesh])`

        Parameters
        ------------
        other : Geometry
          Geometry or 0

        Returns
        ----------
        concat : Geometry
          Geometry of combined result
        """

        if other == 0:
            # adding 0 to a geometry never makes sense
            return self
        # otherwise just use the regular add function
        return self.__add__(type(self)(other))

    @cache_decorator
    def scale(self) -> float:
        """
        A loosely specified "order of magnitude scale" for the
        geometry which always returns a value and can be used
        to make code more robust to large scaling differences.

        It returns the diagonal of the axis aligned bounding box
        or if anything is invalid or undefined, `1.0`.

        Returns
        ----------
        scale : float
          Approximate order of magnitude scale of the geometry.
        """
        # if geometry is empty return 1.0
        if self.extents is None:
            return 1.0

        # get the length of the AABB diagonal
        scale = float((self.extents**2).sum() ** 0.5)
        if scale < tol.zero:
            return 1.0

        return scale

    @property
    def units(self) -> Optional[str]:
        """
        Definition of units for the mesh.

        Returns
        ----------
        units : str
          Unit system mesh is in, or None if not defined
        """
        return self.metadata.get("units", None)

    @units.setter
    def units(self, value: str) -> None:
        """
        Define the units of the current mesh.
        """
        self.metadata["units"] = str(value).lower().strip()


class Geometry3D(Geometry):
    """
    The `Geometry3D` object is the parent object of geometry objects
    which are three dimensional, including Trimesh, PointCloud,
    and Scene objects.
    """

    @caching.cache_decorator
    def bounding_box(self):
        """
        An axis aligned bounding box for the current mesh.

        Returns
        ----------
        aabb : trimesh.primitives.Box
          Box object with transform and extents defined
          representing the axis aligned bounding box of the mesh
        """
        from . import primitives

        transform = np.eye(4)
        # translate to center of axis aligned bounds
        transform[:3, 3] = self.bounds.mean(axis=0)

        return primitives.Box(transform=transform, extents=self.extents, mutable=False)

    @caching.cache_decorator
    def bounding_box_oriented(self):
        """
        An oriented bounding box for the current mesh.

        Returns
        ---------
        obb : trimesh.primitives.Box
          Box object with transform and extents defined
          representing the minimum volume oriented
          bounding box of the mesh
        """
        from . import bounds, primitives

        to_origin, extents = bounds.oriented_bounds(self)
        return primitives.Box(
            transform=np.linalg.inv(to_origin), extents=extents, mutable=False
        )

    @caching.cache_decorator
    def bounding_sphere(self):
        """
        A minimum volume bounding sphere for the current mesh.

        Note that the Sphere primitive returned has an unpadded
        exact `sphere_radius` so while the distance of every vertex
        of the current mesh from sphere_center will be less than
        sphere_radius, the faceted sphere primitive may not
        contain every vertex.

        Returns
        --------
        minball : trimesh.primitives.Sphere
          Sphere primitive containing current mesh
        """
        from . import nsphere, primitives

        center, radius = nsphere.minimum_nsphere(self)
        return primitives.Sphere(center=center, radius=radius, mutable=False)

    @caching.cache_decorator
    def bounding_cylinder(self):
        """
        A minimum volume bounding cylinder for the current mesh.

        Returns
        --------
        mincyl : trimesh.primitives.Cylinder
          Cylinder primitive containing current mesh
        """
        from . import bounds, primitives

        kwargs = bounds.minimum_cylinder(self)
        return primitives.Cylinder(mutable=False, **kwargs)

    @caching.cache_decorator
    def bounding_primitive(self):
        """
        The minimum volume primitive (box, sphere, or cylinder) that
        bounds the mesh.

        Returns
        ---------
        bounding_primitive : object
          Smallest primitive which bounds the mesh:
          trimesh.primitives.Sphere
          trimesh.primitives.Box
          trimesh.primitives.Cylinder
        """
        options = [
            self.bounding_box_oriented,
            self.bounding_sphere,
            self.bounding_cylinder,
        ]
        volume_min = np.argmin([i.volume for i in options])
        return options[volume_min]

    def apply_obb(self, **kwargs) -> NDArray[float64]:
        """
        Apply the oriented bounding box transform to the current mesh.

        This will result in a mesh with an AABB centered at the
        origin and the same dimensions as the OBB.

        Parameters
        ------------
        kwargs
          Passed through to `bounds.oriented_bounds`

        Returns
        ----------
        matrix : (4, 4) float
          Transformation matrix that was applied
          to mesh to move it into OBB frame
        """
        # save the pre-transform volume
        if tol.strict and hasattr(self, "volume"):
            volume = self.volume

        # calculate the OBB passing keyword arguments through
        matrix, extents = bounds.oriented_bounds(self, **kwargs)
        # apply the transform
        self.apply_transform(matrix)

        if tol.strict:
            # obb transform should not have changed volume
            if hasattr(self, "volume") and getattr(self, "is_watertight", False):
                assert np.isclose(self.volume, volume)
            # overall extents should match what we expected
            assert np.allclose(self.extents, extents)

        return matrix
