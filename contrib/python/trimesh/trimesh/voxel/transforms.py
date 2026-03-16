import numpy as np

from .. import caching, util
from .. import transformations as tr
from ..typed import Optional


class Transform:
    """
    Class for caching metadata associated with 4x4 transformations.

    The transformation matrix is used to define relevant properties
    for the voxels, including pitch and origin.
    """

    def __init__(self, matrix, datastore: Optional[caching.DataStore] = None):
        """
        Initialize with a transform.

        Parameters
        -----------
        matrix : (4, 4) float
          Homogeneous transformation matrix
        datastore
          If passed store the actual values in a reference to
          another datastore.
        """
        matrix = np.asanyarray(matrix, dtype=np.float64)
        if matrix.shape != (4, 4) or not np.allclose(matrix[3, :], [0, 0, 0, 1]):
            raise ValueError("matrix is invalid!")

        # store matrix as data
        if datastore is None:
            self._data = caching.DataStore()
        elif isinstance(datastore, caching.DataStore):
            self._data = datastore
        else:
            raise ValueError(f"{type(datastore)} != caching.DataStore")

        self._data["transform_matrix"] = matrix
        # dump cache when matrix changes
        self._cache = caching.Cache(id_function=self._data.__hash__)

    def __hash__(self):
        """
        Get the hash of the current transformation matrix.

        Returns
        ------------
        hash : str
          Hash of transformation matrix
        """
        return self._data.__hash__()

    @property
    def translation(self):
        """
        Get the translation component of the matrix

        Returns
        ------------
        translation : (3,) float
          Cartesian translation
        """
        return self._data["transform_matrix"][:3, 3]

    @property
    def matrix(self):
        """
        Get the homogeneous transformation matrix.

        Returns
        -------------
        matrix : (4, 4) float
          Transformation matrix
        """
        return self._data["transform_matrix"]

    @matrix.setter
    def matrix(self, values):
        """
        Set the homogeneous transformation matrix.

        Parameters
        -------------
        matrix : (4, 4) float
          Transformation matrix
        """
        values = np.asanyarray(values, dtype=np.float64)
        if values.shape != (4, 4):
            raise ValueError("matrix must be (4, 4)!")
        self._data["transform_matrix"] = values

    @caching.cache_decorator
    def scale(self):
        """
        Get the scale factor of the current transformation.

        Returns
        -------------
        scale : (3,) float
          Scale factor from the matrix
        """
        # get the current transformation
        matrix = self.matrix
        # get the (3,) diagonal of the rotation component
        scale = np.diag(matrix[:3, :3])
        if not np.allclose(matrix[:3, :3], scale * np.eye(3), scale * 1e-6 + 1e-8):
            raise RuntimeError("transform features a shear or rotation")
        return scale

    @caching.cache_decorator
    def pitch(self):
        scale = self.scale
        if not util.allclose(scale[0], scale[1:], np.max(np.abs(scale)) * 1e-6 + 1e-8):
            raise RuntimeError("transform features non-uniform scaling")
        return scale

    @caching.cache_decorator
    def unit_volume(self):
        """Volume of a transformed unit cube."""
        return np.linalg.det(self._data["transform_matrix"][:3, :3])

    def apply_transform(self, matrix):
        """Mutate the transform in-place and return self."""
        self.matrix = np.matmul(matrix, self.matrix)
        return self

    def apply_translation(self, translation):
        """Mutate the transform in-place and return self."""
        self.matrix[:3, 3] += translation
        return self

    def apply_scale(self, scale):
        """Mutate the transform in-place and return self."""
        self.matrix[:3] *= scale
        return self

    def transform_points(self, points):
        """
        Apply the transformation to points (not in-place).

        Parameters
        ----------
        points: (n, 3) float
          Points in cartesian space

        Returns
        ----------
        transformed : (n, 3) float
          Points transformed by matrix
        """
        if self.is_identity:
            return points.copy()
        return tr.transform_points(points.reshape(-1, 3), self.matrix).reshape(
            points.shape
        )

    def inverse_transform_points(self, points):
        """Apply the inverse transformation to points (not in-place)."""
        if self.is_identity:
            return points
        return tr.transform_points(points.reshape(-1, 3), self.inverse_matrix).reshape(
            points.shape
        )

    @caching.cache_decorator
    def inverse_matrix(self):
        inv = np.linalg.inv(self.matrix)
        inv.flags.writeable = False
        return inv

    def copy(self):
        return Transform(matrix=self.matrix)

    @caching.cache_decorator
    def is_identity(self):
        """
        Flags this transformation being sufficiently close to eye(4).
        """
        return util.allclose(self.matrix, np.eye(4), 1e-8)
