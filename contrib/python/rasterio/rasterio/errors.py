"""Errors and Warnings."""

from click import FileError


class RasterioError(Exception):
    """Root exception class"""


class InvalidArrayError(RasterioError):
    """Raised when methods are passed invalid arrays"""


class WindowError(RasterioError):
    """Raised when errors occur during window operations"""


class CRSError(ValueError):
    """Raised when a CRS string or mapping is invalid or cannot serve
    to define a coordinate transformation."""


class EnvError(RasterioError):
    """Raised when the state of GDAL/AWS environment cannot be created
    or modified."""


class DriverCapabilityError(RasterioError, ValueError):
    """Raised when a format driver can't a feature such as writing."""


class DriverRegistrationError(ValueError):
    """Raised when a format driver is requested but is not registered."""


class FileOverwriteError(FileError):
    """Raised when Rasterio's CLI refuses to clobber output files."""

    def __init__(self, message):
        """Raise FileOverwriteError with message as hint."""
        super().__init__('', hint=message)


class RasterioIOError(RasterioError, OSError):
    """Raised when a dataset cannot be opened or accessed."""


class NodataShadowWarning(UserWarning):
    """Warn that a dataset's nodata attribute is shadowing its alpha band."""

    def __str__(self):
        return ("The dataset's nodata attribute is shadowing "
                "the alpha band. All masks will be determined "
                "by the nodata attribute")


class NotGeoreferencedWarning(UserWarning):
    """Warn that a dataset isn't georeferenced."""


class TransformWarning(UserWarning):
    """Warn that coordinate transformations may behave unexpectedly"""


class RPCError(ValueError):
    """Raised when RPC transformation is invalid"""


class ShapeSkipWarning(UserWarning):
    """Warn that an invalid or empty shape in a collection has been skipped"""


class GDALBehaviorChangeException(RuntimeError):
    """Raised when GDAL's behavior differs from the given arguments.  For
    example, antimeridian cutting is always on as of GDAL 2.2.0.  Users
    expecting it to be off will be presented with a MultiPolygon when the
    rest of their code expects a Polygon.

    Examples
    --------

    .. code-block:: python

        # Raises an exception on GDAL >= 2.2.0
        rasterio.warp.transform_geometry(
            src_crs, dst_crs, antimeridian_cutting=False)
    """


class GDALOptionNotImplementedError(RasterioError):
    """A dataset opening or dataset creation option can't be supported

    This will be raised from Rasterio's shim modules. For example, when
    a user passes arguments to open_dataset() that can't be evaluated
    by GDAL 1.x.
    """


class GDALVersionError(RasterioError):
    """Raised if the runtime version of GDAL does not meet the required version of GDAL."""


class WindowEvaluationError(ValueError):
    """Raised when window evaluation fails"""


class RasterioDeprecationWarning(FutureWarning):
    """Rasterio module deprecations

    Following https://www.python.org/dev/peps/pep-0565/#additional-use-case-for-futurewarning
    we base this on FutureWarning while continuing to support Python < 3.7.
    """


class RasterBlockError(RasterioError):
    """Raised when raster block access fails"""


class BandOverviewError(UserWarning):
    """Raised when a band overview access fails."""


class WarpOptionsError(RasterioError):
    """Raised when options for a warp operation are invalid"""


class UnsupportedOperation(RasterioError):
    """Raised when reading from a file opened in 'w' mode"""


class OverviewCreationError(RasterioError):
    """Raised when creation of an overview fails"""


class DatasetAttributeError(RasterioError, NotImplementedError):
    """Raised when dataset attributes are misused"""


class PathError(RasterioError):
    """Raised when a dataset path is malformed or invalid"""


class ResamplingAlgorithmError(RasterioError):
    """Raised when a resampling algorithm is invalid or inapplicable"""


class TransformError(RasterioError):
    """Raised when transform arguments are invalid"""


class WarpedVRTError(RasterioError):
    """Raised when WarpedVRT can't be initialized"""


class DatasetIOShapeError(RasterioError):
    """Raised when data buffer shape is a mismatch when reading and writing"""


class WarpOperationError(RasterioError):
    """Raised when a warp operation fails."""


class StatisticsError(RasterioError):
    """Raised when dataset statistics cannot be computed."""


class OpenerRegistrationError(RasterioError):
    """Raised when a Python file opener can not be registered."""


class MergeError(RasterioError):
    """Raised when rasters cannot be merged."""


class StackError(RasterioError):
    """Raised when rasters cannot be stacked."""
