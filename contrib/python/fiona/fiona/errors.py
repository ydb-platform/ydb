# Errors.


class FionaError(Exception):
    """Base Fiona error"""


class FionaValueError(FionaError, ValueError):
    """Fiona-specific value errors"""


class AttributeFilterError(FionaValueError):
    """Error processing SQL WHERE clause with the dataset."""


class DriverError(FionaValueError):
    """Encapsulates unsupported driver and driver mode errors."""


class SchemaError(FionaValueError):
    """When a schema mapping has no properties or no geometry."""


class CRSError(FionaValueError):
    """When a crs mapping has neither init or proj items."""


class UnsupportedOperation(FionaError):
    """Raised when reading from a file opened in 'w' mode"""


class DataIOError(OSError):
    """IO errors involving driver registration or availability."""


class DriverIOError(OSError):
    """A format specific driver error."""


class DriverSupportError(DriverIOError):
    """Driver does not support schema"""


class DatasetDeleteError(OSError):
    """Failure to delete a dataset"""


class FieldNameEncodeError(UnicodeEncodeError):
    """Failure to encode a field name."""


class UnsupportedGeometryTypeError(KeyError):
    """When a OGR geometry type isn't supported by Fiona."""


class GeometryTypeValidationError(FionaValueError):
    """Tried to write a geometry type not specified in the schema"""


class TransactionError(RuntimeError):
    """Failure relating to GDAL transactions"""


class EnvError(FionaError):
    """Environment Errors"""


class GDALVersionError(FionaError):
    """Raised if the runtime version of GDAL does not meet the required
    version of GDAL.
    """


class TransformError(FionaError):
    """Raised if a coordinate transformation fails."""


class OpenerRegistrationError(FionaError):
    """Raised when a Python file opener can not be registered."""


class PathError(FionaError):
    """Raised when a dataset path is malformed or invalid"""


class FionaDeprecationWarning(DeprecationWarning):
    """A warning about deprecation of Fiona features"""


class FeatureWarning(UserWarning):
    """A warning about serialization of a feature"""


class ReduceError(FionaError):
    """"Raised when reduce operation fails."""
