"""Custom errors."""


class DataSourceError(RuntimeError):
    """Errors relating to opening or closing an OGRDataSource (with >= 1 layers)."""


class DataLayerError(RuntimeError):
    """Errors relating to working with a single OGRLayer."""


class CRSError(DataLayerError):
    """Errors relating to getting or setting CRS values."""


class FeatureError(DataLayerError):
    """Errors related to reading or writing a feature."""


class GeometryError(DataLayerError):
    """Errors relating to getting or setting a geometry field."""


class FieldError(DataLayerError):
    """Errors relating to getting or setting a non-geometry field."""
