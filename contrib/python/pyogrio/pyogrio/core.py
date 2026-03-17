"""Core functions to interact with OGR data sources."""

from pathlib import Path

from pyogrio._env import GDALEnv
from pyogrio.util import (
    _mask_to_wkb,
    _preprocess_options_key_value,
    get_vsi_path_or_buffer,
)

with GDALEnv():
    from pyogrio._err import _register_error_handler
    from pyogrio._io import ogr_list_layers, ogr_read_bounds, ogr_read_info
    from pyogrio._ogr import (
        _get_drivers_for_path,
        _register_drivers,
        get_gdal_config_option as _get_gdal_config_option,
        get_gdal_data_path as _get_gdal_data_path,
        get_gdal_geos_version,
        get_gdal_version,
        get_gdal_version_string,
        init_gdal_data as _init_gdal_data,
        init_proj_data as _init_proj_data,
        ogr_list_drivers,
        set_gdal_config_options as _set_gdal_config_options,
    )
    from pyogrio._vsi import (
        ogr_vsi_listtree,
        ogr_vsi_rmtree,
        ogr_vsi_unlink,
    )

    _init_gdal_data()
    _init_proj_data()
    _register_drivers()
    _register_error_handler()

    __gdal_version__ = get_gdal_version()
    __gdal_version_string__ = get_gdal_version_string()
    __gdal_geos_version__ = get_gdal_geos_version()


def list_drivers(read=False, write=False):
    """List drivers available in GDAL.

    Parameters
    ----------
    read: bool, optional (default: False)
        If True, will only return drivers that are known to support read capabilities.
    write: bool, optional (default: False)
        If True, will only return drivers that are known to support write capabilities.

    Returns
    -------
    dict
        Mapping of driver name to file mode capabilities: ``"r"``: read, ``"w"``: write.
        Drivers that are available but with unknown support are marked with ``"?"``

    """
    drivers = ogr_list_drivers()

    if read:
        drivers = {k: v for k, v in drivers.items() if v.startswith("r")}

    if write:
        drivers = {k: v for k, v in drivers.items() if v.endswith("w")}

    return drivers


def detect_write_driver(path):
    """Attempt to infer the driver for a path by extension or prefix.

    Only drivers that support write capabilities will be detected.

    If the path cannot be resolved to a single driver, a ValueError will be
    raised.

    Parameters
    ----------
    path : str
        data source path

    Returns
    -------
    str
        name of the driver, if detected

    """
    # try to infer driver from path
    drivers = _get_drivers_for_path(path)

    if len(drivers) == 0:
        raise ValueError(
            f"Could not infer driver from path: {path}; please specify driver "
            "explicitly"
        )

    # if there are multiple drivers detected, user needs to specify the correct
    # one manually
    elif len(drivers) > 1:
        raise ValueError(
            f"Could not infer driver from path: {path}; multiple drivers are "
            f"available for that extension: {', '.join(drivers)}.  Please "
            "specify driver explicitly."
        )

    return drivers[0]


def list_layers(path_or_buffer, /):
    """List layers available in an OGR data source.

    NOTE: includes both spatial and nonspatial layers.

    Parameters
    ----------
    path_or_buffer : str, pathlib.Path, bytes, or file-like
        A dataset path or URI, raw buffer, or file-like object with a read method.

    Returns
    -------
    ndarray shape (2, n)
        array of pairs of [<layer name>, <layer geometry type>]
        Note: geometry is `None` for nonspatial layers.

    """
    return ogr_list_layers(get_vsi_path_or_buffer(path_or_buffer))


def read_bounds(
    path_or_buffer,
    /,
    layer=None,
    skip_features=0,
    max_features=None,
    where=None,
    bbox=None,
    mask=None,
):
    """Read bounds of each feature.

    This can be used to assist with spatial indexing and partitioning, in
    order to avoid reading all features into memory.  It is roughly 2-3x faster
    than reading the full geometry and attributes of a dataset.

    Parameters
    ----------
    path_or_buffer : str, pathlib.Path, bytes, or file-like
        A dataset path or URI, raw buffer, or file-like object with a read method.
    layer : int or str, optional (default: first layer)
        If an integer is provided, it corresponds to the index of the layer
        with the data source.  If a string is provided, it must match the name
        of the layer in the data source.  Defaults to first layer in data source.
    skip_features : int, optional (default: 0)
        Number of features to skip from the beginning of the file before returning
        features.  Must be less than the total number of features in the file.
    max_features : int, optional (default: None)
        Number of features to read from the file.  Must be less than the total
        number of features in the file minus ``skip_features`` (if used).
    where : str, optional (default: None)
        Where clause to filter features in layer by attribute values.  Uses a
        restricted form of SQL WHERE clause, defined here:
        http://ogdi.sourceforge.net/prop/6.2.CapabilitiesMetadata.html
        Examples: ``"ISO_A3 = 'CAN'"``, ``"POP_EST > 10000000 AND POP_EST < 100000000"``
    bbox : tuple of (xmin, ymin, xmax, ymax), optional (default: None)
        If present, will be used to filter records whose geometry intersects this
        box.  This must be in the same CRS as the dataset.  If GEOS is present
        and used by GDAL, only geometries that intersect this bbox will be
        returned; if GEOS is not available or not used by GDAL, all geometries
        with bounding boxes that intersect this bbox will be returned.
    mask : Shapely geometry, optional (default: None)
        If present, will be used to filter records whose geometry intersects
        this geometry.  This must be in the same CRS as the dataset.  If GEOS is
        present and used by GDAL, only geometries that intersect this geometry
        will be returned; if GEOS is not available or not used by GDAL, all
        geometries with bounding boxes that intersect the bounding box of this
        geometry will be returned.  Requires Shapely >= 2.0.
        Cannot be combined with ``bbox`` keyword.

    Returns
    -------
    tuple of (fids, bounds)
        fids are global IDs read from the FID field of the dataset
        bounds are ndarray of shape(4, n) containing ``xmin``, ``ymin``, ``xmax``,
        ``ymax``

    """
    return ogr_read_bounds(
        get_vsi_path_or_buffer(path_or_buffer),
        layer=layer,
        skip_features=skip_features,
        max_features=max_features or 0,
        where=where,
        bbox=bbox,
        mask=_mask_to_wkb(mask),
    )


def read_info(
    path_or_buffer,
    /,
    layer=None,
    encoding=None,
    force_feature_count=False,
    force_total_bounds=False,
    **kwargs,
):
    """Read information about an OGR data source.

    ``crs``, ``geometry`` and ``total_bounds`` will be ``None`` and ``features`` will be
    0 for a nonspatial layer.

    ``features`` will be -1 if this is an expensive operation for this driver. You can
    force it to be calculated using the ``force_feature_count`` parameter.

    ``total_bounds`` is the 2-dimensional extent of all features within the dataset:
    (xmin, ymin, xmax, ymax). It will be None if this is an expensive operation for this
    driver or if the data source is nonspatial. You can force it to be calculated using
    the ``force_total_bounds`` parameter.

    ``fid_column`` is the name of the FID field in the data source, if the FID is
    physically stored (e.g. in GPKG). If the FID is just a sequence, ``fid_column``
    will be "" (e.g. ESRI Shapefile).

    ``geometry_name`` is the name of the field where the main geometry is stored in the
    data data source, if the field name can by customized (e.g. in GPKG). If no custom
    name is supported, ``geometry_name`` will be "" (e.g. ESRI Shapefile).

    ``encoding`` will be ``UTF-8`` if either the native encoding is likely to be
    ``UTF-8`` or GDAL can automatically convert from the detected native encoding
    to ``UTF-8``.

    Parameters
    ----------
    path_or_buffer : str, pathlib.Path, bytes, or file-like
        A dataset path or URI, raw buffer, or file-like object with a read method.
    layer : str or int, optional
        Name or index of layer in data source.  Reads the first layer by default.
    encoding : str, optional (default: None)
        If present, will be used as the encoding for reading string values from
        the data source, unless encoding can be inferred directly from the data
        source.
    force_feature_count : bool, optional (default: False)
        True if the feature count should be computed even if it is expensive.
    force_total_bounds : bool, optional (default: False)
        True if the total bounds should be computed even if it is expensive.
    **kwargs
        Additional driver-specific dataset open options passed to OGR.  Invalid
        options will trigger a warning.

    Returns
    -------
    dict
        A dictionary with the following keys::

            {
                "layer_name": "<layer name>",
                "crs": "<crs>",
                "fields": <ndarray of field names>,
                "dtypes": <ndarray of field dtypes>,
                "ogr_types": <ndarray of OGR field types>,
                "ogr_subtypes": <ndarray of OGR field subtypes>,
                "encoding": "<encoding>",
                "fid_column": "<fid column name or "">",
                "geometry_name": "<geometry column name or "">",
                "geometry_type": "<geometry type>",
                "features": <feature count or -1>,
                "total_bounds": <tuple with total bounds or None>,
                "driver": "<driver>",
                "capabilities": "<dict of driver capabilities>"
                "dataset_metadata": "<dict of dataset metadata or None>"
                "layer_metadata": "<dict of layer metadata or None>"
            }

    """
    dataset_kwargs = _preprocess_options_key_value(kwargs) if kwargs else {}

    return ogr_read_info(
        get_vsi_path_or_buffer(path_or_buffer),
        layer=layer,
        encoding=encoding,
        force_feature_count=force_feature_count,
        force_total_bounds=force_total_bounds,
        dataset_kwargs=dataset_kwargs,
    )


def set_gdal_config_options(options):
    """Set GDAL configuration options.

    Options are listed here: https://trac.osgeo.org/gdal/wiki/ConfigOptions

    No error is raised if invalid option names are provided.

    These options are applied for an entire session rather than for individual
    functions.

    Parameters
    ----------
    options : dict
        If present, provides a mapping of option name / value pairs for GDAL
        configuration options.  ``True`` / ``False`` are normalized to ``'ON'``
        / ``'OFF'``. A value of ``None`` for a config option can be used to clear out a
        previously set value.

    """
    _set_gdal_config_options(options)


def get_gdal_config_option(name):
    """Get the value for a GDAL configuration option.

    Parameters
    ----------
    name : str
        name of the option to retrive

    Returns
    -------
    value of the option or None if not set
        ``'ON'`` / ``'OFF'`` are normalized to ``True`` / ``False``.

    """
    return _get_gdal_config_option(name)


def get_gdal_data_path():
    """Get the path to the directory GDAL uses to read data files.

    Returns
    -------
    str, or None if data directory was not found

    """
    return _get_gdal_data_path()


def vsi_listtree(path: str | Path, pattern: str | None = None):
    """Recursively list the contents of a VSI directory.

    An fnmatch pattern can be specified to filter the directories/files
    returned.

    Parameters
    ----------
    path : str or pathlib.Path
        Path to the VSI directory to be listed.
    pattern : str, optional
        Pattern to filter results, in fnmatch format.

    """
    if isinstance(path, Path):
        path = path.as_posix()

    return ogr_vsi_listtree(path, pattern=pattern)


def vsi_rmtree(path: str | Path):
    """Recursively remove VSI directory.

    Parameters
    ----------
    path : str or pathlib.Path
        path to the VSI directory to be removed.

    """
    if isinstance(path, Path):
        path = path.as_posix()

    ogr_vsi_rmtree(path)


def vsi_unlink(path: str | Path):
    """Remove a VSI file.

    Parameters
    ----------
    path : str or pathlib.Path
        path to vsimem file to be removed

    """
    if isinstance(path, Path):
        path = path.as_posix()

    ogr_vsi_unlink(path)
