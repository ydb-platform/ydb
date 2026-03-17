"""Functions for reading and writing GeoPandas dataframes."""

import json
import os
import warnings
from datetime import datetime

import numpy as np

from pyogrio._compat import (
    HAS_GEOPANDAS,
    HAS_PYARROW,
    PANDAS_GE_15,
    PANDAS_GE_20,
    PANDAS_GE_22,
    PANDAS_GE_30,
    PYARROW_GE_19,
    __gdal_version__,
)
from pyogrio.errors import DataSourceError
from pyogrio.raw import (
    DRIVERS_NO_MIXED_DIMENSIONS,
    DRIVERS_NO_MIXED_SINGLE_MULTI,
    _get_write_path_driver,
    read,
    read_arrow,
    write,
)


def _stringify_path(path):
    """Convert path-like to a string if possible, pass-through other objects."""
    if isinstance(path, str):
        return path

    # checking whether path implements the filesystem protocol
    if hasattr(path, "__fspath__"):
        return path.__fspath__()

    # pass-though other objects
    return path


def _try_parse_datetime(ser, datetime_as_string: bool, mixed_offsets_as_utc: bool):
    import pandas as pd  # only called when pandas is known to be installed
    from pandas.api.types import is_string_dtype

    datetime_kwargs = {}
    if datetime_as_string:
        if not is_string_dtype(ser.dtype):
            # Support to return datetimes as strings using arrow only available for
            # GDAL >= 3.11, so convert to string here if needed.
            res = ser.astype("str")
            if not PANDAS_GE_30:
                # astype("str") also stringifies missing values in pandas < 3
                res[ser.isna()] = None
            res = res.str.replace(" ", "T")
            return res
        if __gdal_version__ < (3, 7, 0):
            # GDAL < 3.7 doesn't return datetimes in ISO8601 format, so fix that
            return ser.str.replace(" ", "T").str.replace("/", "-")
        return ser

    if PANDAS_GE_22:
        datetime_kwargs["format"] = "ISO8601"
    elif PANDAS_GE_20:
        datetime_kwargs["format"] = "ISO8601"
        datetime_kwargs["errors"] = "ignore"
    else:
        datetime_kwargs["yearfirst"] = True

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            ".*parsing datetimes with mixed time zones will raise.*",
            FutureWarning,
        )

        warning = "Error parsing datetimes, original strings are returned: {message}"
        try:
            res = pd.to_datetime(ser, **datetime_kwargs)

            # With pandas >2 and <3, mixed time zones were returned as pandas
            # Timestamps, so convert them to datetime objects.
            if not mixed_offsets_as_utc and PANDAS_GE_20 and res.dtype == "object":
                res = res.map(lambda x: x.to_pydatetime(), na_action="ignore")

        except Exception as ex:
            if isinstance(ex, ValueError) and "Mixed timezones detected" in str(ex):
                # Parsing mixed time zones with to_datetime is not supported
                # anymore in pandas >= 3.0, leading to a ValueError.
                if mixed_offsets_as_utc:
                    # Convert mixed time zone datetimes to UTC.
                    try:
                        res = pd.to_datetime(ser, utc=True, **datetime_kwargs)
                    except Exception as ex:
                        warnings.warn(warning.format(message=str(ex)), stacklevel=3)
                        return ser
                else:
                    # Using map seems to be the fastest way to convert the strings to
                    # datetime objects.
                    try:
                        res = ser.map(datetime.fromisoformat, na_action="ignore")
                    except Exception as ex:
                        warnings.warn(warning.format(message=str(ex)), stacklevel=3)
                        return ser

            else:
                # If the error is not related to mixed time zones, log it and return
                # the original series.
                warnings.warn(warning.format(message=str(ex)), stacklevel=3)
                if __gdal_version__ < (3, 7, 0):
                    # GDAL < 3.7 doesn't return datetimes in ISO8601 format, so fix that
                    return ser.str.replace(" ", "T").str.replace("/", "-")

                return ser

    # For pandas < 3.0, to_datetime converted mixed time zone data to datetime objects.
    # For mixed_offsets_as_utc they should be converted to UTC though...
    if mixed_offsets_as_utc and res.dtype in ("object", "string"):
        try:
            res = pd.to_datetime(ser, utc=True, **datetime_kwargs)
        except Exception as ex:
            warnings.warn(warning.format(message=str(ex)), stacklevel=3)

    if res.dtype.kind == "M":  # any datetime64
        # GDAL only supports ms precision, convert outputs to match.
        # Pandas 2.0 supports datetime[ms] directly, prior versions only support [ns],
        # Instead, round the values to [ms] precision.
        if PANDAS_GE_20:
            res = res.dt.as_unit("ms")
        else:
            res = res.dt.round(freq="ms")

    return res


def read_dataframe(
    path_or_buffer,
    /,
    layer=None,
    encoding=None,
    columns=None,
    read_geometry=True,
    force_2d=False,
    skip_features=0,
    max_features=None,
    where=None,
    bbox=None,
    mask=None,
    fids=None,
    sql=None,
    sql_dialect=None,
    fid_as_index=False,
    use_arrow=None,
    on_invalid="raise",
    arrow_to_pandas_kwargs=None,
    datetime_as_string=False,
    mixed_offsets_as_utc=True,
    **kwargs,
):
    """Read from an OGR data source to a GeoPandas GeoDataFrame or Pandas DataFrame.

    If the data source does not have a geometry column or ``read_geometry`` is False,
    a DataFrame will be returned.

    If you read data with datetime columns containing time zone information, check out
    the notes below.

    Requires ``geopandas`` >= 0.8.

    Parameters
    ----------
    path_or_buffer : pathlib.Path or str, or bytes buffer
        A dataset path or URI, raw buffer, or file-like object with a read method.
    layer : int or str, optional (default: first layer)
        If an integer is provided, it corresponds to the index of the layer
        with the data source.  If a string is provided, it must match the name
        of the layer in the data source.  Defaults to first layer in data source.
    encoding : str, optional (default: None)
        If present, will be used as the encoding for reading string values from
        the data source.  By default will automatically try to detect the native
        encoding and decode to ``UTF-8``.
    columns : list-like, optional (default: all columns)
        List of column names to import from the data source.  Column names must
        exactly match the names in the data source, and will be returned in
        the order they occur in the data source.  To avoid reading any columns,
        pass an empty list-like.  If combined with ``where`` parameter, must
        include columns referenced in the ``where`` expression or the data may
        not be correctly read; the data source may return empty results or
        raise an exception (behavior varies by driver).
    read_geometry : bool, optional (default: True)
        If True, will read geometry into a GeoSeries.  If False, a Pandas DataFrame
        will be returned instead.
    force_2d : bool, optional (default: False)
        If the geometry has Z values, setting this to True will cause those to
        be ignored and 2D geometries to be returned
    skip_features : int, optional (default: 0)
        Number of features to skip from the beginning of the file before
        returning features.  If greater than available number of features, an
        empty DataFrame will be returned.  Using this parameter may incur
        significant overhead if the driver does not support the capability to
        randomly seek to a specific feature, because it will need to iterate
        over all prior features.
    max_features : int, optional (default: None)
        Number of features to read from the file.
    where : str, optional (default: None)
        Where clause to filter features in layer by attribute values. If the data source
        natively supports SQL, its specific SQL dialect should be used (eg. SQLite and
        GeoPackage: `SQLITE`_, PostgreSQL). If it doesn't, the `OGRSQL WHERE`_ syntax
        should be used. Note that it is not possible to overrule the SQL dialect, this
        is only possible when you use the ``sql`` parameter.
        Examples: ``"ISO_A3 = 'CAN'"``, ``"POP_EST > 10000000 AND POP_EST < 100000000"``
    bbox : tuple of (xmin, ymin, xmax, ymax) (default: None)
        If present, will be used to filter records whose geometry intersects this
        box.  This must be in the same CRS as the dataset.  If GEOS is present
        and used by GDAL, only geometries that intersect this bbox will be
        returned; if GEOS is not available or not used by GDAL, all geometries
        with bounding boxes that intersect this bbox will be returned.
        Cannot be combined with ``mask`` keyword.
    mask : Shapely geometry, optional (default: None)
        If present, will be used to filter records whose geometry intersects
        this geometry.  This must be in the same CRS as the dataset.  If GEOS is
        present and used by GDAL, only geometries that intersect this geometry
        will be returned; if GEOS is not available or not used by GDAL, all
        geometries with bounding boxes that intersect the bounding box of this
        geometry will be returned.  Requires Shapely >= 2.0.
        Cannot be combined with ``bbox`` keyword.
    fids : array-like, optional (default: None)
        Array of integer feature id (FID) values to select. Cannot be combined
        with other keywords to select a subset (``skip_features``,
        ``max_features``, ``where``, ``bbox``, ``mask``, or ``sql``). Note that
        the starting index is driver and file specific (e.g. typically 0 for
        Shapefile and 1 for GeoPackage, but can still depend on the specific
        file). The performance of reading a large number of features usings FIDs
        is also driver specific and depends on the value of ``use_arrow``. The order
        of the rows returned is undefined. If you would like to sort based on FID, use
        ``fid_as_index=True`` to have the index of the GeoDataFrame returned set to the
        FIDs of the features read. If ``use_arrow=True``, the number of FIDs is limited
        to 4997 for drivers with 'OGRSQL' as default SQL dialect. To read a larger
        number of FIDs, set ``user_arrow=False``.
    sql : str, optional (default: None)
        The SQL statement to execute. Look at the sql_dialect parameter for more
        information on the syntax to use for the query. When combined with other
        keywords like ``columns``, ``skip_features``, ``max_features``,
        ``where``, ``bbox``, or ``mask``, those are applied after the SQL query.
        Be aware that this can have an impact on performance, (e.g. filtering
        with the ``bbox`` or ``mask`` keywords may not use spatial indexes).
        Cannot be combined with the ``layer`` or ``fids`` keywords.
    sql_dialect : str, optional (default: None)
        The SQL dialect the SQL statement is written in. Possible values:

          - **None**: if the data source natively supports SQL, its specific SQL dialect
            will be used by default (eg. SQLite and Geopackage: `SQLITE`_, PostgreSQL).
            If the data source doesn't natively support SQL, the `OGRSQL`_ dialect is
            the default.
          - '`OGRSQL`_': can be used on any data source. Performance can suffer
            when used on data sources with native support for SQL.
          - '`SQLITE`_': can be used on any data source. All spatialite_
            functions can be used. Performance can suffer on data sources with
            native support for SQL, except for Geopackage and SQLite as this is
            their native SQL dialect.

    fid_as_index : bool, optional (default: False)
        If True, will use the FIDs of the features that were read as the
        index of the GeoDataFrame.  May start at 0 or 1 depending on the driver.
    use_arrow : bool, optional (default: False)
        Whether to use Arrow as the transfer mechanism of the read data
        from GDAL to Python (requires GDAL >= 3.6 and `pyarrow` to be
        installed). When enabled, this provides a further speed-up.
        Defaults to False, but this default can also be globally overridden
        by setting the ``PYOGRIO_USE_ARROW=1`` environment variable.
    on_invalid : str, optional (default: "raise")
        The action to take when an invalid geometry is encountered. Possible
        values:

        - **raise**: an exception will be raised if a WKB input geometry is
          invalid.
        - **warn**: invalid WKB geometries will be returned as ``None`` and a
          warning will be raised.
        - **ignore**: invalid WKB geometries will be returned as ``None``
          without a warning.
        - **fix**: an effort is made to fix invalid input geometries (currently
          just unclosed rings). If this is not possible, they are returned as
          ``None`` without a warning. Requires GEOS >= 3.11 and shapely >= 2.1.

    arrow_to_pandas_kwargs : dict, optional (default: None)
        When `use_arrow` is True, these kwargs will be passed to the `to_pandas`_
        call for the arrow to pandas conversion.
    datetime_as_string : bool, optional (default: False)
        If True, will return datetime columns as detected by GDAL as ISO8601
        strings and ``mixed_offsets_as_utc`` will be ignored.
    mixed_offsets_as_utc: bool, optional (default: True)
        By default, datetime columns are read as the pandas datetime64 dtype.
        This can represent the data as-is in the case that the column contains
        only naive datetimes (without time zone information), only UTC datetimes,
        or if all datetimes in the column have the same time zone offset. Note
        that in time zones with daylight saving time, datetimes will have
        different offsets throughout the year!

        For columns that don't comply with the above, i.e. columns that contain
        mixed offsets, the behavior depends on the value of this parameter:

        - If ``True`` (default), such datetimes are converted to UTC. In the case
          of a mixture of time zone aware and naive datetimes, the naive
          datetimes are assumed to be in UTC already. Datetime columns returned
          will always be pandas datetime64.
        - If ``False``, such datetimes with mixed offsets are returned with
          those offsets preserved. Because pandas datetime64 columns don't
          support mixed time zone offsets, such columns are returned as object
          columns with python datetime values with fixed offsets. If you want
          to roundtrip datetimes without data loss, this is the recommended
          option, but you lose the functionality of a datetime64 column.

        If ``datetime_as_string`` is True, this option is ignored.

    **kwargs
        Additional driver-specific dataset open options passed to OGR. Invalid
        options will trigger a warning.

    Returns
    -------
    GeoDataFrame or DataFrame (if no geometry is present)

    Notes
    -----
    When you have datetime columns with time zone information, it is important to
    note that GDAL only represents time zones as UTC offsets, whilst pandas uses
    IANA time zones (via `pytz` or `zoneinfo`). As a result, even if a column in a
    DataFrame contains datetimes in a single time zone, this will often still result
    in mixed time zone offsets being written for time zones where daylight saving
    time is used (e.g. +01:00 and +02:00 offsets for time zone Europe/Brussels). When
    roundtripping through GDAL, the information about the original time zone is
    lost, only the offsets can be preserved. By default, `pyogrio.read_dataframe()`
    will convert columns with mixed offsets to UTC to return a datetime64 column. If
    you want to preserve the original offsets, you can use `datetime_as_string=True`
    or `mixed_offsets_as_utc=False`.

    .. _OGRSQL:

        https://gdal.org/user/ogr_sql_dialect.html#ogr-sql-dialect

    .. _OGRSQL WHERE:

        https://gdal.org/user/ogr_sql_dialect.html#where

    .. _SQLITE:

        https://gdal.org/user/sql_sqlite_dialect.html#sql-sqlite-dialect

    .. _spatialite:

        https://www.gaia-gis.it/gaia-sins/spatialite-sql-latest.html

    .. _to_pandas:

        https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.to_pandas

    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required to use pyogrio.read_dataframe()")

    import geopandas as gp
    import pandas as pd

    import shapely  # if geopandas is present, shapely is expected to be present

    path_or_buffer = _stringify_path(path_or_buffer)

    if use_arrow is None:
        use_arrow = bool(int(os.environ.get("PYOGRIO_USE_ARROW", "0")))

    read_func = read_arrow if use_arrow else read
    gdal_force_2d = False if use_arrow else force_2d

    # Always read datetimes as string values to preserve (mixed) time zone info
    # correctly. If arrow is not used, it is needed because numpy does not
    # directly support time zones + performance is also a lot better. If arrow
    # is used, needed because datetime columns don't support mixed time zone
    # offsets + e.g. for .fgb files time zone info isn't handled correctly even
    # for unique time zone offsets if datetimes are not read as string.
    result = read_func(
        path_or_buffer,
        layer=layer,
        encoding=encoding,
        columns=columns,
        read_geometry=read_geometry,
        force_2d=gdal_force_2d,
        skip_features=skip_features,
        max_features=max_features,
        where=where,
        bbox=bbox,
        mask=mask,
        fids=fids,
        sql=sql,
        sql_dialect=sql_dialect,
        return_fids=fid_as_index,
        datetime_as_string=True,
        **kwargs,
    )

    if use_arrow:
        import pyarrow as pa

        meta, table = result

        # split_blocks and self_destruct decrease memory usage, but have as side effect
        # that accessing table afterwards causes crash, so del table to avoid.
        kwargs = {"self_destruct": True}
        if PANDAS_GE_30:
            # starting with pyarrow 19.0, pyarrow will correctly handle this themselves,
            # so only use types_mapper as workaround for older versions
            if not PYARROW_GE_19:
                kwargs["types_mapper"] = {
                    pa.string(): pd.StringDtype(na_value=np.nan),
                    pa.large_string(): pd.StringDtype(na_value=np.nan),
                    pa.json_(): pd.StringDtype(na_value=np.nan),
                }.get
            # TODO enable the below block when upstream issue to accept extension types
            # is fixed
            # else:
            #     # for newer pyarrow, still include mapping for json
            #     # GDAL 3.11 started to emit this extension type, but pyarrow does not
            #     # yet support it properly in the conversion to pandas
            #     kwargs["types_mapper"] = {
            #         pa.json_(): pd.StringDtype(na_value=np.nan),
            #     }.get
        if arrow_to_pandas_kwargs is not None:
            kwargs.update(arrow_to_pandas_kwargs)

        try:
            df = table.to_pandas(**kwargs)
        except UnicodeDecodeError as ex:
            # Arrow does not support reading data in a non-UTF-8 encoding
            raise DataSourceError(
                "The file being read is not encoded in UTF-8; please use_arrow=False"
            ) from ex

        del table

        # convert datetime columns that were read as string to datetime
        for dtype, column in zip(meta["dtypes"], meta["fields"], strict=True):
            if (
                dtype is not None
                and dtype.startswith("datetime")
                # With arrow, date columns are returned as datetime.date objects
                and dtype != "datetime64[D]"
            ):
                df[column] = _try_parse_datetime(
                    df[column], datetime_as_string, mixed_offsets_as_utc
                )
        for ogr_subtype, c in zip(meta["ogr_subtypes"], meta["fields"], strict=True):
            if ogr_subtype == "OFSTJSON":
                # When reading .parquet files with arrow, JSON fields are already
                # parsed, so only parse if strings.
                dtype = pd.api.types.infer_dtype(df[c])
                if dtype == "string":
                    try:
                        df[c] = df[c].map(json.loads, na_action="ignore")
                    except Exception:
                        warnings.warn(
                            f"Could not parse column '{c}' as JSON; leaving as string",
                            stacklevel=2,
                        )

        if fid_as_index:
            df = df.set_index(meta["fid_column"])
            df.index.names = ["fid"]

        geometry_name = meta["geometry_name"] or "wkb_geometry"
        if not fid_as_index and len(df.columns) == 0:
            # Index not asked, no geometry column and no attribute columns: return empty
            return pd.DataFrame()
        elif geometry_name in df.columns:
            wkb_values = df.pop(geometry_name)
            if PANDAS_GE_15 and wkb_values.dtype != object:
                if (
                    HAS_PYARROW
                    and isinstance(wkb_values.dtype, pd.ArrowDtype)
                    and isinstance(wkb_values.dtype.pyarrow_dtype, pa.BaseExtensionType)
                ):
                    # handle BaseExtensionType(extension<geoarrow.wkb>)
                    wkb_values = pa.array(wkb_values.array).to_numpy(
                        zero_copy_only=False
                    )
                else:
                    # for example ArrowDtype will otherwise give numpy array with pd.NA
                    wkb_values = wkb_values.to_numpy(na_value=None)
            df["geometry"] = shapely.from_wkb(wkb_values, on_invalid=on_invalid)
            if force_2d:
                df["geometry"] = shapely.force_2d(df["geometry"])
            return gp.GeoDataFrame(df, geometry="geometry", crs=meta["crs"])
        else:
            return df

    meta, index, geometry, field_data = result

    columns = meta["fields"].tolist()
    data = {columns[i]: field_data[i] for i in range(len(columns))}
    if fid_as_index:
        index = pd.Index(index, name="fid")
    else:
        index = None
    df = pd.DataFrame(data, columns=columns, index=index)
    for dtype, c in zip(meta["dtypes"], meta["fields"], strict=True):
        if dtype.startswith("datetime"):
            df[c] = _try_parse_datetime(df[c], datetime_as_string, mixed_offsets_as_utc)
    for ogr_subtype, c in zip(meta["ogr_subtypes"], meta["fields"], strict=True):
        if ogr_subtype == "OFSTJSON":
            dtype = pd.api.types.infer_dtype(df[c])
            if dtype == "string":
                try:
                    df[c] = df[c].map(json.loads, na_action="ignore")
                except Exception:
                    warnings.warn(
                        f"Could not parse column '{c}' as JSON; leaving as string",
                        stacklevel=2,
                    )

    if geometry is None or not read_geometry:
        return df

    geometry = shapely.from_wkb(geometry, on_invalid=on_invalid)

    return gp.GeoDataFrame(df, geometry=geometry, crs=meta["crs"])


def write_dataframe(
    df,
    path,
    layer=None,
    driver=None,
    encoding=None,
    geometry_type=None,
    promote_to_multi=None,
    nan_as_null=True,
    append=False,
    use_arrow=None,
    dataset_metadata=None,
    layer_metadata=None,
    metadata=None,
    dataset_options=None,
    layer_options=None,
    **kwargs,
):
    """Write GeoPandas GeoDataFrame to an OGR file format.

    Parameters
    ----------
    df : GeoDataFrame or DataFrame
        The data to write. For attribute columns of the "object" dtype,
        all values will be converted to strings to be written to the
        output file, except None and np.nan, which will be set to NULL
        in the output file.
    path : str or io.BytesIO
        path to output file on writeable file system or an io.BytesIO object to
        allow writing to memory.  Will raise NotImplementedError if an open file
        handle is passed; use BytesIO instead.
        NOTE: support for writing to memory is limited to specific drivers.
    layer : str, optional (default: None)
        layer name to create.  If writing to memory and layer name is not
        provided, it layer name will be set to a UUID4 value.
    driver : string, optional (default: None)
        The OGR format driver used to write the vector file. By default attempts
        to infer driver from path.  Must be provided to write to memory.
    encoding : str, optional (default: None)
        If present, will be used as the encoding for writing string values to
        the file.  Use with caution, only certain drivers support encodings
        other than UTF-8.
    geometry_type : string, optional (default: None)
        By default, the geometry type of the layer will be inferred from the
        data, after applying the promote_to_multi logic. If the data only contains a
        single geometry type (after applying the logic of promote_to_multi), this type
        is used for the layer. If the data (still) contains mixed geometry types, the
        output layer geometry type will be set to "Unknown".

        This parameter does not modify the geometry, but it will try to force the layer
        type of the output file to this value. Use this parameter with caution because
        using a non-default layer geometry type may result in errors when writing the
        file, may be ignored by the driver, or may result in invalid files. Possible
        values are: "Unknown", "Point", "LineString", "Polygon", "MultiPoint",
        "MultiLineString", "MultiPolygon" or "GeometryCollection".
    promote_to_multi : bool, optional (default: None)
        If True, will convert singular geometry types in the data to their
        corresponding multi geometry type for writing. By default, will convert
        mixed singular and multi geometry types to multi geometry types for drivers
        that do not support mixed singular and multi geometry types. If False, geometry
        types will not be promoted, which may result in errors or invalid files when
        attempting to write mixed singular and multi geometry types to drivers that do
        not support such combinations.
    nan_as_null : bool, default True
        For floating point columns (float32 / float64), whether NaN values are
        written as "null" (missing value). Defaults to True because in pandas
        NaNs are typically used as missing value. Note that when set to False,
        behaviour is format specific: some formats don't support NaNs by
        default (e.g. GeoJSON will skip this property) or might treat them as
        null anyway (e.g. GeoPackage).
    append : bool, optional (default: False)
        If True, the data source specified by path already exists, and the
        driver supports appending to an existing data source, will cause the
        data to be appended to the existing records in the data source.  Not
        supported for writing to in-memory files.
        NOTE: append support is limited to specific drivers and GDAL versions.
    use_arrow : bool, optional (default: False)
        Whether to use Arrow as the transfer mechanism of the data to write
        from Python to GDAL (requires GDAL >= 3.8 and `pyarrow` to be
        installed). When enabled, this provides a further speed-up.
        Defaults to False, but this default can also be globally overridden
        by setting the ``PYOGRIO_USE_ARROW=1`` environment variable.
        Using Arrow does not support writing an object-dtype column with
        mixed types.
    dataset_metadata : dict, optional (default: None)
        Metadata to be stored at the dataset level in the output file; limited
        to drivers that support writing metadata, such as GPKG, and silently
        ignored otherwise. Keys and values must be strings.
    layer_metadata : dict, optional (default: None)
        Metadata to be stored at the layer level in the output file; limited to
        drivers that support writing metadata, such as GPKG, and silently
        ignored otherwise. Keys and values must be strings.
    metadata : dict, optional (default: None)
        alias of layer_metadata
    dataset_options : dict, optional
        Dataset creation options (format specific) passed to OGR. Specify as
        a key-value dictionary.
    layer_options : dict, optional
        Layer creation options (format specific) passed to OGR. Specify as
        a key-value dictionary.
    **kwargs
        Additional driver-specific dataset or layer creation options passed
        to OGR. pyogrio will attempt to automatically pass those keywords
        either as dataset or as layer creation option based on the known
        options for the specific driver. Alternatively, you can use the
        explicit `dataset_options` or `layer_options` keywords to manually
        do this (for example if an option exists as both dataset and layer
        option).

    Notes
    -----
    When you have datetime columns with time zone information, it is important to
    note that GDAL only represents time zones as UTC offsets, whilst pandas uses
    IANA time zones (via `pytz` or `zoneinfo`). As a result, even if a column in a
    DataFrame contains datetimes in a single time zone, this will often still result
    in mixed time zone offsets being written for time zones where daylight saving
    time is used (e.g. +01:00 and +02:00 offsets for time zone Europe/Brussels).

    Object dtype columns containing `datetime` or `pandas.Timestamp` objects will
    also be written as datetime fields, preserving time zone information where possible.

    """
    # TODO: add examples to the docstring (e.g. OGR kwargs)

    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required to use pyogrio.write_dataframe()")

    import pandas as pd
    from geopandas.array import to_wkb

    if not isinstance(df, pd.DataFrame):
        raise ValueError("'df' must be a DataFrame or GeoDataFrame")

    if use_arrow is None:
        use_arrow = bool(int(os.environ.get("PYOGRIO_USE_ARROW", "0")))
    path, driver = _get_write_path_driver(path, driver, append=append)

    geometry_columns = df.columns[df.dtypes == "geometry"]
    if len(geometry_columns) > 1:
        raise ValueError(
            "'df' must have only one geometry column. "
            "Multiple geometry columns are not supported for output using OGR."
        )

    if len(geometry_columns) > 0:
        geometry_column = geometry_columns[0]
        geometry = df[geometry_column]
    else:
        geometry_column = None
        geometry = None

    # Determine geometry_type and/or promote_to_multi
    if geometry_column is not None:
        geometry_types_all = geometry.geom_type

    if geometry_column is not None and (
        geometry_type is None or promote_to_multi is None
    ):
        tmp_geometry_type = "Unknown"
        has_z = False

        # If there is data, infer layer geometry type + promote_to_multi
        if not df.empty:
            # None/Empty geometries sometimes report as Z incorrectly, so ignore them
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", r"GeoSeries\.notna", UserWarning)
                geometry_notna = geometry.notna()
            has_z_arr = geometry[geometry_notna & (~geometry.is_empty)].has_z
            has_z = has_z_arr.any()
            all_z = has_z_arr.all()

            if driver in DRIVERS_NO_MIXED_DIMENSIONS and has_z and not all_z:
                raise DataSourceError(
                    f"Mixed 2D and 3D coordinates are not supported by {driver}"
                )

            geometry_types = pd.Series(geometry_types_all.unique()).dropna().values
            if len(geometry_types) == 1:
                tmp_geometry_type = geometry_types[0]
                if promote_to_multi and tmp_geometry_type in (
                    "Point",
                    "LineString",
                    "Polygon",
                ):
                    tmp_geometry_type = f"Multi{tmp_geometry_type}"
            elif len(geometry_types) == 2:
                # Check if the types are corresponding multi + single types
                if "Polygon" in geometry_types and "MultiPolygon" in geometry_types:
                    multi_type = "MultiPolygon"
                elif (
                    "LineString" in geometry_types
                    and "MultiLineString" in geometry_types
                ):
                    multi_type = "MultiLineString"
                elif "Point" in geometry_types and "MultiPoint" in geometry_types:
                    multi_type = "MultiPoint"
                else:
                    multi_type = None

                # If they are corresponding multi + single types
                if multi_type is not None:
                    if (
                        promote_to_multi is None
                        and driver in DRIVERS_NO_MIXED_SINGLE_MULTI
                    ):
                        promote_to_multi = True
                    if promote_to_multi:
                        tmp_geometry_type = multi_type

        if geometry_type is None:
            geometry_type = tmp_geometry_type
            if has_z and geometry_type != "Unknown":
                geometry_type = f"{geometry_type} Z"

    crs = None
    if geometry_column is not None and geometry.crs:
        # TODO: this may need to be WKT1, due to issues
        # if possible use EPSG codes instead
        epsg = geometry.crs.to_epsg()
        if epsg:
            crs = f"EPSG:{epsg}"
        else:
            crs = geometry.crs.to_wkt("WKT1_GDAL")

    if use_arrow:
        import pandas as pd  # only called when pandas is known to be installed
        import pyarrow as pa

        from pyogrio.raw import write_arrow

        if geometry_column is not None:
            # Convert to multi type
            if promote_to_multi:
                import shapely

                mask_points = geometry_types_all == "Point"
                mask_linestrings = geometry_types_all == "LineString"
                mask_polygons = geometry_types_all == "Polygon"

                if mask_points.any():
                    geometry[mask_points] = shapely.multipoints(
                        np.atleast_2d(geometry[mask_points]), axis=0
                    )

                if mask_linestrings.any():
                    geometry[mask_linestrings] = shapely.multilinestrings(
                        np.atleast_2d(geometry[mask_linestrings]), axis=0
                    )

                if mask_polygons.any():
                    geometry[mask_polygons] = shapely.multipolygons(
                        np.atleast_2d(geometry[mask_polygons]), axis=0
                    )

            geometry = to_wkb(geometry.values)
            df = df.copy(deep=False)
            # convert to plain DataFrame to avoid warning from geopandas about
            # writing non-geometries to the geometry column
            df = pd.DataFrame(df, copy=False)
            df[geometry_column] = geometry

        # Arrow doesn't support datetime columns with mixed time zones, and GDAL only
        # supports time zone offsets. Hence, to avoid data loss, convert columns that
        # can contain datetime values with different offsets to strings.
        # Also pass a list of these columns on to GDAL so it can still treat them as
        # datetime columns when writing the dataset.
        datetime_cols = []
        for name, dtype in df.dtypes.items():
            if dtype == "object":
                # An object column with datetimes can contain multiple offsets.
                if pd.api.types.infer_dtype(df[name]) == "datetime":
                    df[name] = df[name].astype("string")
                    datetime_cols.append(name)

            elif isinstance(dtype, pd.DatetimeTZDtype) and str(dtype.tz) != "UTC":
                # A pd.datetime64 column with a time zone different than UTC can contain
                # data with different offsets because of summer/winter time.
                df[name] = df[name].astype("string")
                datetime_cols.append(name)

        table = pa.Table.from_pandas(df, preserve_index=False)

        # Add metadata to datetime columns so GDAL knows they are datetimes.
        table = _add_column_metadata(
            table,
            column_metadata={
                col: {"GDAL:OGR:type": "DateTime"} for col in datetime_cols
            },
        )

        # Null arrow columns are not supported by GDAL, so convert to string
        for field_index, field in enumerate(table.schema):
            if field.type == pa.null():
                table = table.set_column(
                    field_index,
                    field.with_type(pa.string()),
                    table[field_index].cast(pa.string()),
                )

        if geometry_column is not None:
            # ensure that the geometry column is binary (for all-null geometries,
            # this could be a wrong type)
            geom_field = table.schema.field(geometry_column)
            if not (
                pa.types.is_binary(geom_field.type)
                or pa.types.is_large_binary(geom_field.type)
            ):
                table = table.set_column(
                    table.schema.get_field_index(geometry_column),
                    geom_field.with_type(pa.binary()),
                    table[geometry_column].cast(pa.binary()),
                )

        write_arrow(
            table,
            path,
            layer=layer,
            driver=driver,
            geometry_name=geometry_column,
            geometry_type=geometry_type,
            crs=crs,
            encoding=encoding,
            append=append,
            dataset_metadata=dataset_metadata,
            layer_metadata=layer_metadata,
            metadata=metadata,
            dataset_options=dataset_options,
            layer_options=layer_options,
            **kwargs,
        )
        return

    # If there is geometry data, prepare it to be written
    if geometry_column is not None:
        geometry = to_wkb(geometry.values)
        fields = [c for c in df.columns if not c == geometry_column]
    else:
        fields = list(df.columns)

    # Convert data to numpy arrays for writing
    # TODO: may need to fill in pd.NA, etc
    field_data = []
    field_mask = []
    # dict[str, np.array(int)] special case for dt-tz fields
    gdal_tz_offsets = {}
    for name in fields:
        col = df[name]
        values = None

        if isinstance(col.dtype, pd.DatetimeTZDtype):
            # Deal with datetimes with time zones by passing down time zone separately
            # pass down naive datetime
            naive = col.dt.tz_localize(None)
            values = naive.values
            # compute offset relative to UTC explicitly
            tz_offset = naive - col.dt.tz_convert("UTC").dt.tz_localize(None)
            # Convert to GDAL time zone offset representation.
            # GMT is represented as 100 and offsets are represented by adding /
            # subtracting 1 for every 15 minutes different from GMT.
            # https://gdal.org/development/rfc/rfc56_millisecond_precision.html#core-changes
            # Convert each row offset to a signed multiple of 15m and add to GMT value
            gdal_offset_representation = tz_offset // pd.Timedelta("15m") + 100
            gdal_tz_offsets[name] = gdal_offset_representation.values

        elif col.dtype == "object":
            # Column of Timestamp/datetime objects, split in naive datetime and tz.
            if pd.api.types.infer_dtype(df[name]) == "datetime":
                tz_offset = col.map(lambda x: x.utcoffset(), na_action="ignore")
                gdal_offset_repr = tz_offset // pd.Timedelta("15m") + 100
                gdal_tz_offsets[name] = gdal_offset_repr.values
                naive = col.map(lambda x: x.replace(tzinfo=None), na_action="ignore")
                values = naive.values

        if values is None:
            values = col.values

        if isinstance(values, pd.api.extensions.ExtensionArray):
            from pandas.arrays import BooleanArray, FloatingArray, IntegerArray

            if isinstance(values, IntegerArray | FloatingArray | BooleanArray):
                field_data.append(values._data)
                field_mask.append(values._mask)
            else:
                field_data.append(np.asarray(values))
                field_mask.append(np.asarray(values.isna()))
        else:
            field_data.append(values)
            field_mask.append(None)

    write(
        path,
        layer=layer,
        driver=driver,
        geometry=geometry,
        field_data=field_data,
        field_mask=field_mask,
        fields=fields,
        crs=crs,
        geometry_type=geometry_type,
        encoding=encoding,
        promote_to_multi=promote_to_multi,
        nan_as_null=nan_as_null,
        append=append,
        dataset_metadata=dataset_metadata,
        layer_metadata=layer_metadata,
        metadata=metadata,
        dataset_options=dataset_options,
        layer_options=layer_options,
        gdal_tz_offsets=gdal_tz_offsets,
        **kwargs,
    )


def _add_column_metadata(table, column_metadata: dict = {}):
    """Add or update column-level metadata to an arrow table.

    Parameters
    ----------
    table : pyarrow.Table
        The table to add the column metadata to.
    column_metadata : dict
        A dictionary with column metadata in the form
            {
                "column_1": {"some": "data"},
                "column_2": {"more": "stuff"},
            }

    Returns
    -------
    pyarrow.Table: table with the updated column metadata.
    """
    import pyarrow as pa

    if not column_metadata:
        return table

    # Create updated column fields with new metadata
    fields = []
    for col in table.schema.names:
        if col in column_metadata:
            # Add/update column metadata
            metadata = table.field(col).metadata or {}
            for key, value in column_metadata[col].items():
                metadata[key] = value
            # Update field with updated metadata
            fields.append(table.field(col).with_metadata(metadata))
        else:
            fields.append(table.field(col))

    # Create new schema with the updated field metadata
    schema = pa.schema(fields, metadata=table.schema.metadata)

    # Build new table with updated schema (shouldn't copy data)
    table = table.cast(schema)

    return table
