import warnings
from pyogrio._ogr cimport *
from pyogrio._err cimport *
from pyogrio._err import CPLE_BaseError, NullPointerError
from pyogrio.errors import DataLayerError, GeometryError


# Mapping of OGR integer geometry types to GeoJSON type names.

GEOMETRY_TYPES = {
    wkbUnknown: "Unknown",
    wkbPoint: "Point",
    wkbLineString: "LineString",
    wkbPolygon: "Polygon",
    wkbMultiPoint: "MultiPoint",
    wkbMultiLineString: "MultiLineString",
    wkbMultiPolygon: "MultiPolygon",
    wkbGeometryCollection: "GeometryCollection",
    wkbNone: None,
    wkbLinearRing: "LinearRing",
    # WARNING: Measured types are not supported in GEOS and downstream uses
    # these are stripped automatically to their corresponding 2D / 3D types
    wkbPointM: "PointM",
    wkbLineStringM: "Measured LineString",
    wkbPolygonM: "Measured Polygon",
    wkbMultiPointM: "Measured MultiPoint",
    wkbMultiLineStringM: "Measured MultiLineString",
    wkbMultiPolygonM: "Measured MultiPolygon",
    wkbGeometryCollectionM: "Measured GeometryCollection",
    wkbPointZM: "Measured 3D Point",
    wkbLineStringZM: "Measured 3D LineString",
    wkbPolygonZM: "Measured 3D Polygon",
    wkbMultiPointZM: "Measured 3D MultiPoint",
    wkbMultiLineStringZM: "Measured 3D MultiLineString",
    wkbMultiPolygonZM: "Measured 3D MultiPolygon",
    wkbGeometryCollectionZM: "Measured 3D GeometryCollection",
    wkbPoint25D: "Point Z",
    wkbLineString25D: "LineString Z",
    wkbPolygon25D: "Polygon Z",
    wkbMultiPoint25D: "MultiPoint Z",
    wkbMultiLineString25D: "MultiLineString Z",
    wkbMultiPolygon25D: "MultiPolygon Z",
    wkbGeometryCollection25D: "GeometryCollection Z",
}

GEOMETRY_TYPE_CODES = {v: k for k, v in GEOMETRY_TYPES.items()}

# add additional aliases from 2.5D format
GEOMETRY_TYPE_CODES.update({
    "2.5D Point": wkbPoint25D,
    "2.5D LineString": wkbLineString25D,
    "2.5D Polygon": wkbPolygon25D,
    "2.5D MultiPoint": wkbMultiPoint25D,
    "2.5D MultiLineString": wkbMultiLineString25D,
    "2.5D MultiPolygon": wkbMultiPolygon25D,
    "2.5D GeometryCollection": wkbGeometryCollection25D
})

# 2.5D also represented using negative numbers not enumerated above
GEOMETRY_TYPES.update({
    -2147483647: "Point Z",
    -2147483646: "LineString Z",
    -2147483645: "Polygon Z",
    -2147483644: "MultiPoint Z",
    -2147483643: "MultiLineString Z",
    -2147483642: "MultiPolygon Z",
    -2147483641: "GeometryCollection Z",
})


cdef str get_geometry_type(void *ogr_layer):
    """Get geometry type for layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer

    Returns
    -------
    str
        geometry type
    """
    cdef void *ogr_featuredef = NULL
    cdef OGRwkbGeometryType ogr_type

    try:
        ogr_featuredef = check_pointer(OGR_L_GetLayerDefn(ogr_layer))
    except NullPointerError:
        raise DataLayerError("Could not get layer definition")

    except CPLE_BaseError as exc:
        raise DataLayerError(str(exc))

    ogr_type = OGR_FD_GetGeomType(ogr_featuredef)

    if ogr_type not in GEOMETRY_TYPES:
        raise GeometryError(f"Geometry type is not supported: {ogr_type}")

    if OGR_GT_HasM(ogr_type):
        original_type = GEOMETRY_TYPES[ogr_type]

        # Downgrade the type to 2D / 3D
        ogr_type = OGR_GT_SetModifier(ogr_type, OGR_GT_HasZ(ogr_type), 0)

        # TODO: review; this might be annoying...
        warnings.warn(
            "Measured (M) geometry types are not supported. "
            f"Original type '{original_type}' "
            f"is converted to '{GEOMETRY_TYPES[ogr_type]}'")

    return GEOMETRY_TYPES[ogr_type]


cdef OGRwkbGeometryType get_geometry_type_code(str geometry_type) except *:
    """Get geometry type code for string geometry type.

    Parameters
    ----------
    geometry_type : str

    Returns
    -------
    int
        geometry type code
    """
    if geometry_type not in GEOMETRY_TYPE_CODES:
        raise GeometryError(f"Geometry type is not supported: {geometry_type}")

    return GEOMETRY_TYPE_CODES[geometry_type]
