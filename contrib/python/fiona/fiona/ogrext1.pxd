# Copyright (c) 2007, Sean C. Gillies
# All rights reserved.
# See ../LICENSE.txt

from libc.stdio cimport FILE


cdef extern from "gdal.h":
    ctypedef void * GDALDriverH
    ctypedef void * GDALMajorObjectH

    const char* GDALGetMetadataItem(GDALMajorObjectH obj, const char *pszName, const char *pszDomain)
    char * GDALVersionInfo (char *pszRequest)


cdef extern from "gdal_version.h":
    int    GDAL_COMPUTE_VERSION(int maj, int min, int rev)


cdef extern from "cpl_conv.h":
    void *  CPLMalloc (size_t)
    void    CPLFree (void *ptr)
    void    CPLSetThreadLocalConfigOption (char *key, char *val)
    void    CPLSetConfigOption (char *key, char *val)
    const char *CPLGetConfigOption (char *, char *)
    int CPLCheckForFile(char *, char **)


cdef extern from "cpl_string.h":
    char ** CSLAddNameValue (char **list, char *name, char *value)
    char ** CSLSetNameValue (char **list, char *name, char *value)
    void    CSLDestroy (char **list)
    char ** CSLAddString(char **list, const char *string)
    int CSLCount(char **papszStrList)


cdef extern from "sys/stat.h" nogil:
    struct stat:
        int st_mode


cdef extern from "cpl_vsi.h" nogil:

    ctypedef int vsi_l_offset
    ctypedef FILE VSILFILE
    ctypedef stat VSIStatBufL

    unsigned char *VSIGetMemFileBuffer(const char *path,
                                       vsi_l_offset *data_len,
                                       int take_ownership)
    VSILFILE *VSIFileFromMemBuffer(const char *path, void *data,
                                   vsi_l_offset data_len, int take_ownership)
    VSILFILE* VSIFOpenL(const char *path, const char *mode)
    int VSIFCloseL(VSILFILE *fp)
    int VSIUnlink(const char *path)
    int VSIMkdir(const char *path, long mode)
    int VSIRmdir(const char *path)
    int VSIFFlushL(VSILFILE *fp)
    size_t VSIFReadL(void *buffer, size_t nSize, size_t nCount, VSILFILE *fp)
    char** VSIReadDir(const char* pszPath)
    int VSIFSeekL(VSILFILE *fp, vsi_l_offset nOffset, int nWhence)
    vsi_l_offset VSIFTellL(VSILFILE *fp)
    int VSIFTruncateL(VSILFILE *fp, vsi_l_offset nNewSize)
    size_t VSIFWriteL(void *buffer, size_t nSize, size_t nCount, VSILFILE *fp)
    int VSIStatL(const char *pszFilename, VSIStatBufL *psStatBuf)
    int VSI_ISDIR(int mode)


ctypedef int OGRErr
ctypedef struct OGREnvelope:
    double MinX
    double MaxX
    double MinY
    double MaxY


cdef extern from "ogr_core.h":
    ctypedef enum OGRwkbGeometryType:
        wkbUnknown
        wkbPoint
        wkbLineString
        wkbPolygon
        wkbMultiPoint
        wkbMultiLineString
        wkbMultiPolygon
        wkbGeometryCollection
        wkbCircularString
        wkbCompoundCurve
        wkbCurvePolygon
        wkbMultiCurve
        wkbMultiSurface
        wkbCurve
        wkbSurface
        wkbPolyhedralSurface
        wkbTIN
        wkbTriangle
        wkbNone
        wkbLinearRing
        wkbCircularStringZ
        wkbCompoundCurveZ
        wkbCurvePolygonZ
        wkbMultiCurveZ
        wkbMultiSurfaceZ
        wkbCurveZ
        wkbSurfaceZ
        wkbPolyhedralSurfaceZ
        wkbTINZ
        wkbTriangleZ
        wkbPointM
        wkbLineStringM
        wkbPolygonM
        wkbMultiPointM
        wkbMultiLineStringM
        wkbMultiPolygonM
        wkbGeometryCollectionM
        wkbCircularStringM
        wkbCompoundCurveM
        wkbCurvePolygonM
        wkbMultiCurveM
        wkbMultiSurfaceM
        wkbCurveM
        wkbSurfaceM
        wkbPolyhedralSurfaceM
        wkbTINM
        wkbTriangleM
        wkbPointZM
        wkbLineStringZM
        wkbPolygonZM
        wkbMultiPointZM
        wkbMultiLineStringZM
        wkbMultiPolygonZM
        wkbGeometryCollectionZM
        wkbCircularStringZM
        wkbCompoundCurveZM
        wkbCurvePolygonZM
        wkbMultiCurveZM
        wkbMultiSurfaceZM
        wkbCurveZM
        wkbSurfaceZM
        wkbPolyhedralSurfaceZM
        wkbTINZM
        wkbTriangleZM
        wkbPoint25D
        wkbLineString25D
        wkbPolygon25D
        wkbMultiPoint25D
        wkbMultiLineString25D
        wkbMultiPolygon25D
        wkbGeometryCollection25D

    ctypedef enum OGRFieldType:
        OFTInteger
        OFTIntegerList
        OFTReal
        OFTRealList
        OFTString
        OFTStringList
        OFTWideString
        OFTWideStringList
        OFTBinary
        OFTDate
        OFTTime
        OFTDateTime
        OFTMaxType

    char *  OGRGeometryTypeToName(int)

    char * ODsCCreateLayer = "CreateLayer"
    char * ODsCDeleteLayer = "DeleteLayer"

cdef extern from "ogr_srs_api.h":

    ctypedef void * OGRSpatialReferenceH

    void    OSRCleanup ()
    OGRSpatialReferenceH  OSRClone (OGRSpatialReferenceH srs)
    int     OSRFixup (OGRSpatialReferenceH srs)
    int     OSRExportToProj4 (OGRSpatialReferenceH srs, char **params)
    int     OSRExportToWkt (OGRSpatialReferenceH srs, char **params)
    int     OSRImportFromEPSG (OGRSpatialReferenceH, int code)
    int     OSRImportFromProj4 (OGRSpatialReferenceH srs, const char *proj)
    int     OSRSetFromUserInput (OGRSpatialReferenceH srs, const char *input)
    int     OSRAutoIdentifyEPSG (OGRSpatialReferenceH srs)
    const char * OSRGetAuthorityName (OGRSpatialReferenceH srs, const char *key)
    const char * OSRGetAuthorityCode (OGRSpatialReferenceH srs, const char *key)
    OGRSpatialReferenceH  OSRNewSpatialReference (char *wkt)
    void    OSRRelease (OGRSpatialReferenceH srs)
    void *  OCTNewCoordinateTransformation (OGRSpatialReferenceH source, OGRSpatialReferenceH dest)
    void    OCTDestroyCoordinateTransformation (void *source)
    int     OCTTransform (void *ct, int nCount, double *x, double *y, double *z)

cdef extern from "ogr_api.h":
    const char * OGR_Dr_GetName (void *driver)
    void *  OGR_Dr_CreateDataSource (void *driver, const char *path, char **options)
    int     OGR_Dr_DeleteDataSource (void *driver, char *)
    void *  OGR_Dr_Open (void *driver, const char *path, int bupdate)
    int     OGR_Dr_TestCapability (void *driver, const char *)
    int     OGR_DS_DeleteLayer (void *datasource, int n)
    void *  OGR_DS_CreateLayer (void *datasource, char *name, void *crs, int geomType, char **options)
    void *  OGR_DS_ExecuteSQL (void *datasource, char *name, void *filter, char *dialext)
    void    OGR_DS_Destroy (void *datasource)
    void *  OGR_DS_GetDriver (void *layer_defn)
    void *  OGR_DS_GetLayerByName (void *datasource, char *name)
    int     OGR_DS_GetLayerCount (void *datasource)
    void *  OGR_DS_GetLayer (void *datasource, int n)
    void    OGR_DS_ReleaseResultSet (void *datasource, void *results)
    int     OGR_DS_SyncToDisk (void *datasource)
    int     OGR_DS_TestCapability(void *datasource, char *capability)
    void *  OGR_F_Create (void *featuredefn)
    void    OGR_F_Destroy (void *feature)
    long    OGR_F_GetFID (void *feature)
    int     OGR_F_IsFieldSet (void *feature, int n)
    int     OGR_F_GetFieldAsDateTime (void *feature, int n, int *y, int *m, int *d, int *h, int *m, int *s, int *z)
    double  OGR_F_GetFieldAsDouble (void *feature, int n)
    int     OGR_F_GetFieldAsInteger (void *feature, int n)
    char *  OGR_F_GetFieldAsString (void *feature, int n)
    unsigned char * OGR_F_GetFieldAsBinary(void *feature, int n, int *s)
    int     OGR_F_GetFieldCount (void *feature)
    void *  OGR_F_GetFieldDefnRef (void *feature, int n)
    int     OGR_F_GetFieldIndex (void *feature, char *name)
    void *  OGR_F_GetGeometryRef (void *feature)
    void *  OGR_F_StealGeometry (void *feature)
    void    OGR_F_SetFieldDateTime (void *feature, int n, int y, int m, int d, int hh, int mm, int ss, int tz)
    void    OGR_F_SetFieldDouble (void *feature, int n, double value)
    void    OGR_F_SetFieldInteger (void *feature, int n, int value)
    void    OGR_F_SetFieldString (void *feature, int n, char *value)
    void    OGR_F_SetFieldBinary (void *feature, int n, int l, unsigned char *value)
    int     OGR_F_SetGeometryDirectly (void *feature, void *geometry)
    void *  OGR_FD_Create (char *name)
    int     OGR_FD_GetFieldCount (void *featuredefn)
    void *  OGR_FD_GetFieldDefn (void *featuredefn, int n)
    int     OGR_FD_GetGeomType (void *featuredefn)
    char *  OGR_FD_GetName (void *featuredefn)
    void *  OGR_Fld_Create (char *name, OGRFieldType fieldtype)
    void    OGR_Fld_Destroy (void *fielddefn)
    char *  OGR_Fld_GetNameRef (void *fielddefn)
    int     OGR_Fld_GetPrecision (void *fielddefn)
    int     OGR_Fld_GetType (void *fielddefn)
    int     OGR_Fld_GetWidth (void *fielddefn)
    void    OGR_Fld_Set (void *fielddefn, char *name, int fieldtype, int width, int precision, int justification)
    void    OGR_Fld_SetPrecision (void *fielddefn, int n)
    void    OGR_Fld_SetWidth (void *fielddefn, int n)
    OGRErr  OGR_G_AddGeometryDirectly (void *geometry, void *part)
    void    OGR_G_AddPoint (void *geometry, double x, double y, double z)
    void    OGR_G_AddPoint_2D (void *geometry, double x, double y)
    void    OGR_G_CloseRings (void *geometry)
    void *  OGR_G_CreateGeometry (int wkbtypecode)
    void    OGR_G_DestroyGeometry (void *geometry)
    unsigned char *  OGR_G_ExportToJson (void *geometry)
    void    OGR_G_ExportToWkb (void *geometry, int endianness, char *buffer)
    int     OGR_G_GetCoordinateDimension (void *geometry)
    int     OGR_G_GetGeometryCount (void *geometry)
    unsigned char *  OGR_G_GetGeometryName (void *geometry)
    int     OGR_G_GetGeometryType (void *geometry)
    void *  OGR_G_GetGeometryRef (void *geometry, int n)
    int     OGR_G_GetPointCount (void *geometry)
    double  OGR_G_GetX (void *geometry, int n)
    double  OGR_G_GetY (void *geometry, int n)
    double  OGR_G_GetZ (void *geometry, int n)
    OGRErr  OGR_G_ImportFromWkb (void *geometry, unsigned char *bytes, int nbytes)
    int     OGR_G_WkbSize (void *geometry)
    void *  OGR_G_ForceToMultiPolygon (void *geometry)
    void *  OGR_G_ForceToPolygon (void *geometry)
    void *  OGR_G_Clone(void *geometry)
    OGRErr  OGR_L_CreateFeature (void *layer, void *feature)
    OGRErr  OGR_L_CreateField (void *layer, void *fielddefn, int flexible)
    OGRErr  OGR_L_GetExtent (void *layer, void *extent, int force)
    void *  OGR_L_GetFeature (void *layer, int n)
    int     OGR_L_GetFeatureCount (void *layer, int m)
    void *  OGR_L_GetLayerDefn (void *layer)
    char *  OGR_L_GetName (void *layer)
    void *  OGR_L_GetNextFeature (void *layer)
    void *  OGR_L_GetSpatialFilter (void *layer)
    void *  OGR_L_GetSpatialRef (void *layer)
    void    OGR_L_ResetReading (void *layer)
    void    OGR_L_SetSpatialFilter (void *layer, void *geometry)
    void    OGR_L_SetSpatialFilterRect (
                void *layer, double minx, double miny, double maxx, double maxy
                )
    int     OGR_L_TestCapability (void *layer, char *name)
    void *  OGRGetDriverByName (char *)
    void *  OGROpen (char *path, int mode, void *x)
    void *  OGROpenShared (char *path, int mode, void *x)
    int     OGRReleaseDataSource (void *datasource)
    OGRErr  OGR_L_SetIgnoredFields (void *layer, const char **papszFields)
    OGRErr  OGR_L_SetAttributeFilter(void *layer, const char*)
    OGRErr  OGR_L_SetNextByIndex (void *layer, long nIndex)
