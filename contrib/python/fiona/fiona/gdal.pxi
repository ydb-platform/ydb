# GDAL API definitions.

from libc.stdio cimport FILE

cdef extern from "gdal_version.h":
    int    GDAL_COMPUTE_VERSION(int maj, int min, int rev)


cdef extern from "cpl_conv.h":
    void *  CPLMalloc (size_t)
    void    CPLFree (void *ptr)
    void    CPLSetThreadLocalConfigOption (char *key, char *val)
    const char *CPLGetConfigOption (char *, char *)
    void CPLSetConfigOption(const char* key, const char* val)
    int CPLCheckForFile(char *, char **)
    const char *CPLFindFile(const char *pszClass, const char *pszBasename)


cdef extern from "cpl_port.h":
    ctypedef char **CSLConstList


cdef extern from "cpl_string.h":
    char ** CSLAddNameValue(char **list, const char *name, const char *value)
    char ** CSLSetNameValue(char **list, const char *name, const char *value)
    void CSLDestroy(char **list)
    char ** CSLAddString(char **list, const char *string)
    int CSLCount(CSLConstList papszStrList)
    char **CSLDuplicate(CSLConstList papszStrList)
    int CSLFindName(CSLConstList papszStrList, const char *pszName)
    int CSLFindString(CSLConstList papszStrList, const char *pszString)
    int CSLFetchBoolean(CSLConstList papszStrList, const char *pszName, int default)
    const char *CSLFetchNameValue(CSLConstList papszStrList, const char *pszName)
    char **CSLMerge(char **first, CSLConstList second)


cdef extern from "cpl_error.h" nogil:
    ctypedef enum CPLErr:
        CE_None
        CE_Debug
        CE_Warning
        CE_Failure
        CE_Fatal

    ctypedef int CPLErrorNum
    ctypedef void (*CPLErrorHandler)(CPLErr, int, const char*)

    void CPLError(CPLErr eErrClass, CPLErrorNum err_no, const char *template, ...)
    void CPLErrorReset()
    int CPLGetLastErrorNo()
    const char* CPLGetLastErrorMsg()
    CPLErr CPLGetLastErrorType()
    void CPLPushErrorHandler(CPLErrorHandler handler)
    void CPLPushErrorHandlerEx(CPLErrorHandler handler, void *userdata)
    void CPLPopErrorHandler()
    void CPLQuietErrorHandler(CPLErr eErrClass, CPLErrorNum nError, const char *pszErrorMsg)


cdef extern from "cpl_vsi.h" nogil:
    ctypedef unsigned long long vsi_l_offset
    ctypedef FILE VSILFILE
    ctypedef struct VSIStatBufL:
        long st_size
        long st_mode
        int st_mtime
    ctypedef enum VSIRangeStatus:
        VSI_RANGE_STATUS_UNKNOWN,
        VSI_RANGE_STATUS_DATA,
        VSI_RANGE_STATUS_HOLE,

    # GDAL Plugin System (GDAL 3.0+)
    # Filesystem functions
    ctypedef int (*VSIFilesystemPluginStatCallback)(void*, const char*, VSIStatBufL*, int)  # Optional
    ctypedef int (*VSIFilesystemPluginUnlinkCallback)(void*, const char*)  # Optional
    ctypedef int (*VSIFilesystemPluginRenameCallback)(void*, const char*, const char*)  # Optional
    ctypedef int (*VSIFilesystemPluginMkdirCallback)(void*, const char*, long)  # Optional
    ctypedef int (*VSIFilesystemPluginRmdirCallback)(void*, const char*)  # Optional
    ctypedef char** (*VSIFilesystemPluginReadDirCallback)(void*, const char*, int)  # Optional
    ctypedef char** (*VSIFilesystemPluginSiblingFilesCallback)(void*, const char*)  # Optional (GDAL 3.2+)
    ctypedef void* (*VSIFilesystemPluginOpenCallback)(void*, const char*, const char*)
    # File functions
    ctypedef vsi_l_offset (*VSIFilesystemPluginTellCallback)(void*)
    ctypedef int (*VSIFilesystemPluginSeekCallback)(void*, vsi_l_offset, int)
    ctypedef size_t (*VSIFilesystemPluginReadCallback)(void*, void*, size_t, size_t)
    ctypedef int (*VSIFilesystemPluginReadMultiRangeCallback)(void*, int, void**, const vsi_l_offset*, const size_t*)  # Optional
    ctypedef VSIRangeStatus (*VSIFilesystemPluginGetRangeStatusCallback)(void*, vsi_l_offset, vsi_l_offset)  # Optional
    ctypedef int (*VSIFilesystemPluginEofCallback)(void*)  # Mandatory?
    ctypedef size_t (*VSIFilesystemPluginWriteCallback)(void*, const void*, size_t, size_t)
    ctypedef int (*VSIFilesystemPluginFlushCallback)(void*)  # Optional
    ctypedef int (*VSIFilesystemPluginTruncateCallback)(void*, vsi_l_offset)
    ctypedef int (*VSIFilesystemPluginCloseCallback)(void*)  # Optional
    # Plugin function container struct
    ctypedef struct VSIFilesystemPluginCallbacksStruct:
        void *pUserData
        VSIFilesystemPluginStatCallback stat
        VSIFilesystemPluginUnlinkCallback unlink
        VSIFilesystemPluginRenameCallback rename
        VSIFilesystemPluginMkdirCallback mkdir
        VSIFilesystemPluginRmdirCallback rmdir
        VSIFilesystemPluginReadDirCallback read_dir
        VSIFilesystemPluginOpenCallback open
        VSIFilesystemPluginTellCallback tell
        VSIFilesystemPluginSeekCallback seek
        VSIFilesystemPluginReadCallback read
        VSIFilesystemPluginReadMultiRangeCallback read_multi_range
        VSIFilesystemPluginGetRangeStatusCallback get_range_status
        VSIFilesystemPluginEofCallback eof
        VSIFilesystemPluginWriteCallback write
        VSIFilesystemPluginFlushCallback flush
        VSIFilesystemPluginTruncateCallback truncate
        VSIFilesystemPluginCloseCallback close
        size_t nBufferSize
        size_t nCacheSize
        VSIFilesystemPluginSiblingFilesCallback sibling_files

    int VSIInstallPluginHandler(const char*, const VSIFilesystemPluginCallbacksStruct*)
    VSIFilesystemPluginCallbacksStruct* VSIAllocFilesystemPluginCallbacksStruct()
    void VSIFreeFilesystemPluginCallbacksStruct(VSIFilesystemPluginCallbacksStruct*)
    char** VSIGetFileSystemsPrefixes()

    unsigned char *VSIGetMemFileBuffer(const char *path,
                                       vsi_l_offset *data_len,
                                       int take_ownership)
    VSILFILE *VSIFileFromMemBuffer(const char *path, void *data,
                                   vsi_l_offset data_len, int take_ownership)
    VSILFILE* VSIFOpenL(const char *path, const char *mode)
    int VSIFCloseL(VSILFILE *fp)
    int VSIUnlink(const char *path)
    int VSIMkdir(const char *path, long mode)
    char** VSIReadDir(const char *path)
    int VSIRmdir(const char *path)
    int VSIRmdirRecursive(const char *path)
    int VSIFFlushL(VSILFILE *fp)
    size_t VSIFReadL(void *buffer, size_t nSize, size_t nCount, VSILFILE *fp)
    int VSIFSeekL(VSILFILE *fp, vsi_l_offset nOffset, int nWhence)
    vsi_l_offset VSIFTellL(VSILFILE *fp)
    int VSIFTruncateL(VSILFILE *fp, vsi_l_offset nNewSize)
    size_t VSIFWriteL(void *buffer, size_t nSize, size_t nCount, VSILFILE *fp)
    int VSIStatL(const char *pszFilename, VSIStatBufL *psStatBuf)
    int VSIMkdir(const char *path, long mode)
    int VSIRmdir(const char *path)
    int VSIStatL(const char *pszFilename, VSIStatBufL *psStatBuf)
    int VSI_ISDIR(int mode)


IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 9):
    cdef extern from "cpl_vsi.h" nogil:
        int VSIRemovePluginHandler(const char*)


cdef extern from "ogr_core.h" nogil:
    ctypedef int OGRErr
    char *OGRGeometryTypeToName(int type)

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
        OFTInteger64
        OFTInteger64List
        OFTMaxType

    ctypedef int OGRFieldSubType
    cdef int OFSTNone = 0
    cdef int OFSTBoolean = 1
    cdef int OFSTInt16 = 2
    cdef int OFSTFloat32 = 3
    cdef int OFSTJSON = 4
    cdef int OFSTUUID = 5
    cdef int OFSTMaxSubType = 5

    ctypedef struct OGREnvelope:
        double MinX
        double MaxX
        double MinY
        double MaxY

    char *  OGRGeometryTypeToName(int)
    char * ODsCCreateLayer = "CreateLayer"
    char * ODsCDeleteLayer = "DeleteLayer"
    char * ODsCTransactions = "Transactions"


cdef extern from "ogr_srs_api.h" nogil:
    ctypedef void * OGRCoordinateTransformationH
    ctypedef void * OGRSpatialReferenceH

    OGRCoordinateTransformationH OCTNewCoordinateTransformation(
                                        OGRSpatialReferenceH source,
                                        OGRSpatialReferenceH dest)
    void OCTDestroyCoordinateTransformation(
        OGRCoordinateTransformationH source)
    int OCTTransform(OGRCoordinateTransformationH ct, int nCount, double *x,
                     double *y, double *z)
    int OSRAutoIdentifyEPSG(OGRSpatialReferenceH srs)
    void OSRCleanup()
    OGRSpatialReferenceH OSRClone(OGRSpatialReferenceH srs)
    int OSRExportToProj4(OGRSpatialReferenceH srs, char **params)
    int OSRExportToWkt(OGRSpatialReferenceH srs, char **params)
    const char *OSRGetAuthorityName(OGRSpatialReferenceH srs, const char *key)
    const char *OSRGetAuthorityCode(OGRSpatialReferenceH srs, const char *key)
    int OSRImportFromEPSG(OGRSpatialReferenceH srs, int code)
    int OSRImportFromProj4(OGRSpatialReferenceH srs, const char *proj)
    int OSRImportFromWkt(OGRSpatialReferenceH srs, char **wkt)
    int OSRIsGeographic(OGRSpatialReferenceH srs)
    int OSRIsProjected(OGRSpatialReferenceH srs)
    int OSRIsSame(OGRSpatialReferenceH srs1, OGRSpatialReferenceH srs2)
    OGRSpatialReferenceH OSRNewSpatialReference(const char *wkt)
    void OSRRelease(OGRSpatialReferenceH srs)
    int OSRSetFromUserInput(OGRSpatialReferenceH srs, const char *input)
    double OSRGetLinearUnits(OGRSpatialReferenceH srs, char **ppszName)
    double OSRGetAngularUnits(OGRSpatialReferenceH srs, char **ppszName)
    int OSREPSGTreatsAsLatLong(OGRSpatialReferenceH srs)
    int OSREPSGTreatsAsNorthingEasting(OGRSpatialReferenceH srs)
    OGRSpatialReferenceH *OSRFindMatches(OGRSpatialReferenceH srs, char **options, int *entries, int **matchConfidence)
    void OSRFreeSRSArray(OGRSpatialReferenceH *srs)
    ctypedef enum OSRAxisMappingStrategy:
        OAMS_TRADITIONAL_GIS_ORDER

    const char* OSRGetName(OGRSpatialReferenceH hSRS)
    void OSRSetAxisMappingStrategy(OGRSpatialReferenceH hSRS, OSRAxisMappingStrategy)
    void OSRSetPROJSearchPaths(const char *const *papszPaths)
    char ** OSRGetPROJSearchPaths()
    OGRErr OSRExportToWktEx(OGRSpatialReferenceH, char ** ppszResult,
                            const char* const* papszOptions)
    OGRErr OSRExportToPROJJSON(OGRSpatialReferenceH hSRS,
                                char ** ppszReturn,
                                const char* const* papszOptions)
    void OSRGetPROJVersion	(int *pnMajor, int *pnMinor, int *pnPatch)

cdef extern from "gdal.h" nogil:

    ctypedef void * GDALMajorObjectH
    ctypedef void * GDALDatasetH
    ctypedef void * GDALRasterBandH
    ctypedef void * GDALDriverH
    ctypedef void * GDALColorTableH
    ctypedef void * GDALRasterAttributeTableH
    ctypedef void * GDALAsyncReaderH

    ctypedef long long GSpacing
    ctypedef unsigned long long GIntBig

    ctypedef enum GDALDataType:
        GDT_Unknown
        GDT_Byte
        GDT_UInt16
        GDT_Int16
        GDT_UInt32
        GDT_Int32
        GDT_Float32
        GDT_Float64
        GDT_CInt16
        GDT_CInt32
        GDT_CFloat32
        GDT_CFloat64
        GDT_TypeCount

    ctypedef enum GDALAccess:
        GA_ReadOnly
        GA_Update

    ctypedef enum GDALRWFlag:
        GF_Read
        GF_Write

    ctypedef enum GDALRIOResampleAlg:
        GRIORA_NearestNeighbour
        GRIORA_Bilinear
        GRIORA_Cubic,
        GRIORA_CubicSpline
        GRIORA_Lanczos
        GRIORA_Average
        GRIORA_Mode
        GRIORA_Gauss

    ctypedef enum GDALColorInterp:
        GCI_Undefined
        GCI_GrayIndex
        GCI_PaletteIndex
        GCI_RedBand
        GCI_GreenBand
        GCI_BlueBand
        GCI_AlphaBand
        GCI_HueBand
        GCI_SaturationBand
        GCI_LightnessBand
        GCI_CyanBand
        GCI_YCbCr_YBand
        GCI_YCbCr_CbBand
        GCI_YCbCr_CrBand
        GCI_Max

    ctypedef struct GDALColorEntry:
        short c1
        short c2
        short c3
        short c4

    ctypedef struct GDAL_GCP:
        char *pszId
        char *pszInfo
        double dfGCPPixel
        double dfGCPLine
        double dfGCPX
        double dfGCPY
        double dfGCPZ

    void GDALAllRegister()
    void GDALDestroyDriverManager()
    int GDALGetDriverCount()
    GDALDriverH GDALGetDriver(int i)
    const char *GDALGetDriverShortName(GDALDriverH driver)
    const char *GDALGetDriverLongName(GDALDriverH driver)
    const char* GDALGetDescription(GDALMajorObjectH obj)
    void GDALSetDescription(GDALMajorObjectH obj, const char *text)
    GDALDriverH GDALGetDriverByName(const char *name)
    GDALDatasetH GDALOpen(const char *filename, GDALAccess access) # except -1
    GDALDatasetH GDALOpenShared(const char *filename, GDALAccess access) # except -1
    void GDALFlushCache(GDALDatasetH hds)
    void GDALClose(GDALDatasetH hds)
    GDALDriverH GDALGetDatasetDriver(GDALDatasetH hds)
    int GDALGetGeoTransform(GDALDatasetH hds, double *transform)
    const char *GDALGetProjectionRef(GDALDatasetH hds)
    int GDALGetRasterXSize(GDALDatasetH hds)
    int GDALGetRasterYSize(GDALDatasetH hds)
    int GDALGetRasterCount(GDALDatasetH hds)
    GDALRasterBandH GDALGetRasterBand(GDALDatasetH hds, int num)
    GDALRasterBandH GDALGetOverview(GDALRasterBandH hband, int num)
    int GDALGetRasterBandXSize(GDALRasterBandH hband)
    int GDALGetRasterBandYSize(GDALRasterBandH hband)
    const char *GDALGetRasterUnitType(GDALRasterBandH hband)
    CPLErr GDALSetRasterUnitType(GDALRasterBandH hband, const char *val)
    int GDALSetGeoTransform(GDALDatasetH hds, double *transform)
    int GDALSetProjection(GDALDatasetH hds, const char *wkt)
    void GDALGetBlockSize(GDALRasterBandH , int *xsize, int *ysize)
    int GDALGetRasterDataType(GDALRasterBandH band)
    double GDALGetRasterNoDataValue(GDALRasterBandH band, int *success)
    int GDALSetRasterNoDataValue(GDALRasterBandH band, double value)
    int GDALDatasetRasterIO(GDALRasterBandH band, int, int xoff, int yoff,
                            int xsize, int ysize, void *buffer, int width,
                            int height, int, int count, int *bmap, int poff,
                            int loff, int boff)
    int GDALRasterIO(GDALRasterBandH band, int, int xoff, int yoff, int xsize,
                     int ysize, void *buffer, int width, int height, int,
                     int poff, int loff)
    int GDALFillRaster(GDALRasterBandH band, double rvalue, double ivalue)
    GDALDatasetH GDALCreate(GDALDriverH driver, const char *path, int width,
                            int height, int nbands, GDALDataType dtype,
                            const char **options)
    GDALDatasetH GDALCreateCopy(GDALDriverH driver, const char *path,
                                GDALDatasetH hds, int strict, char **options,
                                void *progress_func, void *progress_data)
    char** GDALGetMetadata(GDALMajorObjectH obj, const char *pszDomain)
    int GDALSetMetadata(GDALMajorObjectH obj, char **papszMD,
                        const char *pszDomain)
    const char* GDALGetMetadataItem(GDALMajorObjectH obj, const char *pszName, const char *pszDomain)
    int GDALSetMetadataItem(GDALMajorObjectH obj, const char *pszName,
                            const char *pszValue, const char *pszDomain)
    const GDALColorEntry *GDALGetColorEntry(GDALColorTableH table, int)
    void GDALSetColorEntry(GDALColorTableH table, int i,
                           const GDALColorEntry *poEntry)
    int GDALSetRasterColorTable(GDALRasterBandH band, GDALColorTableH table)
    GDALColorTableH GDALGetRasterColorTable(GDALRasterBandH band)
    GDALColorTableH GDALCreateColorTable(int)
    void GDALDestroyColorTable(GDALColorTableH table)
    int GDALGetColorEntryCount(GDALColorTableH table)
    int GDALGetRasterColorInterpretation(GDALRasterBandH band)
    int GDALSetRasterColorInterpretation(GDALRasterBandH band, GDALColorInterp)
    int GDALGetMaskFlags(GDALRasterBandH band)
    int GDALCreateDatasetMaskBand(GDALDatasetH hds, int flags)
    void *GDALGetMaskBand(GDALRasterBandH band)
    int GDALCreateMaskBand(GDALDatasetH hds, int flags)
    int GDALGetOverviewCount(GDALRasterBandH band)
    int GDALBuildOverviews(GDALDatasetH hds, const char *resampling,
                           int nOverviews, int *overviews, int nBands,
                           int *bands, void *progress_func,
                           void *progress_data)
    int GDALCheckVersion(int nVersionMajor, int nVersionMinor,
                         const char *pszCallingComponentName)
    const char* GDALVersionInfo(const char *pszRequest)
    CPLErr GDALSetGCPs(GDALDatasetH hDS, int nGCPCount, const GDAL_GCP *pasGCPList,
                       const char *pszGCPProjection)
    const GDAL_GCP *GDALGetGCPs(GDALDatasetH hDS)
    int GDALGetGCPCount(GDALDatasetH hDS)
    const char *GDALGetGCPProjection(GDALDatasetH hDS)
    int GDALGetCacheMax()
    void GDALSetCacheMax(int nBytes)
    GIntBig GDALGetCacheMax64()
    void GDALSetCacheMax64(GIntBig nBytes)
    CPLErr GDALDeleteDataset(GDALDriverH, const char *)
    char** GDALGetFileList(GDALDatasetH hDS)
    CPLErr GDALCopyDatasetFiles (GDALDriverH hDriver, const char * pszNewName, const char * pszOldName)

    void * GDALOpenEx(const char * pszFilename,
                      unsigned int nOpenFlags,
                      const char *const *papszAllowedDrivers,
                      const char *const *papszOpenOptions,
                      const char *const *papszSiblingFiles
                      )
    int GDAL_OF_UPDATE
    int GDAL_OF_READONLY
    int GDAL_OF_VECTOR
    int GDAL_OF_VERBOSE_ERROR
    int GDALDatasetGetLayerCount(void * hds)
    void * GDALDatasetGetLayer(void * hDS, int iLayer)
    void * GDALDatasetGetLayerByName(void * hDS, char * pszName)
    void GDALClose(void * hDS)
    void * GDALCreate(void * hDriver,
                      const char * pszFilename,
                      int nXSize,
                      int     nYSize,
                      int     nBands,
                      GDALDataType eBandType,
                      char ** papszOptions)
    void * GDALDatasetCreateLayer(void * hDS,
                                  const char * pszName,
                                  void * hSpatialRef,
                                  int eType,
                                  char ** papszOptions)
    int GDALDatasetDeleteLayer(void * hDS, int iLayer)
    void GDALFlushCache(void * hDS)
    char * GDALGetDriverShortName(void * hDriver)
    OGRErr GDALDatasetStartTransaction (void * hDataset, int bForce)
    OGRErr GDALDatasetCommitTransaction (void * hDataset)
    OGRErr GDALDatasetRollbackTransaction (void * hDataset)
    int GDALDatasetTestCapability (void * hDataset, char *)


cdef extern from "ogr_api.h" nogil:

    ctypedef void * OGRLayerH
    ctypedef void * OGRDataSourceH
    ctypedef void * OGRSFDriverH
    ctypedef void * OGRFieldDefnH
    ctypedef void * OGRFeatureDefnH
    ctypedef void * OGRFeatureH
    ctypedef void * OGRGeometryH

    ctypedef struct OGREnvelope:
        double MinX
        double MaxX
        double MinY
        double MaxY

    void OGRRegisterAll()
    void OGRCleanupAll()
    int OGRGetDriverCount()

    char *OGR_Dr_GetName(OGRSFDriverH driver)
    OGRDataSourceH OGR_Dr_CreateDataSource(OGRSFDriverH driver,
                                           const char *path, char **options)
    int OGR_Dr_DeleteDataSource(OGRSFDriverH driver, const char *path)
    int OGR_DS_DeleteLayer(OGRDataSourceH datasource, int n)
    OGRLayerH OGR_DS_CreateLayer(OGRDataSourceH datasource, const char *name,
                                 OGRSpatialReferenceH crs, int geomType,
                                 char **options)
    OGRLayerH OGR_DS_ExecuteSQL(OGRDataSourceH, const char *name,
                                OGRGeometryH filter, const char *dialext)
    void OGR_DS_Destroy(OGRDataSourceH datasource)
    OGRSFDriverH OGR_DS_GetDriver(OGRLayerH layer_defn)
    OGRLayerH OGR_DS_GetLayerByName(OGRDataSourceH datasource,
                                    const char *name)
    int OGR_DS_GetLayerCount(OGRDataSourceH datasource)
    OGRLayerH OGR_DS_GetLayer(OGRDataSourceH datasource, int n)
    void OGR_DS_ReleaseResultSet(OGRDataSourceH datasource, OGRLayerH results)
    int OGR_DS_SyncToDisk(OGRDataSourceH datasource)
    OGRFeatureH OGR_F_Create(OGRFeatureDefnH featuredefn)
    void OGR_F_Destroy(OGRFeatureH feature)
    long OGR_F_GetFID(OGRFeatureH feature)
    int OGR_F_IsFieldSet(OGRFeatureH feature, int n)
    int OGR_F_GetFieldAsDateTime(OGRFeatureH feature, int n, int *y, int *m,
                                 int *d, int *h, int *m, int *s, int *z)
    double OGR_F_GetFieldAsDouble(OGRFeatureH feature, int n)
    int OGR_F_GetFieldAsInteger(OGRFeatureH feature, int n)
    const char *OGR_F_GetFieldAsString(OGRFeatureH feature, int n)
    char **OGR_F_GetFieldAsStringList( OGRFeatureH feature, int n)
    int OGR_F_GetFieldCount(OGRFeatureH feature)
    OGRFieldDefnH OGR_F_GetFieldDefnRef(OGRFeatureH feature, int n)
    int OGR_F_GetFieldIndex(OGRFeatureH feature, const char *name)
    OGRGeometryH OGR_F_GetGeometryRef(OGRFeatureH feature)
    void OGR_F_SetFieldDateTime(OGRFeatureH feature, int n, int y, int m,
                                int d, int hh, int mm, int ss, int tz)
    void OGR_F_SetFieldDouble(OGRFeatureH feature, int n, double value)
    void OGR_F_SetFieldInteger(OGRFeatureH feature, int n, int value)
    void OGR_F_SetFieldString(OGRFeatureH feature, int n, const char *value)
    void OGR_F_SetFieldStringList(OGRFeatureH feature, int n, const char **value)
    int OGR_F_SetGeometryDirectly(OGRFeatureH feature, OGRGeometryH geometry)
    OGRFeatureDefnH OGR_FD_Create(const char *name)
    int OGR_FD_GetFieldCount(OGRFeatureDefnH featuredefn)
    OGRFieldDefnH OGR_FD_GetFieldDefn(OGRFeatureDefnH featuredefn, int n)
    int OGR_FD_GetGeomType(OGRFeatureDefnH featuredefn)
    const char *OGR_FD_GetName(OGRFeatureDefnH featuredefn)
    OGRFieldDefnH OGR_Fld_Create(const char *name, int fieldtype)
    void OGR_Fld_Destroy(OGRFieldDefnH)
    char *OGR_Fld_GetNameRef(OGRFieldDefnH)
    int OGR_Fld_GetPrecision(OGRFieldDefnH)
    int OGR_Fld_GetType(OGRFieldDefnH)
    int OGR_Fld_GetWidth(OGRFieldDefnH)
    void OGR_Fld_Set(OGRFieldDefnH, const char *name, int fieldtype, int width,
                     int precision, int justification)
    void OGR_Fld_SetPrecision(OGRFieldDefnH, int n)
    void OGR_Fld_SetWidth(OGRFieldDefnH, int n)
    OGRErr OGR_G_AddGeometryDirectly(OGRGeometryH geometry, OGRGeometryH part)
    OGRErr OGR_G_RemoveGeometry(OGRGeometryH geometry, int i, int delete)
    void OGR_G_AddPoint(OGRGeometryH geometry, double x, double y, double z)
    void OGR_G_AddPoint_2D(OGRGeometryH geometry, double x, double y)
    void OGR_G_CloseRings(OGRGeometryH geometry)
    OGRGeometryH OGR_G_CreateGeometry(int wkbtypecode)
    OGRGeometryH OGR_G_CreateGeometryFromJson(const char *json)
    void OGR_G_DestroyGeometry(OGRGeometryH geometry)
    char *OGR_G_ExportToJson(OGRGeometryH geometry)
    OGRErr OGR_G_ExportToWkb(OGRGeometryH geometry, int endianness, char *buffer)
    int OGR_G_GetCoordinateDimension(OGRGeometryH geometry)
    int OGR_G_GetGeometryCount(OGRGeometryH geometry)
    const char *OGR_G_GetGeometryName(OGRGeometryH geometry)
    int OGR_G_GetGeometryType(OGRGeometryH geometry)
    OGRGeometryH OGR_G_GetGeometryRef(OGRGeometryH geometry, int n)
    int OGR_G_GetPointCount(OGRGeometryH geometry)
    double OGR_G_GetX(OGRGeometryH geometry, int n)
    double OGR_G_GetY(OGRGeometryH geometry, int n)
    double OGR_G_GetZ(OGRGeometryH geometry, int n)
    OGRErr OGR_G_ImportFromWkb(OGRGeometryH geometry, unsigned char *bytes,
                             int nbytes)
    int OGR_G_WkbSize(OGRGeometryH geometry)
    OGRErr OGR_L_CreateFeature(OGRLayerH layer, OGRFeatureH feature)
    int OGR_L_CreateField(OGRLayerH layer, OGRFieldDefnH, int flexible)
    OGRErr OGR_L_GetExtent(OGRLayerH layer, void *extent, int force)
    OGRFeatureH OGR_L_GetFeature(OGRLayerH layer, int n)
    int OGR_L_GetFeatureCount(OGRLayerH layer, int m)
    OGRFeatureDefnH OGR_L_GetLayerDefn(OGRLayerH layer)
    const char *OGR_L_GetName(OGRLayerH layer)
    OGRFeatureH OGR_L_GetNextFeature(OGRLayerH layer)
    OGRGeometryH OGR_L_GetSpatialFilter(OGRLayerH layer)
    OGRSpatialReferenceH OGR_L_GetSpatialRef(OGRLayerH layer)
    void OGR_L_ResetReading(OGRLayerH layer)
    void OGR_L_SetSpatialFilter(OGRLayerH layer, OGRGeometryH geometry)
    void OGR_L_SetSpatialFilterRect(OGRLayerH layer, double minx, double miny,
                                    double maxx, double maxy)
    int OGR_L_TestCapability(OGRLayerH layer, const char *name)
    OGRSFDriverH OGRGetDriverByName(const char *)
    OGRSFDriverH OGRGetDriver(int i)
    OGRDataSourceH OGROpen(const char *path, int mode, void *x)
    OGRDataSourceH OGROpenShared(const char *path, int mode, void *x)
    int OGRReleaseDataSource(OGRDataSourceH datasource)
    const char * OGR_Dr_GetName (void *driver)
    int     OGR_Dr_TestCapability (void *driver, const char *)
    void *  OGR_F_Create (void *featuredefn)
    void    OGR_F_Destroy (void *feature)
    long    OGR_F_GetFID (void *feature)
    int     OGR_F_IsFieldSet (void *feature, int n)
    int     OGR_F_GetFieldAsDateTimeEx (void *feature, int n, int *y, int *m, int *d, int *h, int *m, float *s, int *z)
    double  OGR_F_GetFieldAsDouble (void *feature, int n)
    int     OGR_F_GetFieldAsInteger (void *feature, int n)
    char *  OGR_F_GetFieldAsString (void *feature, int n)
    unsigned char * OGR_F_GetFieldAsBinary(void *feature, int n, int *s)
    int     OGR_F_GetFieldCount (void *feature)
    void *  OGR_F_GetFieldDefnRef (void *feature, int n)
    int     OGR_F_GetFieldIndex (void *feature, char *name)
    void *  OGR_F_GetGeometryRef (void *feature)
    void *  OGR_F_StealGeometry (void *feature)
    void    OGR_F_SetFieldDateTimeEx (void *feature, int n, int y, int m, int d, int hh, int mm, float ss, int tz)
    void    OGR_F_SetFieldDouble (void *feature, int n, double value)
    void    OGR_F_SetFieldInteger (void *feature, int n, int value)
    void    OGR_F_SetFieldString (void *feature, int n, char *value)
    void    OGR_F_SetFieldBinary (void *feature, int n, int l, unsigned char *value)
    void    OGR_F_SetFieldNull (void *feature, int n)  # new in GDAL 2.2
    int     OGR_F_SetGeometryDirectly (void *feature, void *geometry)
    void *  OGR_FD_Create (char *name)
    int     OGR_FD_GetFieldCount (void *featuredefn)
    void *  OGR_FD_GetFieldDefn (void *featuredefn, int n)
    int     OGR_FD_GetGeomType (void *featuredefn)
    char *  OGR_FD_GetName (void *featuredefn)
    OGRFieldSubType OGR_Fld_GetSubType(void *fielddefn)
    void    OGR_Fld_SetSubType(void *fielddefn, OGRFieldSubType subtype)
    void *  OGR_G_ForceToMultiPolygon (void *geometry)
    void *  OGR_G_ForceToPolygon (void *geometry)
    void *  OGR_G_Clone(void *geometry)
    void *  OGR_G_GetLinearGeometry (void *hGeom, double dfMaxAngleStepSizeDegrees, char **papszOptions)
    OGRErr  OGR_L_SetIgnoredFields (void *layer, const char **papszFields)
    OGRErr  OGR_L_SetAttributeFilter(void *layer, const char*)
    OGRErr  OGR_L_SetNextByIndex (void *layer, long nIndex)
    long long OGR_F_GetFieldAsInteger64 (void *feature, int n)
    void    OGR_F_SetFieldInteger64 (void *feature, int n, long long value)
    int OGR_F_IsFieldNull(void *feature, int n)
    OGRwkbGeometryType OGR_GT_GetLinear(OGRwkbGeometryType eType)


cdef extern from "gdalwarper.h" nogil:

    ctypedef enum GDALResampleAlg:
        GRA_NearestNeighbour
        GRA_Bilinear
        GRA_Cubic
        GRA_CubicSpline
        GRA_Lanczos
        GRA_Average
        GRA_Mode

    ctypedef int (*GDALMaskFunc)(
        void *pMaskFuncArg, int nBandCount, int eType, int nXOff, int nYOff,
        int nXSize, int nYSize, unsigned char **papabyImageData,
        int bMaskIsFloat, void *pMask)

    ctypedef int (*GDALTransformerFunc)(
        void *pTransformerArg, int bDstToSrc, int nPointCount, double *x,
        double *y, double *z, int *panSuccess)

    ctypedef struct GDALWarpOptions:
        char **papszWarpOptions
        double dfWarpMemoryLimit
        GDALResampleAlg eResampleAlg
        GDALDataType eWorkingDataType
        GDALDatasetH hSrcDS
        GDALDatasetH hDstDS
        # 0 for all bands
        int nBandCount
        # List of source band indexes
        int *panSrcBands
        # List of destination band indexes
        int *panDstBands
        # The source band so use as an alpha (transparency) value, 0=disabled
        int nSrcAlphaBand
        # The dest. band so use as an alpha (transparency) value, 0=disabled
        int nDstAlphaBand
        # The "nodata" value real component for each input band, if NULL there isn't one */
        double *padfSrcNoDataReal
        # The "nodata" value imaginary component - may be NULL even if real component is provided. */
        double *padfSrcNoDataImag
        # The "nodata" value real component for each output band, if NULL there isn't one */
        double *padfDstNoDataReal
        # The "nodata" value imaginary component - may be NULL even if real component is provided. */
        double *padfDstNoDataImag
        # GDALProgressFunc() compatible progress reporting function, or NULL if there isn't one. */
        void *pfnProgress
        # Callback argument to be passed to pfnProgress. */
        void *pProgressArg
        # Type of spatial point transformer function */
        GDALTransformerFunc pfnTransformer
        # Handle to image transformer setup structure */
        void *pTransformerArg
        GDALMaskFunc *papfnSrcPerBandValidityMaskFunc
        void **papSrcPerBandValidityMaskFuncArg
        GDALMaskFunc pfnSrcValidityMaskFunc
        void *pSrcValidityMaskFuncArg
        GDALMaskFunc pfnSrcDensityMaskFunc
        void *pSrcDensityMaskFuncArg
        GDALMaskFunc pfnDstDensityMaskFunc
        void *pDstDensityMaskFuncArg
        GDALMaskFunc pfnDstValidityMaskFunc
        void *pDstValidityMaskFuncArg
        int (*pfnPreWarpChunkProcessor)(void *pKern, void *pArg)
        void *pPreWarpProcessorArg
        int (*pfnPostWarpChunkProcessor)(void *pKern, void *pArg)
        void *pPostWarpProcessorArg
        # Optional OGRPolygonH for a masking cutline. */
        OGRGeometryH hCutline
        # Optional blending distance to apply across cutline in pixels, default is 0
        double dfCutlineBlendDist

    GDALWarpOptions *GDALCreateWarpOptions()
    void GDALDestroyWarpOptions(GDALWarpOptions *options)

    GDALDatasetH GDALAutoCreateWarpedVRT(
        GDALDatasetH hSrcDS, const char *pszSrcWKT, const char *pszDstWKT,
        GDALResampleAlg eResampleAlg, double dfMaxError,
        const GDALWarpOptions *psOptionsIn)

    GDALDatasetH GDALCreateWarpedVRT(
        GDALDatasetH hSrcDS, int nPixels, int nLines,
         double *padfGeoTransform, const GDALWarpOptions *psOptionsIn)


cdef extern from "gdal_alg.h" nogil:

    int GDALPolygonize(GDALRasterBandH band, GDALRasterBandH mask_band,
                       OGRLayerH layer, int fidx, char **options,
                       void *progress_func, void *progress_data)
    int GDALFPolygonize(GDALRasterBandH band, GDALRasterBandH mask_band,
                        OGRLayerH layer, int fidx, char **options,
                        void *progress_func, void *progress_data)
    int GDALSieveFilter(GDALRasterBandH src_band, GDALRasterBandH mask_band,
                        GDALRasterBandH dst_band, int size, int connectivity,
                        char **options, void *progress_func,
                        void *progress_data)
    int GDALRasterizeGeometries(GDALDatasetH hds, int band_count,
                                int *dst_bands, int geom_count,
                                OGRGeometryH *geometries,
                                GDALTransformerFunc transform_func,
                                void *transform, double *pixel_values,
                                char **options, void *progress_func,
                                void *progress_data)
    void *GDALCreateGenImgProjTransformer(GDALDatasetH src_hds,
                                 const char *pszSrcWKT, GDALDatasetH dst_hds,
                                 const char *pszDstWKT,
                                 int bGCPUseOK, double dfGCPErrorThreshold,
                                 int nOrder)
    void *GDALCreateGenImgProjTransformer2(GDALDatasetH src_hds, GDALDatasetH dst_hds, char **options)
    void *GDALCreateGenImgProjTransformer3(
            const char *pszSrcWKT, const double *padfSrcGeoTransform,
            const char *pszDstWKT, const double *padfDstGeoTransform)
    void GDALSetGenImgProjTransformerDstGeoTransform(void *hTransformArg, double *padfGeoTransform)
    int GDALGenImgProjTransform(void *pTransformArg, int bDstToSrc,
                                int nPointCount, double *x, double *y,
                                double *z, int *panSuccess)
    void GDALDestroyGenImgProjTransformer(void *)
    void *GDALCreateApproxTransformer(GDALTransformerFunc pfnRawTransformer,
                                      void *pRawTransformerArg,
                                      double dfMaxError)
    int  GDALApproxTransform(void *pTransformArg, int bDstToSrc, int npoints,
                             double *x, double *y, double *z, int *panSuccess)
    void GDALDestroyApproxTransformer(void *)
    void GDALApproxTransformerOwnsSubtransformer(void *, int)
    int GDALFillNodata(GDALRasterBandH dst_band, GDALRasterBandH mask_band,
                       double max_search_distance, int deprecated,
                       int smoothing_iterations, char **options,
                       void *progress_func, void *progress_data)
    int GDALChecksumImage(GDALRasterBandH band, int xoff, int yoff, int width,
                          int height)
    int GDALSuggestedWarpOutput2(
            GDALDatasetH hSrcDS, GDALTransformerFunc pfnRawTransformer,
            void * pTransformArg, double * padfGeoTransformOut, int * pnPixels,
            int * pnLines, double * padfExtent, int nOptions)
