# GDAL API definitions.

from libc.stdio cimport FILE


cdef extern from "cpl_conv.h" nogil:

    void *CPLMalloc(size_t)
    void CPLFree(void* ptr)
    void CPLSetThreadLocalConfigOption(const char* key, const char* val)
    void CPLSetConfigOption(const char* key, const char* val)
    const char *CPLGetConfigOption(const char* key, const char* default)
    const char *CPLFindFile(const char *pszClass, const char *pszBasename)


cdef extern from "cpl_port.h":
    ctypedef char **CSLConstList


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
    void *CPLGetErrorHandlerUserData()
    void CPLPushErrorHandler(CPLErrorHandler handler)
    void CPLPushErrorHandlerEx(CPLErrorHandler handler, void *userdata)
    void CPLPopErrorHandler()
    void CPLQuietErrorHandler(CPLErr eErrClass, CPLErrorNum nError, const char *pszErrorMsg)


cdef extern from "cpl_progress.h":

    ctypedef int (*GDALProgressFunc)(double dfComplete, const char *pszMessage, void *pProgressArg)


cdef extern from "cpl_string.h" nogil:

    int CSLCount(char **papszStrList)
    char **CSLAddString(char **strlist, const char *string)
    char **CSLAddNameValue(char **papszStrList, const char *pszName,
                           const char *pszValue)
    char **CSLDuplicate(char **papszStrList)
    int CSLFindName(char **papszStrList, const char *pszName)
    int CSLFindString(char **papszStrList, const char *pszString)
    int CSLFetchBoolean(char **papszStrList, const char *pszName, int default)
    const char *CSLFetchNameValue(char **papszStrList, const char *pszName)
    char **CSLSetNameValue(char **list, char *name, char *val)
    void CSLDestroy(char **list)
    char **CSLMerge(char **first, char **second)
    const char* CPLParseNameValue(const char *pszNameValue, char **ppszKey)


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

    void VSICurlPartialClearCache(const char *)
    void VSICurlClearCache()

IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 9):
    cdef extern from "cpl_vsi.h" nogil:
        int VSIRemovePluginHandler(const char*)


cdef extern from "ogr_srs_api.h" nogil:
    ctypedef int OGRErr
    ctypedef void * OGRCoordinateTransformationH
    ctypedef void * OGRSpatialReferenceH

    OGRCoordinateTransformationH OCTNewCoordinateTransformation(
                                        OGRSpatialReferenceH source,
                                        OGRSpatialReferenceH dest)
    void OCTDestroyCoordinateTransformation(
        OGRCoordinateTransformationH source)
    int OCTTransform(OGRCoordinateTransformationH ct, int nCount, double *x,
                     double *y, double *z)
    void OSRCleanup()
    OGRSpatialReferenceH OSRClone(OGRSpatialReferenceH srs)
    OGRSpatialReferenceH OSRCloneGeogCS(OGRSpatialReferenceH srs)
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
    int OSRIsSameEx(OGRSpatialReferenceH srs1, OGRSpatialReferenceH srs2, const char* const* papszOptions)
    OGRSpatialReferenceH OSRNewSpatialReference(const char *wkt)
    void OSRRelease(OGRSpatialReferenceH srs)
    int OSRSetFromUserInput(OGRSpatialReferenceH srs, const char *input)
    OGRErr OSRValidate(OGRSpatialReferenceH srs)
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

    int OCTTransformBounds(
        OGRCoordinateTransformationH hCT,
        const double xmin,
        const double ymin,
        const double xmax,
        const double ymax,
        double* out_xmin,
        double* out_ymin,
        double* out_xmax,
        double* out_ymax,
        const int densify_pts )


cdef extern from "gdal.h" nogil:

    const int GDAL_OF_READONLY
    const int GDAL_OF_UPDATE
    const int GDAL_OF_RASTER
    const int GDAL_OF_SHARED
    const int GDAL_OF_VERBOSE_ERROR

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
        GDT_Int8
        GDT_UInt16
        GDT_Int16
        GDT_UInt32
        GDT_Int32
        GDT_UInt64
        GDT_Int64
        GDT_Float16
        GDT_Float32
        GDT_Float64
        GDT_CInt16
        GDT_CInt32
        GDT_CFloat16
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
        # below enums exist in GDAL 3.10+
        GCI_PanBand
        GCI_CoastalBand
        GCI_RedEdgeBand
        GCI_NIRBand
        GCI_SWIRBand
        GCI_MWIRBand
        GCI_LWIRBand
        GCI_TIRBand
        GCI_OtherIRBand
        GCI_IR_Reserved_1
        GCI_IR_Reserved_2
        GCI_IR_Reserved_3
        GCI_IR_Reserved_4
        GCI_SAR_Ka_Band
        GCI_SAR_K_Band
        GCI_SAR_Ku_Band
        GCI_SAR_X_Band
        GCI_SAR_C_Band
        GCI_SAR_S_Band
        GCI_SAR_L_Band
        GCI_SAR_P_Band
        GCI_SAR_Reserved_1
        GCI_SAR_Reserved_2
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

    ctypedef struct GDALRPCInfo:
        double dfLINE_OFF
        double dfSAMP_OFF
        double dfLAT_OFF
        double dfLONG_OFF
        double dfHEIGHT_OFF

        double dfLINE_SCALE
        double dfSAMP_SCALE
        double dfLAT_SCALE
        double dfLONG_SCALE
        double dfHEIGHT_SCALE

        double adfLINE_NUM_COEFF[20]
        double adfLINE_DEN_COEFF[20]
        double adfSAMP_NUM_COEFF[20]
        double adfSAMP_DEN_COEFF[20]

        double dfMIN_LONG
        double dfMIN_LAT
        double dfMAX_LONG
        double dfMAX_LAT

    int GDALExtractRPCInfo(char **papszMD, GDALRPCInfo * )
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
    GDALDatasetH GDALOpenEx(const char *filename, int flags, const char **allowed_drivers, const char **options, const char **siblings) # except -1
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
    int GDALDeleteRasterNoDataValue(GDALRasterBandH hBand)
    int GDALDatasetRasterIO(GDALRasterBandH band, int, int xoff, int yoff,
                            int xsize, int ysize, void *buffer, int width,
                            int height, int, int count, int *bmap, int poff,
                            int loff, int boff)
    CPLErr GDALDatasetRasterIOEx(GDALDatasetH hDS, GDALRWFlag eRWFlag, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer, int nBXSize, int nBYSize, GDALDataType eBDataType, int nBandCount, int *panBandCount, GSpacing nPixelSpace, GSpacing nLineSpace, GSpacing nBandSpace, GDALRasterIOExtraArg *psExtraArg)
    int GDALRasterIO(GDALRasterBandH band, int, int xoff, int yoff, int xsize,
                     int ysize, void *buffer, int width, int height, int,
                     int poff, int loff)
    CPLErr GDALGetRasterStatistics(GDALRasterBandH band, int approx, int force, double *min, double *max, double *mean, double *std)
    void GDALDatasetClearStatistics(GDALDatasetH hDS)
    CPLErr GDALComputeRasterStatistics(GDALRasterBandH band, int approx, double *min, double *max, double *mean, double *std, void *, void *)
    CPLErr GDALSetRasterStatistics(GDALRasterBandH band, double min, double max, double mean, double std)
    ctypedef struct GDALRasterIOExtraArg:
        int nVersion
        GDALRIOResampleAlg eResampleAlg
        GDALProgressFunc pfnProgress
        void *pProgressData
        int bFloatingPointWindowValidity
        double dfXOff
        double dfYOff
        double dfXSize
        double dfYSize

    CPLErr GDALRasterIOEx(GDALRasterBandH hRBand, GDALRWFlag eRWFlag, int nDSXOff, int nDSYOff, int nDSXSize, int nDSYSize, void *pBuffer, int nBXSize, int nBYSize, GDALDataType eBDataType, GSpacing nPixelSpace, GSpacing nLineSpace, GDALRasterIOExtraArg *psExtraArg)

    int GDALFillRaster(GDALRasterBandH band, double rvalue, double ivalue)
    GDALDatasetH GDALCreate(GDALDriverH driver, const char *path, int width,
                            int height, int nbands, GDALDataType dtype,
                            const char **options)
    GDALDatasetH GDALCreateCopy(GDALDriverH driver, const char *path,
                                GDALDatasetH hds, int strict, char **options,
                                void *progress_func, void *progress_data)
    char** GDALGetMetadataDomainList(GDALMajorObjectH obj)
    char** GDALGetMetadata(GDALMajorObjectH obj, const char *pszDomain)
    int GDALSetMetadata(GDALMajorObjectH obj, char **papszMD,
                        const char *pszDomain)
    const char* GDALGetMetadataItem(GDALMajorObjectH obj, const char *pszName,
                                    const char *pszDomain)
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
    CPLErr GDALSetGCPs(GDALDatasetH hDS, int nGCPCount, const GDAL_GCP *pasGCPList,
                       const char *pszGCPProjection)
    const GDAL_GCP *GDALGetGCPs(GDALDatasetH hDS)
    int GDALGetGCPCount(GDALDatasetH hDS)
    const char *GDALGetGCPProjection(GDALDatasetH hDS)
    int GDALGCPsToGeoTransform(int nGCPCount, const GDAL_GCP *pasGCPs, double *padfGeoTransform,
                               int bApproxOK)
    int GDALGetCacheMax()
    void GDALSetCacheMax(int nBytes)
    GIntBig GDALGetCacheMax64()
    void GDALSetCacheMax64(GIntBig nBytes)
    CPLErr GDALDeleteDataset(GDALDriverH, const char *)
    char** GDALGetFileList(GDALDatasetH hDS)
    CPLErr GDALCopyDatasetFiles (GDALDriverH hDriver, const char * pszNewName, const char * pszOldName)

    double GDALGetRasterScale(GDALRasterBandH hBand, int * pbSuccess)
    double GDALGetRasterOffset(GDALRasterBandH hBand, int * pbSuccess)
    CPLErr GDALSetRasterScale(GDALRasterBandH hBand, double dfNewScale)
    CPLErr GDALSetRasterOffset(GDALRasterBandH hBand, double dfNewOffset)

    int GDALDumpOpenDatasets(FILE *fp)

    int GDALReferenceDataset(GDALDatasetH hds)
    int GDALDereferenceDataset(GDALDatasetH hds)

IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 10):
    cdef extern from "gdal.h" nogil:
        const int GDAL_OF_THREAD_SAFE
ELSE:
    cdef int GDAL_OF_THREAD_SAFE = 0x800

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
    int OGR_F_GetFieldCount(OGRFeatureH feature)
    OGRFieldDefnH OGR_F_GetFieldDefnRef(OGRFeatureH feature, int n)
    int OGR_F_GetFieldIndex(OGRFeatureH feature, const char *name)
    OGRGeometryH OGR_F_GetGeometryRef(OGRFeatureH feature)
    void OGR_F_SetFieldDateTime(OGRFeatureH feature, int n, int y, int m,
                                int d, int hh, int mm, int ss, int tz)
    void OGR_F_SetFieldDouble(OGRFeatureH feature, int n, double value)
    void OGR_F_SetFieldInteger(OGRFeatureH feature, int n, int value)
    void OGR_F_SetFieldString(OGRFeatureH feature, int n, const char *value)
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
    void OGR_G_AddPoint(OGRGeometryH geometry, double x, double y, double z)
    void OGR_G_AddPoint_2D(OGRGeometryH geometry, double x, double y)
    void OGR_G_CloseRings(OGRGeometryH geometry)
    OGRGeometryH OGR_G_CreateGeometry(int wkbtypecode)
    OGRGeometryH OGR_G_CreateGeometryFromJson(const char *json)
    void OGR_G_DestroyGeometry(OGRGeometryH geometry)
    char *OGR_G_ExportToJson(OGRGeometryH geometry)
    void OGR_G_ExportToWkb(OGRGeometryH geometry, int endianness, char *buffer)
    int OGR_G_GetCoordinateDimension(OGRGeometryH geometry)
    int OGR_G_GetGeometryCount(OGRGeometryH geometry)
    const char *OGR_G_GetGeometryName(OGRGeometryH geometry)
    int OGR_G_GetGeometryType(OGRGeometryH geometry)
    OGRGeometryH OGR_G_GetGeometryRef(OGRGeometryH geometry, int n)
    int OGR_G_GetPointCount(OGRGeometryH geometry)
    double OGR_G_GetX(OGRGeometryH geometry, int n)
    double OGR_G_GetY(OGRGeometryH geometry, int n)
    double OGR_G_GetZ(OGRGeometryH geometry, int n)
    void OGR_G_ImportFromWkb(OGRGeometryH geometry, unsigned char *bytes,
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
    OGRDataSourceH OGROpen(const char *path, int mode, void *x)
    OGRDataSourceH OGROpenShared(const char *path, int mode, void *x)
    int OGRReleaseDataSource(OGRDataSourceH datasource)


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

    GDALDatasetH GDALAutoCreateWarpedVRTEx(
        GDALDatasetH hSrcDS, const char *pszSrcWKT, const char *pszDstWKT,
        GDALResampleAlg eResampleAlg, double dfMaxError,
        const GDALWarpOptions *psOptions, char** papszTransformerOptions)


cdef extern from "gdal_alg.h" nogil:
    void *GDALCreateGCPTransformer( int nGCPCount, const GDAL_GCP *pasGCPList,
                          int nReqOrder, int bReversed)
    void *GDALDestroyGCPTransformer( void *pTransformArg)
    int GDALGCPTransform( void *pTransformArg, int bDstToSrc, int nPointCount,
                          double *x, double *y, double *z, int *panSuccess)
    void *GDALCreateTPSTransformer( int nGCPCount, const GDAL_GCP *pasGCPList,
                          int bReversed)
    void *GDALDestroyTPSTransformer( void *pTransformArg)
    int GDALTPSTransform( void *pTransformArg, int bDstToSrc, int nPointCount,
                          double *x, double *y, double *z, int *panSuccess)
    void *GDALCreateRPCTransformer( GDALRPCInfo *psRPC, int bReversed,
                          double dfPixErrThreshold,
                          char **papszOptions )
    void GDALDestroyRPCTransformer( void *pTransformArg )
    int GDALRPCTransform(void *pTransformArg, int bDstToSrc,
                         int nPointCount, double *x, double *y, double *z,
                         int *panSuccess )

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

IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 11):
    cdef extern from "gdal_alg.h" nogil:
        const char *GDALGetGenImgProjTranformerOptionList()


cdef extern from "ogr_core.h" nogil:

    char *OGRGeometryTypeToName(int type)

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