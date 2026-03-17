cdef extern from "gdal.h" nogil:
    int GDALCheckVersion(int nVersionMajor, int nVersionMinor,
                         const char *pszCallingComponentName)
    const char* GDALVersionInfo(const char *pszRequest)

cdef extern from "ogr_srs_api.h" nogil:
    void OSRGetPROJVersion(int *pnMajor, int *pnMinor, int *pnPatch)

cdef extern from "ogr_core.h" nogil:
    bint OGRGetGEOSVersion(int *pnMajor, int *pnMinor, int *pnPatch)
