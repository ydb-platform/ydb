# Cross-platform API functions.

cdef extern from "cpl_conv.h":
    void *  CPLMalloc (size_t)
    void    CPLFree (void *ptr)
    void    CPLSetThreadLocalConfigOption (char *key, char *val)
    const char *CPLGetConfigOption (char *, char *)

cdef extern from "cpl_vsi.h":
    ctypedef struct VSILFILE:
        pass
    int VSIFCloseL (VSILFILE *)
    VSILFILE * VSIFileFromMemBuffer (const char * filename,
                                     unsigned char * data,
                                     int data_len,
                                     int take_ownership)
    int VSIUnlink (const char * pathname)

ctypedef int OGRErr
ctypedef struct OGREnvelope:
    double MinX
    double MaxX
    double MinY
    double MaxY
