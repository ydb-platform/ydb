from rasterio._io cimport DatasetReaderBase

include "gdal.pxi"


cdef extern from "gdalwarper.h" nogil:

    ctypedef enum GDALResampleAlg:
        pass

    ctypedef struct GDALWarpOptions

    cdef cppclass GDALWarpOperation:
        GDALWarpOperation() except +
        int Initialize(const GDALWarpOptions *psNewOptions)
        const GDALWarpOptions *GetOptions()
        int ChunkAndWarpImage(
            int nDstXOff, int nDstYOff, int nDstXSize, int nDstYSize )
        int ChunkAndWarpMulti(
            int nDstXOff, int nDstYOff, int nDstXSize, int nDstYSize )
        int WarpRegion( int nDstXOff, int nDstYOff,
                        int nDstXSize, int nDstYSize,
                        int nSrcXOff=0, int nSrcYOff=0,
                        int nSrcXSize=0, int nSrcYSize=0,
                        double dfProgressBase=0.0, double dfProgressScale=1.0)
        int WarpRegionToBuffer( int nDstXOff, int nDstYOff,
                                int nDstXSize, int nDstYSize,
                                void *pDataBuf,
                                int eBufDataType,
                                int nSrcXOff=0, int nSrcYOff=0,
                                int nSrcXSize=0, int nSrcYSize=0,
                                double dfProgressBase=0.0,
                                double dfProgressScale=1.0)


cdef extern from "ogr_geometry.h" nogil:

    cdef cppclass OGRGeometry:
        pass

    cdef cppclass OGRGeometryFactory:
        void * transformWithOptions(void *geom, void *ct, char **options)


cdef extern from "ogr_spatialref.h":

    cdef cppclass OGRCoordinateTransformation:
        pass


cdef extern from "gdal_alg.h":

    ctypedef int (*GDALTransformerFunc)(
        void *pTransformerArg, int bDstToSrc, int nPointCount, double *x,
        double *y, double *z, int *panSuccess)


cdef class WarpedVRTReaderBase(DatasetReaderBase):
    pass
