#cython: freethreading_compatible = True

from libc.stdint cimport uint32_t

cdef extern from "FeatureDetector/CpuInfo.hpp":
    cdef int CPU_FEATURE_SSE2
    cdef int CPU_FEATURE_AVX2

    cdef cppclass CpuInfo:
        @staticmethod
        bint supports(uint32_t features)

SSE2 = CPU_FEATURE_SSE2
AVX2 = CPU_FEATURE_AVX2

def supports(uint32_t features):
    return CpuInfo.supports(features)
