# This resource library is part of the CUDA toolchain.
# The resources provided by this library contain only shared libraries
# starting with the prefix "libcublas". Use this library for testing purposes
# only - when it is necessary to run your GPU tests on YT.

RESOURCES_LIBRARY()

WITHOUT_LICENSE_TEXTS()
LICENSE(BSD-3-Clause)

VERSION(Service-proxy-version)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

IF (CUDA_VERSION == "12.6")
    DECLARE_EXTERNAL_RESOURCE(CUBLAS_ENV_SHARED sbr:8689928725)
ELSEIF (CUDA_VERSION == "11.8")
    DECLARE_EXTERNAL_RESOURCE(CUBLAS_ENV_SHARED sbr:8690191160)
ELSE()
    MESSAGE(FATAL "Not supported CUBLAS_ENV_SHARED library for CUDA: $CUDA_VERSION, probably you need one more resource")
ENDIF()

END()
