LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(BSD-3-Clause)

VERSION(Service-proxy-version)

NO_PLATFORM()

IF (HAVE_CUDA)
    PEERDIR(
        build/internal/platform/cuda
    )
    IF (OS_LINUX)
        LDFLAGS(
            -lcublas_static
            -lcublasLt_static
        )
        PEERDIR(
            contrib/libs/cxxsupp/stdc
        )
    ELSE()
        # OS_WINDOWS
        LDFLAGS(
            cublas.lib
            cublasLt.lib
        )
    ENDIF()
ENDIF()

END()

RECURSE(
    implib
)
