LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(BSD-3-Clause)

VERSION(Service-proxy-version)

NO_PLATFORM()

IF (HAVE_CUDA)
    PEERDIR(
        build/internal/platform/cuda
    )
    IF (NOT OS_WINDOWS)
        LDFLAGS(-lcurand_static)
    ELSE()
        LDFLAGS(curand.lib)
    ENDIF()
ENDIF()

END()

RECURSE(
    implib
)
