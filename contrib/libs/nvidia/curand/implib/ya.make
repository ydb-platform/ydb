LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Nvidia-Gov)

VERSION(Service-proxy-version)

PEERDIR(
    build/internal/platform/cuda
)

NO_RUNTIME()

IF (ARCH_X86_64)
    SET(LIBDIR lib64)
ELSE()
    SET(LIBDIR lib)
ENDIF()

GENERATE_IMPLIB(curand $CUDA_TARGET_ROOT/$LIBDIR/stubs/libcurand.so)

END()
