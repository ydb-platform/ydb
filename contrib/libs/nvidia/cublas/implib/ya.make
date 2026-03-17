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

GENERATE_IMPLIB(cublas $CUDA_TARGET_ROOT/$LIBDIR/stubs/libcublas.so)
GENERATE_IMPLIB(cublasLt $CUDA_TARGET_ROOT/$LIBDIR/stubs/libcublasLt.so)

END()
