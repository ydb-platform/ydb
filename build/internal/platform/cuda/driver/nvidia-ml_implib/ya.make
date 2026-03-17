LIBRARY()

PEERDIR(build/internal/platform/cuda)

NO_RUNTIME()

IF (ARCH_X86_64)
    SET(LIBDIR lib64)
ELSE()
    SET(LIBDIR lib)
ENDIF()

GENERATE_IMPLIB(libnvidia-ml $CUDA_TARGET_ROOT/$LIBDIR/stubs/libnvidia-ml.so)

END()
