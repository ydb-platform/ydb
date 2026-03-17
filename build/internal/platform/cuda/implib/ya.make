LIBRARY()

PEERDIR(
    build/internal/platform/cuda
)

NO_RUNTIME()

IF (ARCH_X86_64)
    SET(LIBDIR lib64)
ELSE()
    SET(LIBDIR lib)
ENDIF()

SET(CALLBACK "_")
IF (SANITIZER_TYPE)
    SET(CALLBACK "__cuda_sanitizers_dlopen_stub")
    SRCS(cuda_stub_loader.cpp)
ENDIF()

GENERATE_IMPLIB(cudart $CUDA_TARGET_ROOT/$LIBDIR/libcudart.so DLOPEN_CALLBACK $CALLBACK)

END()
