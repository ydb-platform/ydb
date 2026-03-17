LIBRARY()

NO_UTIL()
NO_RUNTIME()
NO_PLATFORM()

PEERDIR(
    build/internal/platform/cuda/res
    build/internal/platform/cuda/plugin
)

# For backward-compatibility only
# Targets using CUDA Driver API should use PEERDIR directly
IF (CUDA_REQUIRED)
    PEERDIR(
        build/internal/platform/cuda/driver
    )
ENDIF()

END()

RECURSE(
    driver
    implib
    sanitizers
)
