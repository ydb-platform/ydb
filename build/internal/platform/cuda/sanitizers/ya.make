LIBRARY()

NO_UTIL()
NO_RUNTIME()
NO_PLATFORM()

PEERDIR(
    build/internal/platform/cuda/sanitizers/res
    build/internal/platform/cuda/plugin
)

END()

RECURSE(
    res
)
