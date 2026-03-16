IF (SANITIZER_TYPE)
    MESSAGE(WARNING "Binary files with CUDA and sanitizers may be aborted during a CUDA function call")    
ENDIF()
DISABLE(SANITIZER_TYPE)

IF (AUTOCHECK)
    ENABLE(HAVE_CUDA)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/build/internal/platform/cuda/res/ya.make)
