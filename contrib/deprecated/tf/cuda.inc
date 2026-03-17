IF (TENSORFLOW_WITH_CUDA)
    PEERDIR(
        contrib/libs/nvidia/cudnn
    )

    ADDINCL(
        contrib/libs/nvidia/cudnn
    )
    PEERDIR(contrib/libs/nvidia/cusolver)
    IF (OS_LINUX)
        PEERDIR(contrib/libs/nvidia/cufft)
    ENDIF()
    IF (OS_DARWIN)
        LDFLAGS(
            -framework CoreFoundation
            -framework IOKit
        )
    ENDIF()

    CFLAGS(
        -DGOOGLE_CUDA=1
        -I$CUDA_ROOT/extras/CUPTI/include
    )
    IF (TENSORFLOW_WITH_OLD_GPUS)
        CUDA_NVCC_FLAGS(
            -gencode=arch=compute_35,code=sm_35
            -gencode=arch=compute_52,code=sm_52
        )
    ENDIF()
    CUDA_NVCC_FLAGS(
        -gencode arch=compute_61,code=sm_61
        -gencode arch=compute_70,code=sm_70
        --expt-relaxed-constexpr
        --ftz=true
    )
ENDIF()
