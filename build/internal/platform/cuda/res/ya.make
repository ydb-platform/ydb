RESOURCES_LIBRARY()

# https://docs.yandex-team.ru/ya-make/manual/project_specific/cuda#cuda_host_compiler

OPENSOURCE_EXPORT_REPLACEMENT(
    CMAKE CUDAToolkit
    CMAKE_TARGET CUDA::toolkit
)

IF (NOT HAVE_CUDA)
    MESSAGE(FATAL_ERROR "No CUDA Toolkit for your build")
ENDIF()

IF (SANITIZER_TYPE AND NOT CUDA_SANITIZE)
    MESSAGE(FATAL_ERROR "Binary files with CUDA and sanitizers may be aborted during a CUDA function call")
ENDIF()

# This is a workaround for YMAKE-1727.
# In some cases CUDA location doesn't incorporate into compilation nodes' uids,
# so we have to add a direct dependency to it.
# It's crucial to use a non-global variable here.
# Also when updating a resource for some CUDA version, one must update CUDA_FAKEID variable.
SET(CUDA_FAKEID "20251110")
CFLAGS(
    GLOBAL "-DCUDA_VERSION_FOR_CACHE_INVALIDATION=${CUDA_VERSION}"
    GLOBAL "-DCUDA_FAKEID=${CUDA_FAKEID}"
)

IF (USE_ARCADIA_CUDA)
    IF (HOST_OS_LINUX AND HOST_ARCH_X86_64)
        IF (OS_LINUX AND ARCH_X86_64)
            IF (CUDA_VERSION == "13.0")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:9759105406) # CUDA Toolkit 13.0.1 for Linux x86-64 patched with https://paste.yandex-team.ru/49e19ccf-43dd-400c-9cd5-8e51f4f2c71c
            ELSEIF (CUDA_VERSION == "12.9")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:9346600889) # CUDA Toolkit 12.9.1 for Linux x86-64 patched with https://paste.yandex-team.ru/e9508ce2-3d8b-49d7-a5a1-544144117a1a
            ELSEIF (CUDA_VERSION == "12.8")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:8211571945) # CUDA Toolkit 12.8.1 for Linux x86-64 patched with https://paste.yandex-team.ru/de1776fa-467b-4509-8b3a-cac555cbc7b2
            ELSEIF (CUDA_VERSION == "12.6.2")
                 DECLARE_EXTERNAL_RESOURCE(CUDA sbr:11092672114) # CUDA Toolkit 12.6.2 for Linux x86-64 patched with https://paste.yandex-team.ru/ee46341f-a8a7-4af0-aea6-7c9b3b056d03
            ELSEIF (CUDA_VERSION == "12.6" OR CUDA_VERSION == "12.6.3")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:7978027151) # CUDA Toolkit 12.6.3 for Linux x86-64 patched with https://paste.yandex-team.ru/047d50ab-d9f7-4749-9cca-d2fb9800e1c7
            ELSEIF (CUDA_VERSION == "12.2")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:5992886800) # CUDA Toolkit 12.2.2 for Linux x86-64
            ELSEIF (CUDA_VERSION == "12.1")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:5048904205) # CUDA Toolkit 12.1.1 for Linux x86-64
            ELSEIF (CUDA_VERSION == "11.8")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:4526354763) # CUDA Toolkit 11.8.0 for Linux x86-64
            ELSEIF (CUDA_VERSION == "11.4")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:4899244608) # CUDA Toolkit 11.4.4 for Linux x86-64
            ELSE()
                ENABLE(CUDA_NOT_FOUND)
            ENDIF()
        ELSEIF(OS_LINUX AND ARCH_AARCH64)
            IF (CUDA_VERSION == "13.0")
                # CUDA Toolkit 13.0.1 for Linux x86-64 patched with https://paste.yandex-team.ru/49e19ccf-43dd-400c-9cd5-8e51f4f2c71c
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:9759105406)
                # CUDA Toolkit 13.0 for Linux arm64-sbsa (target part only)
                DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:9747793149)
            ELSEIF (CUDA_VERSION == "12.6")
                # CUDA Toolkit 12.6.3 for Linux x86-64 patched with https://paste.yandex-team.ru/047d50ab-d9f7-4749-9cca-d2fb9800e1c7
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:7978027151)

                IF (JETSON)
                    # CUDA Toolkit 12.6.3 for Linux aarch64-jetson (target part only)
                    # Patched with https://a.yandex-team.ru/arcadia/devtools/contrib/docs/cuda_12.6.3.patch
                    DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:8149446824)
                ELSE()
                    # CUDA Toolkit 12.6.3 for Linux arm64-sbsa (target part only)
                    # Patched with https://a.yandex-team.ru/arcadia/devtools/contrib/docs/cuda_12.6.3.patch
                    DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:7661831621)
                ENDIF()
            ELSEIF (CUDA_HOST_VERSION == "11.8")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:4526354763) # CUDA Toolkit 11.8.0 for Linux x86-64

                IF (CUDA_VERSION == "11.4.19")
                    DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:7000367268) # CUDA Toolkit 11.4.19 for linux-aarch64 (target part only)
                ELSEIF (CUDA_VERSION == "10.2")
                    DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:6472409541) # CUDA Toolkit 10.2 for linux-aarch64 (target part only)
                ELSE()
                    ENABLE(CUDA_NOT_FOUND)
                ENDIF()
            ELSEIF (CUDA_VERSION == "11.4")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:2410761119) # CUDA Toolkit 11.4.2 for linux-x64_64 (host part only)
                DECLARE_EXTERNAL_RESOURCE(CUDA_TARGET sbr:3840142733) # CUDA Toolkit 11.4.2 for linux-aarch64 (target part only)
            ELSE()
                ENABLE(CUDA_NOT_FOUND)
            ENDIF()
        ELSE()
            ENABLE(CUDA_NOT_FOUND)
        ENDIF()

        IF (CUDA_VERSION VERSION_GE "13.0")
            IF (OS_SDK == "ubuntu-22" OR OS_SDK == "ubuntu-24" OR OS_SDK == "local")
            ELSE()
                MESSAGE(
                    FATAL_ERROR
                    CUDA $CUDA_VERSION " can be used only with Ubuntu 22.04 and above."
                    "Please add `-DOS_SDK=ubuntu-[22|24]` to specify the oldest target system, where you want to run your code"
                )
            ENDIF()
        ELSEIF (CUDA_VERSION VERSION_GE "12.0")
            IF (OS_SDK == "ubuntu-16" OR OS_SDK == "ubuntu-18" OR OS_SDK == "ubuntu-20" OR OS_SDK == "ubuntu-22" OR OS_SDK == "local")
            ELSE()
                MESSAGE(
                    FATAL_ERROR
                    CUDA $CUDA_VERSION " can be used only with Ubuntu 18.04 and above."
                    "Please add `-DOS_SDK=ubuntu-[16|18|20]` to specify the oldest target system, where you want to run your code"
                )
            ENDIF()
        ELSEIF (CUDA_VERSION VERSION_GE "11.8")
            IF (OS_SDK == "ubuntu-16" OR OS_SDK == "ubuntu-18" OR OS_SDK == "ubuntu-20" OR OS_SDK == "local")
            ELSE()
                MESSAGE(
                    FATAL_ERROR
                    CUDA $CUDA_VERSION " can be used only with Ubuntu 16.04 and above."
                    "Please add `-DOS_SDK=ubuntu-[16|18|20]` to specify the oldest target system, where you want to run your code"
                )
            ENDIF()
        ENDIF()

    ELSEIF (HOST_OS_WINDOWS AND HOST_ARCH_X86_64)
        # CUDA on Windows does not support cross-compilation,
        # hence there is no need to divide it into HOST and TARGET resources.
        IF (OS_WINDOWS AND ARCH_X86_64)
            IF (CUDA_VERSION == "11.4")
                DECLARE_EXTERNAL_RESOURCE(CUDA sbr:3866867639) # CUDA Toolkit 11.4.2 for windows-x86_64
            ELSE()
                ENABLE(CUDA_NOT_FOUND)
            ENDIF()
        ELSE()
            ENABLE(CUDA_NOT_FOUND)
        ENDIF()

    ELSE()
        ENABLE(CUDA_NOT_FOUND)
    ENDIF()
ENDIF()

IF (USE_ARCADIA_CUDA_HOST_COMPILER)
    IF (HOST_OS_WINDOWS AND HOST_ARCH_X86_64)
        IF (OS_WINDOWS AND ARCH_X86_64)
            # To create this toolchain, install MSVS on Windows and run:
            # devtools/tools_build/pack_sdk.py msvc out.tar
            # Note: it will contain patched "VC/Auxiliary/Build/vcvarsall.bat"
            # to prevent "nvcc fatal   : Host compiler targets unsupported OS."
            IF (CUDA_HOST_MSVC_VERSION == "14.28.29910")
                DECLARE_EXTERNAL_RESOURCE(CUDA_HOST_TOOLCHAIN sbr:2153212401)
            ELSE()
                MESSAGE(FATAL_ERROR "Unexpected or unspecified Microsoft Visual C++ CUDA host compiler version")
            ENDIF()

        ELSE()
            ENABLE(CUDA_HOST_COMPILER_NOT_FOUND)
        ENDIF()

    ELSEIF (OS_LINUX)
        IF (CUDA_VERSION VERSION_GE "12.6")
            DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(CUDA_HOST_TOOLCHAIN ${ARCADIA_ROOT}/build/platform/clang/clang20.json)
        ELSE()
            DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(CUDA_HOST_TOOLCHAIN ${ARCADIA_ROOT}/build/platform/clang/clang14.json)
        ENDIF()
    ELSE()
        ENABLE(CUDA_HOST_COMPILER_NOT_FOUND)
    ENDIF()
ENDIF()

IF (CUDA_NOT_FOUND)
    MESSAGE(FATAL_ERROR "No CUDA Toolkit for the selected platform")
ENDIF()

IF (CUDA_HOST_COMPILER_NOT_FOUND)
    MESSAGE(FATAL_ERROR "No CUDA host compiler for the selected platform and CUDA Toolkit version ${CUDA_VERSION}")
ENDIF()

# Use thrust and cub from Arcadia, not from HPC SDK
# NB:
#   it would be better to use PEERDIR instead,
#   but ymake does not allow PEERDIRs from RESOURCES_LIBRARY.
IF (CUDA_VERSION VERSION_GE "11.0" AND CUDA_VERSION VERSION_LE "11.9")
    ADDINCL(
        GLOBAL contrib/libs/tbb/include
        GLOBAL contrib/deprecated/nvidia/thrust
        GLOBAL contrib/deprecated/nvidia/cub
    )
ELSEIF (CUDA_VERSION VERSION_GE "12.0")
    ADDINCL(
        GLOBAL contrib/libs/tbb/include
        GLOBAL contrib/libs/nvidia/cccl/libcudacxx/include
        GLOBAL contrib/libs/nvidia/cccl/cub
        GLOBAL contrib/libs/nvidia/cccl/thrust
    )
ELSE()
    ADDINCL(
        # Use thrust from CUDA Toolkit
        GLOBAL contrib/libs/nvidia/cub-1.8.0
    )
ENDIF()

IF (OS_WINDOWS)
    # Not using CFLAGS / LDFLAGS on Windows, as these macros do not allow spaces in variables
    # (and paths containing spaces are quite common on Windows)
    SET_APPEND_WITH_GLOBAL(USER_CFLAGS
        GLOBAL "\"-I${CUDA_ROOT}/include\""
        GLOBAL "\"-I${CUDA_ROOT}/extras/CUPTI/include\""
    )
    SET_APPEND(LDFLAGS_GLOBAL
        "\"/LIBPATH:${CUDA_ROOT}/lib/x64\""
        "\"/LIBPATH:${CUDA_ROOT}/extras/CUPTI/lib64\""
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    CFLAGS(GLOBAL "-I${CUDA_TARGET_RESOURCE_GLOBAL}/include")
    LDFLAGS(
        "-L${CUDA_TARGET_RESOURCE_GLOBAL}/lib"
        "-L${CUDA_TARGET_RESOURCE_GLOBAL}/lib/stubs"
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    CFLAGS(
        GLOBAL "-I${CUDA_ROOT}/include"
        GLOBAL "-I${CUDA_ROOT}/extras/CUPTI/include"
    )
    LDFLAGS(
        "-L${CUDA_ROOT}/lib64"
        "-L${CUDA_ROOT}/lib64/stubs"
        "-L${CUDA_ROOT}/extras/CUPTI/lib64"
    )
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported target platform")
ENDIF()

# Some project in Arcadia do not build with CUDA 11.X because of linker error, related to relocation overflow
# Seems like one simple trick from https://maskray.me/blog/2023-05-14-relocation-overflow-and-code-models helps for most of those projects:
#
# """
# On x86-64, linkers optimize some GOT-indirect instructions (R_X86_64_REX_GOTPCRELX; e.g. movq var@GOTPCREL(%rip), %rax) to PC-relative instructions.
# The distance between a code section and .got is usually smaller than the distance between a code section and .data/.bss.
# ld.lld's one-pass relocation scanning scheme has a limitation:
# if it decides to suppress a GOT entry and it turns out that optimizing the instruction will lead to relocation overflow, the decision cannot be reverted.
# It should be easy to work around the issue with -Wl,--no-relax.
# """
IF (OS_LINUX AND CUDA_VERSION VERSION_GE 11.0)
    # Inspired by https://maskray.me/blog/2023-05-14-relocation-overflow-and-code-models
    LDFLAGS(-Wl,--no-relax)
ENDIF()

IF (HOST_OS_WINDOWS)
    LDFLAGS(cudadevrt.lib cudart_static.lib)
ELSE()
    EXTRALIBS(-lcudadevrt -lculibos)
    IF (USE_DYNAMIC_CUDA)
        EXTRALIBS(-lcudart)
    ELSE()
        EXTRALIBS(-lcudart_static)
    ENDIF()
ENDIF()

END()
