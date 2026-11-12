from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.platform_macros import LLVM_VERSION
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as flang_rt:
        # Upstream derives FLANG_RT_SUPPORTS_REAL16 by probing the Fortran compiler,
        # which is skipped in our build (see flags). flang only enables REAL(16)
        # when it is built with libquadmath or on hosts with 128-bit long double,
        # so for the flang toolchain used by Arcadia it is linux-aarch64 only.
        flang_rt.CFLAGS.remove("-DFLANG_RT_SUPPORTS_REAL16=0")
        flang_rt.after(
            "CFLAGS",
            Switch(
                {
                    "OS_LINUX AND ARCH_AARCH64": Linkable(CFLAGS=["-DFLANG_RT_SUPPORTS_REAL16=1"]),
                    "default": Linkable(CFLAGS=["-DFLANG_RT_SUPPORTS_REAL16=0"]),
                }
            ),
        )
        flang_rt.before(
            "END",
            """
            IF (OS_WINDOWS)
                LDFLAGS(
                    /LIBPATH:${ARCADIA_ROOT}/build/essentials/flang-empty-runtimes
                )
            ENDIF()
            """,
        )


llvm_flang_rt = CMakeNinjaNixProject(
    nixattr=f"llvmPackages_{LLVM_VERSION}.compiler-rt",
    arcdir="contrib/libs/llvm/flang-rt",
    cmake_subdir="runtimes",
    build_subdir="runtimes/build",
    flags=[
        "-DLLVM_ENABLE_RUNTIMES=flang-rt",
        # Fortran language is disabled via disable-fortran.patch, so the upstream
        # check_fortran_quadmath_support() introspection can not run.
        # Predefine the answer; the actual per-platform value is set in post_install.
        "-DFORTRAN_SUPPORTS_REAL16=OFF",
    ],
    copy_sources=[
        "flang/include/flang/Common/windows-include.h",
    ],
    use_full_libnames=True,
    install_targets=[
        "libflang_rt.runtime",
    ],
    disable_includes=[
        "stdfloat",
        "cuda/",
        # clang GPU resource header, only reachable from NVPTX / AMDGPU / SPIRV builds
        "gpuintrin.h",
    ],
    platform_dispatchers=[
        "flang-rt/config.h",
    ],
    post_install=post_install,
)
