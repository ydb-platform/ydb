from devtools.yamaker.platform_macros import LLVM_VERSION
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as flang_rt:
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
    ],
    copy_sources=[
        "flang/include/flang/Common/windows-include.h",
    ],
    disable_includes=[
        "stdfloat",
        "cuda/",
    ],
    platform_dispatchers=[
        "flang-rt/config.h",
    ],
    post_install=post_install,
)
