from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker.modules import Linkable, Switch


def post_install(self):
    with self.yamakes["."] as libunwind:
        libunwind.DISABLE.add("USE_LTO")
        libunwind.NO_COMPILER_WARNINGS = False
        libunwind.NO_RUNTIME = True
        libunwind.NO_SANITIZE = True
        libunwind.NO_SANITIZE_COVERAGE = True
        libunwind.ADDINCL = [f"{self.arcdir}/include"]
        libunwind.CFLAGS += ["-fno-exceptions", "-fno-rtti", "-funwind-tables"]
        libunwind.after("CFLAGS", Switch({"SANITIZER_TYPE == memory": "CFLAGS(-fPIC)"}))
        libunwind.PEERDIR.add("library/cpp/sanitizer/include")
        libunwind.SRCS.add("src/UnwindRegistersSave.S")
        libunwind.SRCS.add("src/UnwindRegistersRestore.S")

        sources = libunwind.SRCS
        libunwind.SRCS = []

        libunwind.after(
            "SRCS",
            Switch(
                {
                    "NOT OS_EMSCRIPTEN": Linkable(
                        SRCS=sources,
                        CFLAGS=["-D_LIBUNWIND_IS_NATIVE_ONLY"],
                    ),
                    "OS_EMSCRIPTEN AND ARCH_WASM32": Linkable(
                        SRCS=["src/Unwind-wasm.c"],
                        PEERDIR=["contrib/restricted/emscripten/include"],
                        CFLAGS=[
                            "-D_LIBUNWIND_HIDE_SYMBOLS",
                        ],
                    ),
                    "OS_EMSCRIPTEN AND NOT ARCH_WASM32": Linkable(
                        SRCS=["src/Unwind-wasm.c"],
                        PEERDIR=["contrib/restricted/emscripten/include"],
                        CFLAGS=[
                            "-D_LIBUNWIND_HIDE_SYMBOLS",
                            "-D__USING_WASM_EXCEPTIONS__",
                        ],
                    ),
                }
            ),
        )


llvm_libunwind = CMakeNinjaNixProject(
    owners=["g:cpp-contrib", "g:cpp-committee"],
    arcdir="contrib/libs/libunwind",
    nixattr="llvmPackages_latest.libunwind",
    copy_sources=[
        "include/unwind_arm_ehabi.h",
        "include/unwind_unwind_itanium.h",
        "src/assembly.h",
        "src/FrameHeaderCache.hpp",
        "src/UnwindRegistersSave.S",
        "src/UnwindRegistersRestore.S",
    ],
    disable_includes=[
        "sys/debug.h",
        "sys/pseg.h",
        "System/pthread_machdep.h",
    ],
    post_install=post_install,
)
