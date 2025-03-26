from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker.modules import Linkable, Switch


def post_install(self):
    with self.yamakes["."] as libunwind:
        libunwind.NO_COMPILER_WARNINGS = False
        libunwind.NO_LTO = True
        libunwind.NO_RUNTIME = True
        libunwind.NO_SANITIZE = True
        libunwind.NO_SANITIZE_COVERAGE = True
        # There should be a clang option to disable pragma comment(lib) completely.
        # Having these defines breaks musl build, as there is no such libs in musl
        libunwind.CFLAGS.remove("-D_LIBUNWIND_LINK_DL_LIB")
        libunwind.CFLAGS.remove("-D_LIBUNWIND_LINK_PTHREAD_LIB")

        # original build uses -f options heavily, keep only necessary subset
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
                            "-D__WASM_EXCEPTIONS__",
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
        "commpage_defs.h",
        "sys/debug.h",
        "sys/pseg.h",
        "System/pthread_machdep.h",
    ],
    post_install=post_install,
)
