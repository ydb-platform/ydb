import shutil

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    shutil.copy(
        self.builddir + "/runtime/src/omp.h", self.dstdir
    )  # This file is patched during installation so we need to copy it here


def post_install(self):
    with self.yamakes["."] as openmp:
        openmp.NO_LTO = True
        openmp.ADDINCL = [
            GLOBAL(self.arcdir),
        ]
        openmp.CFLAGS = [
            "-fno-exceptions",
            "-DKMP_USE_MONITOR=1",  # DTCC-842
        ]
        openmp.before(
            "SRCS",
            Switch({"SANITIZER_TYPE == thread": Linkable(NO_SANITIZE=True, CFLAGS=["-fPIC"])}),
        )
        openmp.before(
            "SRCS",
            Switch({"SANITIZER_TYPE == memory": Linkable(NO_SANITIZE=True, CFLAGS=["-fPIC"])}),
        )
        openmp.before(
            "SRCS",
            """
            # The KMP_DEBUG define enables OpenMP debugging support, including tracing (controlled by environment variables)
            # and debug asserts. The upstream version unconditionally enables KMP_DEBUG for Debug/RelWithDebInfo builds.
            # Instead, we make this opt-in via a `ymake` variable to avoid accidentally releasing a relwithdebinfo binary
            # with KMP_DEBUG enabled. Note that the `ymake` variable is called OPENMP_DEBUG for clarity, since no one
            # really knows what KMP is.
            """,
        )
        openmp.before(
            "SRCS",
            Switch(
                {
                    "OPENMP_DEBUG": Linkable(
                        CFLAGS=["-DKMP_DEBUG=1"],
                    )
                }
            ),
        )


llvm_openmp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib", "g:cpp-committee"],
    arcdir="contrib/libs/cxxsupp/openmp",
    nixattr="llvmPackages_13.openmp",
    install_subdir="runtime/src",
    ignore_commands=["python3.10"],
    flags=[
        "-DOPENMP_ENABLE_LIBOMPTARGET=OFF",
        "-DOPENMP_ENABLE_LIBOMP_PROFILING=OFF",
        "-DOPENMP_ENABLE_OMPT_TOOLS=OFF",
        "-DLIBOMP_OMPD_SUPPORT=OFF",  # Disable OMPD support as it breaks build under MSAN
        "-DLIBOMP_USE_ITT_NOTIFY=OFF",
        "-DLIBOMP_USE_VERSION_SYMBOLS=OFF",
    ],
    disable_includes=[
        "ittnotify.h",
        "ittnotify_config.h",
        "kmp_debugger.h",
        "kmp_dispatch_hier.h",
        "kmp_itt.inl",
        "kmp_stats_timing.h",
        "kmp_stub.h",
        "legacy/ittnotify.h",
        "libperfstat.h",
        "llvm/Support/TimeProfiler.h",
        "ompd-specific.h",
        "procfs.h",
    ],
    platform_dispatchers=["kmp_config.h"],
    post_build=post_build,
    post_install=post_install,
)
