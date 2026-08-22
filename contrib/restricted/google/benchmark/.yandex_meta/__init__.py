from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Py3Program, Recursable, Recurse, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    fileutil.re_sub_dir(f"{self.dstdir}/test", "TEST_BENCHMARK_LIBRARY_HAS_NO_ASSERTIONS", "NDEBUG")
    fileutil.copy([f"{self.srcdir}/tools/compare.py", f"{self.srcdir}/tools/gbench"], f"{self.dstdir}/tools/compare")

    with self.yamakes["."] as benchmark:
        idx = benchmark.CFLAGS.index("-DBENCHMARK_STATIC_DEFINE")
        benchmark.CFLAGS[idx] = GLOBAL("-DBENCHMARK_STATIC_DEFINE")

        benchmark.CFLAGS.remove("-DBENCHMARK_HAS_PTHREAD_AFFINITY")
        benchmark.after(
            "CFLAGS",
            Switch(
                OS_LINUX=Linkable(CFLAGS=["-DBENCHMARK_HAS_PTHREAD_AFFINITY"]),
            ),
        )
        benchmark.RECURSE.add("tools")

    with self.yamakes["test"] as benchmark_gtest:
        benchmark_gtest.module = "GTEST"
        # fmt: off
        benchmark_gtest.PEERDIR = [
            peerdir
            for peerdir in benchmark_gtest.PEERDIR
            if not peerdir.startswith("contrib/restricted/googletest")
        ]
        # fmt: on

    self.yamakes["tools"] = Recurse()
    with self.yamakes["tools"] as tools:
        # google/benchmark contrib is built for wide range of platforms by devtools checks
        # but python/numpy/scipy are supported only for subset of platforms.
        # So here we have such complex condition to enable recurse only for supported platforms.
        tools.after(
            "RECURSE",
            Switch(
                {
                    "NOT USE_STL_SYSTEM": Switch(
                        {
                            "NOT USE_SYSTEM_PYTHON OR NOT _SYSTEM_PYTHON27": Switch(
                                {
                                    "OS_LINIX OR OS_DARWIN OR OS_WINDOWS": Switch(
                                        {"ARCH_X86_64 OR ARCH_ARM64": Recursable(RECURSE={"compare"})}
                                    )
                                }
                            )
                        }
                    )
                }
            ),
        )

    self.yamakes["tools/compare"] = self.module(
        Py3Program,
        LICENSE=[self.license],
        PY_SRCS=[
            "TOP_LEVEL",
            "compare.py",
            "gbench/__init__.py",
            "gbench/report.py",
            "gbench/util.py",
        ],
        PY_MAIN="compare",
        PEERDIR=["contrib/python/numpy", "contrib/python/scipy"],
        NO_LINT=True,
    )


benchmark = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/google/benchmark",
    license="Apache-2.0",
    nixattr="gbenchmark",
    addincl_global={".": {"./include"}},
    disable_includes=[
        "kstat.h",
        "qurt.h",
        "perfmon/",
        "sys/syspage.h",
    ],
    flags=[
        # Adds -DBENCHMARK_STATIC_DEFINE preprocessor flag which fixed MSVC build
        "-DBUILD_SHARED_LIBS=OFF",
        # Otherwise the tests do not use assert.
        "-DCMAKE_BUILD_TYPE=Debug",
        "-DBENCHMARK_HAS_CXX03_FLAG=NO",
        "-DBENCHMARK_USE_BUNDLED_GTEST=OFF",
    ],
    install_targets={
        "benchmark",
        "benchmark_gtest",
        "benchmark_name_gtest",
        "commandlineflags_gtest",
        "statistics_gtest",
        "string_util_gtest",
    },
    put={
        "benchmark": ".",
        "benchmark_gtest": "test",
    },
    put_with={
        "benchmark_gtest": [
            "benchmark_name_gtest",
            "commandlineflags_gtest",
            "statistics_gtest",
            "string_util_gtest",
        ]
    },
    post_install=post_install,
)
