from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.platform_macros import LLVM_VERSION
from devtools.yamaker.project import CMakeNinjaNixProject


# as of 15.0.x, libfuzzer makes use of
#
# ATTRIBUTE_NO_SANITIZE_ALL
# size_t ForEachNonZeroByte()
#
# which invokes undefined behavior,
# which is not disabled by ATTRIBUTE_NO_SANITIZE_ALL
S1 = """
IF (SANITIZER_TYPE == "undefined")
    NO_SANITIZE()
ENDIF()
"""


def post_install(self):
    with self.yamakes["."] as m:
        m.NO_SANITIZE_COVERAGE = True
        m.after("CFLAGS", S1)
        m.before(
            "NO_SANITIZE_COVERAGE",
            Switch(
                {
                    'SANITIZE_COVERAGE MATCHES "trace-pc"': 'MESSAGE(FATAL_ERROR "I will crash you with trace-pc or trace-pc-guard. Use inline-8bit-counters.")'
                }
            ),
        )
        m.SET.append(["SANITIZER_CFLAGS"])
        m.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(SRCS=["lib/fuzzer/standalone/StandaloneFuzzTargetMain.c"]),
                default=Linkable(SRCS=m.SRCS),
            ),
        )
        m.SRCS = []
        m.PEERDIR.add("library/cpp/sanitizer/include")

    with self.yamakes["lib/fuzzer/afl"] as m:
        m.NO_SANITIZE = True
        m.PEERDIR = ["contrib/libs/afl/llvm_mode"]


llvm_libfuzzer = CMakeNinjaNixProject(
    nixattr=f"llvmPackages_{LLVM_VERSION}.compiler-rt",
    arcdir="contrib/libs/libfuzzer",
    copy_sources=[
        "include/fuzzer/FuzzedDataProvider.h",
        "lib/fuzzer/standalone/StandaloneFuzzTargetMain.c",
    ],
    install_targets=[
        "clang_rt.fuzzer-x86_64",
        "fuzzer-afl",
    ],
    put={
        "clang_rt.fuzzer-x86_64": ".",
        "fuzzer-afl": "lib/fuzzer/afl",
    },
    post_install=post_install,
)
