from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.SRCS.remove("src/tbb/itt_notify.cpp")
        m.CFLAGS.append("-D__TBB_DYNAMIC_LOAD_ENABLED=0")
        m.CFLAGS.remove("-D__TBB_USE_ITT_NOTIFY")
        m.after(
            "SRCS",
            Switch(
                {
                    "CLANG OR CLANG_CL": Switch(
                        {
                            "ARCH_I386 OR ARCH_I686 OR ARCH_X86_64": Linkable(CFLAGS=["-mrtm", "-mwaitpkg"]),
                        }
                    ),
                }
            ),
        )
        m.after(
            "SRCS",
            Switch(
                default=Linkable(CFLAGS=["-DUSE_PTHREAD"]),
                OS_WINDOWS=Linkable(CFLAGS=["-DUSE_WINTHREAD"]),
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    "GCC": Linkable(CFLAGS=["-flifetime-dse=1", "-mrtm"]),
                }
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    "NOT ARCH_ARM64": Linkable(
                        CFLAGS=["-D__TBB_USE_ITT_NOTIFY", "-DDO_ITT_NOTIFY"],
                        SRCS=["src/tbb/itt_notify.cpp"],
                    )
                }
            ),
        )


tbb = CMakeNinjaNixProject(
    license="Apache-2.0",
    arcdir="contrib/libs/tbb",
    nixattr="tbb",
    build_targets=["tbb"],
    addincl_global={".": {"./include"}},
    copy_sources=["include/"],
    disable_includes=[
        # if defined(__OpenBSD__) || __TBB_has_include(<sys/futex.h>)
        "sys/futex.h",
        "tbb_misc.h",
        "Softpub.h",
        "wintrust.h",
    ],
    post_install=post_install,
)
