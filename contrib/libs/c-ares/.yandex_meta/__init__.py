import os

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    # make ares_build.h includeable from c-ares headers
    os.rename(
        f"{self.dstdir}/ares_build.h",
        f"{self.dstdir}/include/ares_build.h",
    )
    # make ares_config.h includeable from c-ares sources
    os.rename(
        f"{self.dstdir}/ares_config.h",
        f"{self.dstdir}/src/lib/ares_config.h",
    )


def post_install(self):
    with self.yamakes["."] as c_ares:
        # remove autodetected ADDINCL for moved headers above
        c_ares.ADDINCL.remove(self.arcdir)

        c_ares.PEERDIR.add("contrib/libs/libc_compat")
        c_ares.SRCS.add("src/lib/atomic.cpp")
        c_ares.before(
            "SRCS",
            Switch({"NOT EXPORT_CMAKE": "CHECK_CONFIG_H(src/lib/ares_setup.h)"}),
        )

        c_ares.after(
            "CFLAGS",
            Switch(
                ARCH_ARM7=Linkable(CFLAGS=[GLOBAL("-D__SIZEOF_LONG__=4")]),
            ),
        )

        c_ares.after(
            "CFLAGS",
            Switch({"OS_DARWIN OR OS_IOS": Linkable(LDFLAGS=["-lresolv"])}),
        )

        c_ares.CFLAGS.append(GLOBAL("-DCARES_STATICLIB"))

        # atomic.cpp depends on util (IGNIETFERRO-1491)
        c_ares.NO_RUNTIME = False
        c_ares.NO_UTIL = False


c_ares = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/c-ares",
    nixattr="c-ares",
    copy_sources=[
        "src/lib/ares_android.h",
        "src/lib/ares_iphlpapi.h",
        "src/lib/ares_writev.h",
        "src/lib/config-win32.h",
        "src/lib/thirdparty/apple",
    ],
    # Keep arcadia files.
    keep_paths=[
        "src/lib/atomic.cpp",
        "src/lib/atomic.h",
    ],
    addincl_global={
        ".": {"./include"},
    },
    install_targets={
        "cares",
    },
    put={
        "cares": ".",
    },
    platform_dispatchers=[
        "include/ares_build.h",
        "src/lib/ares_config.h",
    ],
    post_build=post_build,
    post_install=post_install,
)
