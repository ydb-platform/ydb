from devtools.yamaker.modules import GLOBAL, Switch, Linkable
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # These flags are already present in config.h (which is a proper platform_dispatcher)
        m.CFLAGS = [flag for flag in m.CFLAGS if not flag.startswith("-DHAVE")]

        m.CFLAGS.insert(0, GLOBAL("-DLIBDE265_STATIC_BUILD"))

        # This line is needed in order to fix resolution of
        # #include <libde265/de265-version.h>
        m.ADDINCL.insert(0, "contrib/libs/libde265/include")
        m.ADDINCL.sorted = False

        x86_srcs = [src for src in m.SRCS if src.startswith("libde265/x86")]
        m.SRCS = set(m.SRCS) - set(x86_srcs)
        m.after(
            "SRCS",
            Switch(
                ARCH_X86_64=Linkable(
                    ADDINCL=[f"{self.arcdir}/libde265/x86"],
                    SRCS=x86_srcs,
                ),
            ),
        )

        m.after("SRCS", Switch(OS_WINDOWS=Linkable(SRCS=["extra/win32cond.c"])))


libde265 = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libde265",
    nixattr="libde265",
    use_full_libnames=True,
    install_targets=[
        # NB:
        #   the actual target name is liblibde265.so,
        #   hence the lib prefix is present even without use_full_libnames=True
        "libde265",
    ],
    copy_sources=[
        "extra/win32cond.*",
    ],
    disable_includes=[
        "arm/arm.h",
        "libvideogfx.hh",
        "openssl/md5.h",
        "sdl.hh",
    ],
    platform_dispatchers=["config.h"],
    inclink={
        "include/libde265": [
            "libde265/de265.h",
            "libde265/de265-version.h",
        ],
        "libde265/encoder": ["libde265/util.h"],
    },
    post_install=post_install,
)
