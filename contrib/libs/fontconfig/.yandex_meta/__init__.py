from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.before("SRCS", "CHECK_CONFIG_H(config.h)")


fontconfig = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/fontconfig",
    nixattr="fontconfig",
    platform_dispatchers=["config.h"],
    copy_sources=["src/fcwindows.h"],
    disable_includes=[
        "atomic.h",
        "mbarrier.h",
        "libxml/*.h",
        "xmlparse.h",
    ],
    ignore_commands=[
        "sh",
        "bash",
        "gperf",
        # Do not generate fc-case / fc-lang contents during build,
        # use nixpkgs generated one instead
        "python",
    ],
    install_targets=["fontconfig"],
    inclink={
        "include/fontconfig/fcfreetype.h": "fontconfig/fcfreetype.h",
        "include/fontconfig/fcprivate.h": "fontconfig/fcprivate.h",
        "include/fontconfig/fontconfig.h": "fontconfig/fontconfig.h",
    },
    post_install=post_install,
)

fontconfig.copy_top_sources_except.add("ABOUT-NLS")
