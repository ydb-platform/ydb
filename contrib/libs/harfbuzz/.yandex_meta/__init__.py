from devtools.yamaker.modules import Library, Recursable, Switch
from devtools.yamaker.project import MesonNixProject


def post_install(self):
    # Split LGPL-dependent part.
    self.yamakes["glib"] = self.module(
        Library,
        PEERDIR=[
            self.arcdir,
            "contrib/restricted/glib",
        ],
        CFLAGS=self.yamakes["."].CFLAGS,
        SRCDIR=[self.arcdir],
        SRCS=["src/hb-glib.cc"],
        NO_UTIL=True,
        NO_COMPILER_WARNINGS=True,
    )

    with self.yamakes["."] as hb:
        hb.SRCS.remove("src/hb-glib.cc")
        hb.PEERDIR.remove("contrib/restricted/glib")
        hb.PEERDIR.remove("contrib/restricted/glib/include")
        hb.after("END", Switch({"NOT OS_IOS AND NOT OS_ANDROID": Recursable(RECURSE=["glib"])}))


harfbuzz = MesonNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/harfbuzz",
    nixattr="harfbuzz",
    platform_dispatchers=["config.h"],
    copy_sources=["src/*.hh", "src/*.h"],
    inclink={
        "include": [
            "src/*.h",
            "src/hb-cplusplus.hh",
        ],
        "src": ["config.h"],
    },
    disable_includes=[
        "atomic.h",
        "config-override.h",
        "dwrite_3.h",
        "mbarrier.h",
        "unicode/*.h",
        "HB_CONFIG_OVERRIDE_H",
    ],
    install_targets=["harfbuzz", "harfbuzz-subset"],
    put_with={
        "harfbuzz": ["harfbuzz-subset"],
    },
    post_install=post_install,
)
