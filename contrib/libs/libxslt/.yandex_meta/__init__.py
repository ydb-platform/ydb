from devtools.yamaker import fileutil
from devtools.yamaker.project import GNUMakeNixProject
from devtools.yamaker.modules import GLOBAL, Switch, Linkable


def post_build(self):
    fileutil.convert_to_utf8(f"{self.dstdir}/NEWS", from_="latin1")


def post_install(self):
    with self.yamakes["."] as m:
        m.PEERDIR.remove("contrib/libs/libiconv")
        m.after(
            "CFLAGS",
            Switch(OS_WINDOWS=Linkable(CFLAGS=[GLOBAL("-DLIBXSLT_STATIC")])),
        )


libxslt = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libxslt",
    nixattr="libxslt",
    install_targets=["xslt"],
    copy_sources=[
        "Copyright",
        "libxslt/trio.h",
        "libxslt/triodef.h",
        "libxslt/win32config.h",
    ],
    addincl_global={".": ["."]},
    copy_top_sources_except=[
        # COPYING is a symlink to Copyright
        "COPYING",
    ],
    platform_dispatchers=["config.h"],
    post_build=post_build,
    post_install=post_install,
)
