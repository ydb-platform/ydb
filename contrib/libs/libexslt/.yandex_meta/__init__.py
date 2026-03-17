from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def post_build(self):
    fileutil.convert_to_utf8(f"{self.dstdir}/NEWS", from_="latin1")


def post_install(self):
    with self.yamakes["."] as m:
        m.NO_UTIL = True
        m.PEERDIR.remove("contrib/libs/libiconv")
        m.after(
            "CFLAGS",
            Switch(OS_WINDOWS=Linkable(CFLAGS=[GLOBAL("-DLIBEXSLT_STATIC")])),
        )


libexslt = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libexslt",
    nixattr="libxslt",
    install_targets=["exslt"],
    unbundle_from={"xslt": "libxslt"},
    copy_sources=[
        "Copyright",
    ],
    addincl_global={".": ["."]},
    platform_dispatchers=["config.h"],
    copy_top_sources_except=[
        # COPYING is a symlink to Copyright
        "COPYING",
    ],
    # Добавляет специфичный для libexslt инклюд
    use_provides=["contrib/libs/libexslt/.yandex_meta"],
    post_build=post_build,
    post_install=post_install,
)
