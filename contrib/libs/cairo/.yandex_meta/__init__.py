from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import MesonNixProject


def post_install(self):
    with self.yamakes["."] as cairo:
        cairo.PEERDIR.remove("contrib/libs/fontconfig")
        cairo.before(
            "SRCS",
            Switch(
                default=Linkable(PEERDIR=["contrib/libs/fontconfig"]),
                OS_WINDOWS=Linkable(CFLAGS=[GLOBAL("-DCAIRO_WIN32_STATIC_BUILD")]),
            ),
        )


cairo = MesonNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/cairo",
    nixattr="cairo",
    platform_dispatchers=[
        "config.h",
    ],
    inclink={"src": ["config.h"]},
    install_targets=["cairo", "cairo-gobject"],
    addincl_global={".": ["contrib/libs/fontconfig/include"]},
    post_install=post_install,
)
