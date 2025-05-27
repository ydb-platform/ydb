from devtools.yamaker import fileutil

from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import NixProject


def post_build(self):
    fileutil.convert_to_utf8(f"{self.dstdir}/AUTHORS", from_="latin1")


def post_install(self):
    with self.yamakes["."] as lcms:
        # reentrable variant of gmtime is gmtime_r on Unix, gmtime_s on Windows
        lcms.CFLAGS.remove("-DHAVE_GMTIME_R=1")
        lcms.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(CFLAGS=["-DHAVE_GMTIME_S=1"]),
                default=Linkable(CFLAGS=["-DHAVE_GMTIME_R=1"]),
            ),
        )


lcms2 = NixProject(
    owners=["shindo", "g:mds", "g:cpp-contrib"],
    arcdir="contrib/libs/lcms2",
    nixattr="lcms2",
    post_build=post_build,
    post_install=post_install,
)
