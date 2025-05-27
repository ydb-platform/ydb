from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as zlib:
        zlib.CFLAGS.remove("-DHAVE_HIDDEN")
        zlib.after("CFLAGS", Switch({"NOT MSVC": Linkable(CFLAGS=["-DHAVE_HIDDEN"])}))


zlib = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/zlib",
    nixattr="zlib",
    makeflags=["libz.a"],
    inclink={"include": ["zconf.h", "zlib.h"]},
    addincl_global={
        ".": {"./include"},
    },
    disable_includes={
        # if defined(VMS)
        "unixio.h",
    },
    post_install=post_install,
)
