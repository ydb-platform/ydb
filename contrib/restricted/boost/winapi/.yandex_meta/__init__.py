from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)
    with self.yamakes["."] as winapi:
        winapi.after("CFLAGS", Switch({"OS_WINDOWS": Linkable(CFLAGS=[GLOBAL("-DBOOST_USE_WINDOWS_H")])}))


boost_winapi = NixSourceProject(
    nixattr="boost_winapi",
    arcdir=boost.make_arcdir("winapi"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "_cygwin.h",
    ],
    post_install=post_install,
)
