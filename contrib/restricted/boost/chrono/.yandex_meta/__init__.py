from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)
    with self.yamakes["."] as chrono:
        chrono.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_CHRONO_DYN_LINK")])}))


boost_chrono = NixSourceProject(
    nixattr="boost_chrono",
    arcdir=boost.make_arcdir("chrono"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
