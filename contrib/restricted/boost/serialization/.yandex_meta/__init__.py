from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)
    with self.yamakes["."] as serialization:
        serialization.after(
            "CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_SERIALIZATION_DYN_LINK")])})
        )


boost_serialization = NixSourceProject(
    nixattr="boost_serialization",
    arcdir=boost.make_arcdir("serialization"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src",
    ],
    disable_includes=[
        "BOOST_SLIST_HEADER",
    ],
    post_install=post_install,
)
