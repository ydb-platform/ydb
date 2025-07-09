from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_multi_index = NixSourceProject(
    nixattr="boost_multi_index",
    arcdir=boost.make_arcdir("multi_index"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "BOOST_MULTI_INDEX_BLOCK_BOOSTDEP_HEADER",
    ],
    post_install=post_install,
)
