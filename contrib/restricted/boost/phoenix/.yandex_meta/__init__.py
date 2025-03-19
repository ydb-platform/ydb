from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_phoenix = NixSourceProject(
    nixattr="boost_phoenix",
    arcdir="contrib/restricted/boost/phoenix",
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "BOOST_PHOENIX_HASH_MAP_HEADER",
        "BOOST_PHOENIX_HASH_SET_HEADER",
    ],
    post_install=post_install,
)
