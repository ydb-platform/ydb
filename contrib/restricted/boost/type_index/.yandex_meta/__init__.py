from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_type_index = NixSourceProject(
    nixattr="boost_type_index",
    arcdir=boost.make_arcdir("type_index"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "BOOST_TYPE_INDEX_USER_TYPEINDEX",
    ],
    post_install=post_install,
)
