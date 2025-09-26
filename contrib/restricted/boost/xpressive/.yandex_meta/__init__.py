from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_xpressive = NixSourceProject(
    nixattr="boost_xpressive",
    arcdir=boost.make_arcdir("xpressive"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        # from old boost versions
        "boost/spirit/fusion/*",
    ],
    post_install=post_install,
)
