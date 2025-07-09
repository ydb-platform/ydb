from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_format = NixSourceProject(
    nixattr="boost_format",
    arcdir=boost.make_arcdir("format"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "streambuf.h",
    ],
    post_install=post_install,
)
