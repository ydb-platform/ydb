from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_numeric_conversion = NixSourceProject(
    nixattr="boost_numeric_conversion",
    arcdir=boost.make_arcdir("numeric_conversion"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
