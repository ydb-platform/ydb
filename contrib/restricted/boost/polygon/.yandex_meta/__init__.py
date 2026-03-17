from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_polygon = NixSourceProject(
    nixattr="boost_polygon",
    arcdir=boost.make_arcdir("polygon"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
