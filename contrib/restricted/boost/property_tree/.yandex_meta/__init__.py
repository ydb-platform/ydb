from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_property_tree = NixSourceProject(
    nixattr="boost_property_tree",
    arcdir=boost.make_arcdir("property_tree"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
