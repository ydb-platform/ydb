from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_algorithm = NixSourceProject(
    nixattr="boost_algorithm",
    arcdir=boost.make_arcdir("algorithm"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "slist",
    ],
    post_install=post_install,
)
