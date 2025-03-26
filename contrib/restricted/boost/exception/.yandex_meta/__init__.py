from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)


boost_exception = NixSourceProject(
    nixattr="boost_exception",
    arcdir=boost.make_arcdir("exception"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
