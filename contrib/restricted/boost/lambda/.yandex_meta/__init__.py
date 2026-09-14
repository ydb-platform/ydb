from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_lambda = NixSourceProject(
    nixattr="boost_lambda",
    arcdir=boost.make_arcdir("lambda"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
