from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_smart_ptr = NixSourceProject(
    nixattr="boost_smart_ptr",
    arcdir=boost.make_arcdir("smart_ptr"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
