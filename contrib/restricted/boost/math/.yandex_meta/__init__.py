from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_math = NixSourceProject(
    nixattr="boost_math",
    arcdir=boost.make_arcdir("math"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "BOOST_MATH_LOGGER_INCLUDE",
        "stdfloat",
    ],
    # exclude tr1/c99 functions to keep this header-only
    copy_sources_except=["math/tr1*"],
    post_install=post_install,
)
