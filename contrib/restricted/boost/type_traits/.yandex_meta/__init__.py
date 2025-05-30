from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_type_traits = NixSourceProject(
    nixattr="boost_type_traits",
    arcdir=boost.make_arcdir("type_traits"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        # if defined(BOOST_TT_PREPROCESSING_MODE)
        "PP1",
        "PP2",
        "PP3",
        "PPI",
        "BOOST_PP_ITERATE()",
        "stdfloat",
    ],
    post_install=post_install,
)
