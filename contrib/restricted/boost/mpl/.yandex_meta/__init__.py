from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_mpl = NixSourceProject(
    nixattr="boost_mpl",
    arcdir=boost.make_arcdir("mpl"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/**",
    ],
    copy_sources_except=[
        "include/boost/mpl/aux_/preprocessed/bcc**",
        "include/boost/mpl/aux_/preprocessed/dmc/**",
        "include/boost/mpl/aux_/preprocessed/msvc**",
        "include/boost/mpl/aux_/preprocessed/mwcw/**",
        "include/boost/mpl/aux_/preprocessed/no_ctps/**",
        "include/boost/mpl/aux_/preprocessed/no_ttp/**",
        "include/boost/mpl/map/aux_/preprocessed/no_ctps/**",
        "include/boost/mpl/map/aux_/preprocessed/plain/**",
        "include/boost/mpl/vector/aux_/preprocessed/no_ctps/**",
        "include/boost/mpl/vector/aux_/preprocessed/plain/**",
    ],
    disable_includes=[
        # if BOOST_WORKAROUND(__IBMCPP__, BOOST_TESTED_AT(700))
        "AUX778076_INCLUDE_STRING",
    ],
    post_install=post_install,
)
