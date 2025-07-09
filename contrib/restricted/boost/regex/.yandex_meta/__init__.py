from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        populate_srcs=True,
        CFLAGS=[
            "-DBOOST_HAS_ICU",
            "-DBOOST_NO_CXX98_BINDERS",
            "-DBOOST_REGEX_STANDALONE",
        ],
        PEERDIR=[
            "contrib/libs/icu",
        ],
    )
    with self.yamakes["."] as regex:
        regex.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_REGEX_DYN_LINK")])}))
        # default on other platforms, on windows uses native otherwise
        regex.after("CFLAGS", Switch({"OS_WINDOWS": Linkable(CFLAGS=[GLOBAL("-DBOOST_REGEX_USE_CPP_LOCALE")])}))


boost_regex = NixSourceProject(
    nixattr="boost_regex",
    arcdir=boost.make_arcdir("regex"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        # Copy evertthing except for boost/regex/v4
        "include/boost/*.hpp",
        "include/boost/regex.h",
        "include/boost/regex/*.hpp",
        "include/boost/regex/v5",
        "include/boost/regex/config",
        "include/boost/regex/pending",
        "src/",
    ],
    disable_includes=[
        "BOOST_REGEX_H1",
        "BOOST_REGEX_H2",
        "BOOST_REGEX_H3",
        "boost/core/",
        "boost/regex/v4/",
        "boost/static_assert.hpp",
    ],
    post_install=post_install,
)
