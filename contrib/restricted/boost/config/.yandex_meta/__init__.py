from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        CFLAGS=[
            GLOBAL("-DBOOST_ALL_NO_LIB"),
        ],
    )


boost_config = NixSourceProject(
    nixattr="boost_config",
    arcdir=boost.make_arcdir("config"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/**/*.hpp",
    ],
    copy_sources_except=[
        "boost/config/platform/vxworks.hpp",
    ],
    disable_includes=[
        "boost/config/platform/vxworks.hpp",
    ],
    post_install=post_install,
)
