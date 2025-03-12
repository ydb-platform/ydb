from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)
    with self.yamakes["."] as iostreams:
        iostreams.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_DATE_TIME_DYN_LINK")])}))


boost_date_time = NixSourceProject(
    nixattr="boost_date_time",
    arcdir=boost.make_arcdir("date_time"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    copy_sources_except=[
        # doxygen helpers
        "gregorian/gregorian_types.cpp",
        "posix_time/posix_time_types.cpp",
        "date_time.doc",
    ],
    post_install=post_install,
)
