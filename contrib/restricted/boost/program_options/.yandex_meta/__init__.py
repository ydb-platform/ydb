from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)
    with self.yamakes["."] as program_options:
        program_options.after(
            "CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_PROGRAM_OPTIONS_DYN_LINK")])})
        )


boost_program_options = NixSourceProject(
    nixattr="boost_program_options",
    arcdir=boost.make_arcdir("program_options"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
