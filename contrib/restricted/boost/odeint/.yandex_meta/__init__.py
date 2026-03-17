from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        optional_deps=("compute", "mpi"),
    )


boost_odeint = NixSourceProject(
    nixattr="boost_odeint",
    arcdir=boost.make_arcdir("odeint"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    copy_sources_except=[
        "numeric/odeint/external",
    ],
    post_install=post_install,
)
