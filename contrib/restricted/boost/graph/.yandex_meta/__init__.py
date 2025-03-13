from devtools.yamaker import boost
from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)
    with self.yamakes["."] as graph:
        # Disable parallel version which makes use of OpenMP / MPI
        fileutil.re_sub_dir(
            f"{self.dstdir}/include",
            r"#include BOOST_GRAPH_MPI_INCLUDE\(.*\)",
            "",
        )

        graph.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_GRAPH_DYN_LINK")])}))


boost_graph = NixSourceProject(
    nixattr="boost_graph",
    arcdir=boost.make_arcdir("graph"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/**/*.hpp",
        "src/",
    ],
    copy_sources_except=[
        "include/boost/graph/use_mpi.hpp",
    ],
    post_install=post_install,
)
