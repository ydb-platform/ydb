import os

from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    os.unlink(self.dstdir + "/unwind-itanium.h")
    os.unlink(self.dstdir + "/libelftc_dem_gnu3.c")
    with self.yamakes["."] as libcxxrt:
        # Do not create peerdir loop from libcxx and libcxxrt
        libcxxrt.NO_RUNTIME = True
        libcxxrt.SRCS.remove("libelftc_dem_gnu3.c")
        libcxxrt.before(
            "SRCS",
            Switch({"SANITIZER_TYPE == undefined OR FUZZING": Linkable(NO_SANITIZE=True, NO_SANITIZE_COVERAGE=True)}),
        )
        libcxxrt.CXXFLAGS = ["-nostdinc++"]
        libcxxrt.PEERDIR |= {
            "contrib/libs/libunwind",
            "library/cpp/sanitizer/include",
        }


libcxxrt = CMakeNinjaNixProject(
    arcdir="contrib/libs/cxxsupp/libcxxrt",
    nixattr="libcxxrt",
    owners=["g:cpp-committee", "g:cpp-contrib"],
    install_subdir="src",
    inclink={"include": {"cxxabi.h"}},
    keep_paths=[
        "unwind.h",
    ],
    post_install=post_install,
)

libcxxrt.copy_top_sources_except |= {
    "INTERFACE_LINK_LIBRARIES.txt",
}
