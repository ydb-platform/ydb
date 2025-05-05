from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        populate_srcs=True,
        CFLAGS=["-DBOOST_IOSTREAMS_USE_DEPRECATED"],
    )
    with self.yamakes["."] as iostreams:
        iostreams.ADDINCL += [
            "contrib/libs/libbz2",
            "contrib/libs/zstd/include",
        ]
        iostreams.PEERDIR += [
            "contrib/libs/libbz2",
            "contrib/libs/lzma",
            "contrib/libs/zlib",
            "contrib/libs/zstd",
        ]
        iostreams.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_IOSTREAMS_DYN_LINK")])}))


boost_iostreams = NixSourceProject(
    nixattr="boost_iostreams",
    arcdir=boost.make_arcdir("iostreams"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    disable_includes=[
        "fstream.h",
        "iostream.h",
        "streambuf.h",
    ],
    post_install=post_install,
)
