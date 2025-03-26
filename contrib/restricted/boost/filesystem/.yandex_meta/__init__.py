from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        populate_srcs=True,
        CFLAGS=[
            # Our libc++ does not have std::atomic_ref yet
            "-DBOOST_FILESYSTEM_NO_CXX20_ATOMIC_REF",
        ],
    )
    with self.yamakes["."] as filesystem:
        filesystem.after(
            "CFLAGS",
            Switch(
                OS_LINUX=Linkable(CFLAGS=[GLOBAL("-DBOOST_FILESYSTEM_HAS_POSIX_AT_APIS")]),
            ),
        )
        filesystem.after(
            "CFLAGS",
            Switch(
                DYNAMIC_BOOST=Linkable(CFLAGS=[GLOBAL("-DBOOST_FILESYSTEM_DYN_LINK"), "-DBOOST_FILESYSTEM_SOURCE"]),
            ),
        )


boost_filesystem = NixSourceProject(
    nixattr="boost_filesystem",
    arcdir=boost.make_arcdir("filesystem"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
