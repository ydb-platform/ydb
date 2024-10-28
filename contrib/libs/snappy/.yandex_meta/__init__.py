import os.path

from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as snappy:
        # snappy is patched to support TString
        snappy.NO_UTIL = False

        # At the time, contrib/libs/snappy goes to ADDINCL GLOBAL
        # due to presense in INTERFACE_INCLUDE_DIRECTORIES.txt.
        #
        # Replacing it with ADDINCL to newly generate inclink directory.
        snappy.ADDINCL = [GLOBAL(os.path.join(self.arcdir, "include"))]

        snappy.PEERDIR.add("library/cpp/sanitizer/include")


snappy = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/snappy",
    nixattr="snappy",
    platform_dispatchers=["config.h"],
    inclink={
        "include": [
            "snappy.h",
            "snappy-c.h",
            "snappy-sinksource.h",
            "snappy-stubs-public.h",
        ]
    },
    # Do not install unittests
    install_targets=["snappy"],
    post_install=post_install,
)
