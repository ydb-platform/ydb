import os

from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    # remove unnecessary test and cleanup its sources
    test = self.yamakes.pop(".")
    for src in test.SRCS:
        os.remove(f"{self.dstdir}/{src}")
    # remove files from src/ subfolder (without recursion)
    for header in os.listdir(f"{self.dstdir}/src"):
        if os.path.isfile(f"{self.dstdir}/src/{header}"):
            os.remove(f"{self.dstdir}/src/{header}")

    # generate LIBRARY modules
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[GLOBAL(f"{self.arcdir}/src")],
    )


# Though sparsehash is a header-only library.
# yet we need to generate config.h during norman autoconf routine.
# Hence we can not use NixSourceProject for it.
sparsehash = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/sparsehash",
    nixattr="sparsehash",
    ignore_commands=[
        "bash",
        "gawk",
    ],
    # FIXME:
    # There is no need to install anything, yet yamaker does not handle,
    # but we need to bring in the config.h generated during build.
    #
    # This target will be removed during post_install().
    install_targets=["hashtable_test"],
    put={"hashtable_test": "."},
    copy_sources=[
        "src/sparsehash/**",
    ],
    post_install=post_install,
)
