import os
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    includeFilePath = os.path.join(self.dstdir, "lib/includes/nghttp3/nghttp3.h")

    with open(includeFilePath, "r") as file:
        filedata = file.read()

    filedata = filedata.replace("<nghttp3/version.h>", '"version.h"')

    with open(includeFilePath, "w") as file:
        file.write(filedata)

    # Add PEERDIR to contrib/libs/nghttp2 and remove sfparse.c from SRCS
    # because nghttp2 already has sfparse.c.
    # Using sfparse.c from nghttp3 may cause a linker error in projects
    # that use nghttp2 and nghttp3 when building with the flag -all_load
    with self.yamakes["."] as m:
        m.PEERDIR.add("contrib/libs/nghttp2")
        m.SRCS.remove("lib/sfparse/sfparse.c")


nghttp3 = CMakeNinjaNixProject(
    license="MIT",
    flags=["-DENABLE_LIB_ONLY=1"],
    owners=["g:devtools-contrib", "g:yandex-io"],
    nixattr="nghttp3",
    arcdir="contrib/libs/nghttp3",
    post_install=post_install,
    platform_dispatchers=["config.h"],
    addincl_global={".": {"./lib/includes"}},
)
