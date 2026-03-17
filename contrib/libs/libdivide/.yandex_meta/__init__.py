from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        NO_UTIL=True,
        NO_RUNTIME=True,
    )


libdivide = NixSourceProject(
    owners=["g:cpp-contrib", "g:clickhouse"],
    arcdir="contrib/libs/libdivide",
    nixattr="libdivide",
    copy_sources=["libdivide.h"],
    post_install=post_install,
)
