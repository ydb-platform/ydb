from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        NO_UTIL=True,
        NO_RUNTIME=True,
        NO_COMPILER_WARNINGS=True,
    )


fast_float = NixSourceProject(
    owners=["g:clickhouse", "g:cpp-contrib"],
    nixattr="fast-float",
    arcdir="contrib/restricted/fast_float",
    copy_sources=["include/fast_float/*.h"],
    disable_includes=[
        "stdfloat",
        "sys/byteorder.h",
    ],
    post_install=post_install,
)
