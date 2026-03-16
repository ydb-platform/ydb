from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        SRCS=[
            "MurmurHash1.cpp",
            "MurmurHash2.cpp",
            "MurmurHash3.cpp",
        ],
        # Despite being written in C++, MurmurHash
        # does not depend on the standard library
        NO_UTIL=True,
        NO_RUNTIME=True,
        NO_COMPILER_WARNINGS=True,
    )


murmurhash = NixSourceProject(
    owners=[],
    arcdir="contrib/restricted/murmurhash",
    nixattr="murmurhash",
    copy_sources=["MurmurHash*"],
    post_install=post_install,
    install_subdir="src",
)
