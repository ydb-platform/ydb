from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        NO_RUNTIME=True,
        NO_COMPILER_WARNINGS=True,
        PEERDIR=["contrib/libs/openssl"],
        ADDINCL=[ArcPath(f"{self.arcdir}/include", ONE_LEVEL=True)],
    )


jwt_cpp = NixSourceProject(
    arcdir="contrib/libs/jwt-cpp",
    nixattr="jwt-cpp",
    post_install=post_install,
    copy_sources=[
        "include/jwt-cpp/*.h",
        "include/picojson/picojson.h",
    ],
)
