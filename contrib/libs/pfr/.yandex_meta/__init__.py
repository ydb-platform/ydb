from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[GLOBAL(f"{self.arcdir}/include")],
    )


pfr = NixSourceProject(
    nixattr="pfr",
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/pfr",
    copy_sources=[
        "include/*.hpp",
        "include/**/*.hpp",
    ],
    post_install=post_install,
)
