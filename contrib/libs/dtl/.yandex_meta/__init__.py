from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
    )


dtl = NixSourceProject(
    nixattr="dtl",
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/dtl",
    copy_sources=["dtl/*.hpp"],
    post_install=post_install,
)
