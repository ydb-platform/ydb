from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(Library)


miniselect = NixSourceProject(
    nixattr="miniselect",
    arcdir="contrib/libs/miniselect",
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/",
    ],
    post_install=post_install,
)
