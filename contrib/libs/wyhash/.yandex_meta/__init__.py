from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(Library)


wyhash = NixSourceProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/wyhash",
    nixattr="yamaker-wyhash",
    copy_sources=["wyhash.h"],
    post_install=post_install,
)
