from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(Library)


pdqsort = NixSourceProject(
    nixattr="pdqsort",
    arcdir="contrib/libs/pdqsort",
    copy_sources=["license.txt", "pdqsort.h"],
    post_install=post_install,
)
