from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[GLOBAL(self.arcdir + "/include")],
    )


morton_nd = NixSourceProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/clickhouse-deps/morton-nd",
    nixattr="yamaker-morton-nd",
    copy_sources=["include/morton-nd/*.h"],
    post_install=post_install,
)
