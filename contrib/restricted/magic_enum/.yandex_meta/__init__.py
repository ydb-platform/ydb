from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[GLOBAL(self.arcdir + "/include")],
    )


magic_enum = NixSourceProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/magic_enum",
    nixattr="yamaker-magic-enum",
    copy_sources=[
        "include/**/*.hpp",
    ],
    disable_includes=[
        "MAGIC_ENUM_CONFIG_FILE",
    ],
    keep_paths=[
        "include/magic_enum.hpp",
    ],
    post_install=post_install,
)
