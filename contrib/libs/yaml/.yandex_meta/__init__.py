import os
import shutil

from devtools.yamaker.project import GNUMakeNixProject


def libyaml_post_install(self):
    shutil.move(
        os.path.join(self.dstdir, "include/config.h"),
        os.path.join(self.dstdir, "src/config.h"),
    )


libyaml = GNUMakeNixProject(
    owners=[
        "g:cpp-contrib",
    ],
    arcdir="contrib/libs/yaml",
    nixattr="libyaml",
    addincl_global={".": {"./include"}},
    copy_sources=[
        "License",
    ],
    makeflags=["-C", "src"],
    post_install=libyaml_post_install,
)
