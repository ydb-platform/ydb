from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[ArcPath(f"{self.arcdir}/include", GLOBAL=True)],
        NO_UTIL=True,
    )


nlohman_json = NixSourceProject(
    nixattr="nlohmann_json",
    owners=["g:logbroker", "g:cpp-contrib"],
    arcdir="contrib/restricted/nlohmann_json",
    copy_sources=[
        "include/",
    ],
    disable_includes=[
        "experimental/filesystem",
    ],
    post_install=post_install,
)
