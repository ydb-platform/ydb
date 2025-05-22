from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject
from devtools.yamaker import python


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        NO_COMPILER_WARNINGS=True,
        # pybind11 supports TString, hence it requires util dependency
        NO_UTIL=False,
        ADDINCL=[
            ArcPath(f"{self.arcdir}/include", GLOBAL=True),
        ],
    )


pybind11 = NixSourceProject(
    owners=["g:cpp-contrib", "g:python-contrib"],
    nixattr=python.make_nixattr("pybind11"),
    arcdir="contrib/libs/pybind11",
    copy_sources=[
        "include/pybind11/**/*.h",
    ],
    disable_includes=[
        "experimental/optional",
    ],
    post_install=post_install,
)
