from devtools.yamaker.project import CMakeNinjaNixProject

h3 = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/h3",
    nixattr="h3",
    install_targets=["h3"],
    install_subdir="src",
    projects=["h3"],
    inclink={"h3lib/include/h3": ["h3lib/include/h3api.h"]},
)
