from devtools.yamaker.project import CMakeNinjaNixProject


double_conversion = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/double-conversion",
    nixattr="double-conversion",
    copy_sources=["double-conversion/double-conversion.h"],
)
