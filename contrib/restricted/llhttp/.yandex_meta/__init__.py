from devtools.yamaker.project import CMakeNinjaNixProject


llhttp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/llhttp",
    nixattr="llhttp",
)
