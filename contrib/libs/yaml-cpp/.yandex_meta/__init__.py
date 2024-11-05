from devtools.yamaker.project import CMakeNinjaNixProject

yaml_cpp = CMakeNinjaNixProject(
    owners=["g:antiinfra", "g:cpp-contrib"],
    arcdir="contrib/libs/yaml-cpp",
    nixattr="yaml-cpp",
    copy_sources=["include/yaml-cpp/**/*.h"],
)
