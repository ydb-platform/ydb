from devtools.yamaker.project import CMakeNinjaNixProject


simdjson = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    nixattr="simdjson",
    arcdir="contrib/libs/simdjson",
    disable_includes=[
        "CppCoreCheck\\Warnings.h",
        "simdjson/nonstd/string_view.hpp",
        "simdjson/ppc64/",
        "sys/byteorder.h",
        "ppc64.cpp",
        "lasx.cpp",
        "lasxintrin.h",
        "lsx.cpp",
        "lsxintrin.h",
    ],
    addincl_global={".": {"./include"}},
    copy_sources=[
        "src/arm64.cpp",
        "include/**/*.h",
    ],
)
