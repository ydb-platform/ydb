from devtools.yamaker.project import CMakeNinjaNixProject


simdjson = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    nixattr="simdjson",
    arcdir="contrib/libs/simdjson",
    disable_includes=[
        "CppCoreCheck\\Warnings.h",
        "simdjson/nonstd/string_view.hpp",
        "simdjson/ppc64/",
        "simdjson/generic/builder/json_string_builder.h",
        "sys/byteorder.h",
        "ppc64.cpp",
        "lasx.cpp",
        "lasxintrin.h",
        "lsx.cpp",
        "lsxintrin.h",
        "meta",
        "experimental/meta",
    ],
    addincl_global={".": {"./include"}},
    copy_sources=[
        "src/arm64.cpp",
        "include/**/*.h",
    ],
)
