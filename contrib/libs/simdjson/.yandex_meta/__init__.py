from devtools.yamaker.project import CMakeNinjaNixProject


simdjson = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    nixattr="simdjson",
    arcdir="contrib/libs/simdjson",
    disable_includes=[
        "CppCoreCheck\\Warnings.h",
        "simdjson/generic/ondemand/json_string_builder.h",
        "simdjson/nonstd/string_view.hpp",
        # PPC64 arch support
        "simdjson/ppc64/",
        "ppc64.cpp",
        "sys/byteorder.h",
        # LASX arch support
        "lasx.cpp",
        "lasxintrin.h",
        "lsx.cpp",
        "lsxintrin.h",
        # RISC-V arch support
        "riscv_bitmanip.h",
        "riscv_vector.h",
        "rvv-vls.cpp",
        "meta",
        "experimental/meta",
    ],
    addincl_global={".": {"./include"}},
    copy_sources=[
        "src/arm64.cpp",
        "include/**/*.h",
    ],
)
