from devtools.yamaker.project import CMakeNinjaNixProject


croaring = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/croaring",
    nixattr="croaring",
    copy_sources=[
        "cpp/roaring/*.hh",
    ],
    disable_includes=[
        "sys/byteorder.h",
    ],
    addincl_global={
        ".": ["./include"],
    },
    inclink={"include/roaring": ["cpp/roaring/*.hh"]},
)
