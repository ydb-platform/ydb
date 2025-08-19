from devtools.yamaker.project import GNUMakeNixProject


nghttp2 = GNUMakeNixProject(
    nixattr="nghttp2",
    arcdir="contrib/libs/nghttp2",
    owners=["g:cpp-contrib"],
    makeflags=["-C", "lib", "libnghttp2.la"],
    addincl_global={
        ".": {"./lib/includes"},
    },
    platform_dispatchers=[
        "config.h",
    ],
)
