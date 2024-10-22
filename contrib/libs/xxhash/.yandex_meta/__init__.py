from devtools.yamaker.project import GNUMakeNixProject


xxhash = GNUMakeNixProject(
    arcdir="contrib/libs/xxhash",
    nixattr="xxHash",
    makeflags=["libxxhash"],
    disable_includes=[
        "arm_sve.h",
    ],
)
