from devtools.yamaker.project import GNUMakeNixProject

sctp = GNUMakeNixProject(
    arcdir="contrib/libs/sctp",
    nixattr="lksctp-tools",
    makeflags=["-C", "src/lib"],
)
