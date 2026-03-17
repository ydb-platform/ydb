from devtools.yamaker.project import GNUMakeNixProject

giflib = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/giflib",
    nixattr="giflib",
    makeflags=["libgif.a"],
)
