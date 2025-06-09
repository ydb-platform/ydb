from devtools.yamaker.project import CMakeNinjaNixProject

ftxui = CMakeNinjaNixProject(
    owners=["segoon", "g:taxi-common"],
    nixattr="ftxui",
    flags=[
        "-DFTXUI_BUILD_EXAMPLES=OFF",
    ],
    disable_includes=["emscripten.h"],
    put_with={"ftxui-component": ["ftxui-dom", "ftxui-screen"]},
    arcdir="contrib/libs/ftxui",
)
