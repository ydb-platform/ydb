from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as ftxui:
        ftxui.after(
            "CFLAGS",
            Switch({"OS_WINDOWS": Linkable(CFLAGS=["-DUNICODE", "-D_UNICODE"])}),
        )


ftxui = CMakeNinjaNixProject(
    owners=["g:taxi-common"],
    nixattr="ftxui",
    flags=[
        "-DFTXUI_BUILD_EXAMPLES=OFF",
    ],
    disable_includes=["emscripten.h"],
    put_with={"ftxui-component": ["ftxui-dom", "ftxui-screen"]},
    arcdir="contrib/libs/ftxui",
    post_install=post_install,
)
