from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as lib:
        lib.CFLAGS = [GLOBAL("-DFMT_EXPORT")]


fmt = CMakeNinjaNixProject(
    arcdir="contrib/libs/fmt",
    nixattr="fmt_8",
    disable_includes=[
        "experimental/string_view",
    ],
    copy_sources=[
        "include/**/*.h",
    ],
    post_install=post_install,
)
