from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]
    # Support Windows (if it is configured).
    m.SRCS.remove("tif_unix.c")
    m.after(
        "SRCS",
        Switch(
            OS_WINDOWS=Linkable(SRCS=["tif_win32.c"]),
            default=Linkable(SRCS=["tif_unix.c"]),
        ),
    )


libtiff = CMakeNinjaNixProject(
    owners=["g:cpp-contrib", "g:images"],
    arcdir="contrib/libs/libtiff",
    nixattr="libtiff",
    build_targets=["tiff", "tiffxx"],
    disable_includes=[
        "jbig.h",
        "libdeflate.h",
        "Lerc_c_api.h",
        "LIBJPEG_12_PATH",
    ],
    install_subdir="libtiff",
    put_with={"tiff": {"tiffxx"}},
    copy_sources=[
        "tif_win32.c",
        "tiffio.hxx",
    ],
    addincl_global={".": {"."}},
    post_install=post_install,
)
