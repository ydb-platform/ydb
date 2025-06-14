from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as openjpeg:
        openjpeg.CFLAGS.remove("-DMUTEX_pthread")
        openjpeg.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=[
                        GLOBAL("/DOPJ_STATIC"),
                        "/DMUTEX_win32",
                    ]
                ),
                default=Linkable(
                    CFLAGS=[
                        "-DMUTEX_pthread",
                    ]
                ),
            ),
        )


openjpeg = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/openjpeg",
    nixattr="openjpeg",
    flags=["-DBUILD_CODEC=OFF", "-DBUILD_MJ2=OFF", "-DBUILD_TESTING=OFF"],
    disable_includes=["cidx_manager.h", "indexbox_manager.h", "openjpwl/jpwl.h"],
    build_targets=["openjp2"],
    install_subdir="src/lib/openjp2",
    inclink={
        "include": [
            "opj_config.h",
            "openjpeg.h",
        ],
    },
    platform_dispatchers=["opj_config_private.h"],
    post_install=post_install,
)
