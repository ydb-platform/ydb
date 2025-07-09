import os.path

from devtools.yamaker import boost, fileutil, pathutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        SRCS=["src/future.cpp"],
    )
    with self.yamakes["."] as thread:
        thread.after(
            "SRCS",
            Switch(
                {
                    "OS_WINDOWS": Linkable(
                        SRCS=fileutil.files(
                            os.path.join(self.dstdir, "src", "win32"), rel=self.dstdir, test=pathutil.is_source
                        ),
                        CFLAGS=[
                            GLOBAL("-DBOOST_THREAD_WIN32"),
                            "-DBOOST_THREAD_USES_CHRONO",
                            "-DWIN32_LEAN_AND_MEAN",
                            "-DBOOST_USE_WINDOWS_H",
                        ],
                    ),
                    "default": Linkable(
                        SRCS=fileutil.files(
                            os.path.join(self.dstdir, "src", "pthread"),
                            rel=self.dstdir,
                            # once_atomic is conditionally included from once.cpp
                            test=lambda p: pathutil.is_source(p) and p != "src/pthread/once_atomic.cpp",
                        ),
                        CFLAGS=[GLOBAL("-DBOOST_THREAD_POSIX"), "-DBOOST_THREAD_DONT_USE_CHRONO"],
                    ),
                }
            ),
        )
        thread.after(
            "CFLAGS",
            Switch(
                {
                    "DYNAMIC_BOOST": Linkable(CFLAGS=["-DBOOST_THREAD_BUILD_DLL", GLOBAL("-DBOOST_THREAD_USE_DLL")]),
                    "default": Linkable(CFLAGS=["-DBOOST_THREAD_BUILD_LIB", GLOBAL("-DBOOST_THREAD_USE_LIB")]),
                }
            ),
        )


boost_thread = NixSourceProject(
    nixattr="boost_thread",
    arcdir=boost.make_arcdir("thread"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    copy_sources_except=[
        # only used in tests
        "tss_null.cpp",
    ],
    disable_includes=[
        "vxCpuLib.h",
    ],
    post_install=post_install,
)
