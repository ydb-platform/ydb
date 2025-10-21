import os.path
import shutil

from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    for root_file in [
        "CONTRIBUTING.md",
        "CHANGES.txt",
        "LICENSE.txt",
        "README.md",
    ]:
        shutil.copy(os.path.join(self.srcdir, f"../../{root_file}"), self.dstdir)
    shutil.copy(f"{self.srcdir}/README.md", f"{self.dstdir}/README-cpp.md")

    with self.yamakes["."] as runtime:
        runtime.ADDINCL = [GLOBAL(os.path.join(self.arcdir, "src"))]
        runtime.CFLAGS = [
            GLOBAL("-DANTLR4CPP_STATIC"),
            GLOBAL("-DANTLR4_USE_THREAD_LOCAL_CACHE"),
            GLOBAL("-DANTLR4CPP_USING_ABSEIL"),
        ]
        runtime.PEERDIR = ["contrib/restricted/abseil-cpp"]


antlr4_cpp_runtime = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/antlr4_cpp_runtime",
    nixattr="antlr4_13",
    nixsrcdir="source/runtime/Cpp",
    install_subdir="runtime",
    disable_includes=[
        # ifdef GUID_ANDROID
        "jni.h",
        # ifdef GUID_LIBUUID
        "uuid/uuid.h",
        # ifdef GUID_CFUUID
        "CoreFoundation/CFUUID.h",
        # ifdef GUID_WINDOWS
        "objbase.h",
        # ifdef USE_UTF8_INSTEAD_OF_CODECVT
        "utf8.h",
    ],
    post_install=post_install,
)
