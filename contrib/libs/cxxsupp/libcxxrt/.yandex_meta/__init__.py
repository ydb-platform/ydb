import os

from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import CMakeNinjaNixProject

EXCEPTION_ONLY_SRCS = ["exception.cc"]
NOEXCEPT_ONLY_SRCS = ["noexception.cc"]

def post_install(self):
    os.unlink(self.dstdir + "/unwind-itanium.h")
    os.unlink(self.dstdir + "/libelftc_dem_gnu3.c")
    with self.yamakes["."] as libcxxrt:
        # Do not create peerdir loop from libcxx and libcxxrt
        libcxxrt.NO_RUNTIME = True
        libcxxrt.SRCS.remove("libelftc_dem_gnu3.c")
        libcxxrt.before(
            "SRCS",
            Switch({"SANITIZER_TYPE == undefined OR FUZZING": Linkable(NO_SANITIZE=True, NO_SANITIZE_COVERAGE=True)}),
        )
        libcxxrt.CXXFLAGS = ["-nostdinc++"]
        libcxxrt.PEERDIR |= {
            "contrib/libs/libunwind",
            "library/cpp/sanitizer/include",
        }
        # make that if from https://github.com/libcxxrt/libcxxrt/blob/a6f71cbc3a1e1b8b9df241e081fa0ffdcde96249/src/CMakeLists.txt#L12
        for src in EXCEPTION_ONLY_SRCS:
            if src in libcxxrt.SRCS:
                libcxxrt.SRCS.remove(src)
        for src in NOEXCEPT_ONLY_SRCS:
            if src in libcxxrt.SRCS:
                libcxxrt.SRCS.remove(src)
        libcxxrt.after(
            "SRCS",
            Switch(
                {
                    "NO_CXX_EXCEPTIONS": Linkable(SRCS=NOEXCEPT_ONLY_SRCS),
                    "default": Linkable(SRCS=EXCEPTION_ONLY_SRCS),
                }
            ),
        )
        libcxxrt.after(
            "SRCS",
            """
            IF (NO_CXX_EXCEPTIONS)
                CFLAGS(-D_CXXRT_NO_EXCEPTIONS)
            ENDIF()
            """,
        )


libcxxrt = CMakeNinjaNixProject(
    arcdir="contrib/libs/cxxsupp/libcxxrt",
    nixattr="libcxxrt",
    owners=["g:cpp-contrib"],
    install_subdir="src",
    inclink={"include": {"cxxabi.h"}},
    keep_paths=[
        "unwind.h",
    ],
    post_install=post_install,
    copy_sources=NOEXCEPT_ONLY_SRCS
)

libcxxrt.copy_top_sources_except |= {
    "INTERFACE_LINK_LIBRARIES.txt",
}
