from devtools.yamaker import fileutil
from devtools.yamaker import platform_macros
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


ATEXIT_SRC = """
SRC_C_PIC(
    src/cxa_thread_atexit.cpp -fno-lto
)
"""


def post_install(self):
    # libcxxabi-parts is built from libcxxabi sources
    # Update VERSION and ORIGINAL_SOURCE values upon libcxxabi update.
    fileutil.re_sub_file(
        f"{self.ctx.arc}/contrib/libs/cxxsupp/libcxxabi-parts/ya.make",
        r"ORIGINAL_SOURCE\(.*\)",
        f"ORIGINAL_SOURCE({self.source_url})",
    )

    fileutil.re_sub_file(
        f"{self.ctx.arc}/contrib/libs/cxxsupp/libcxxabi-parts/ya.make",
        r"VERSION\(.*\)",
        f"VERSION({self.version})",
    )

    with self.yamakes["."] as libcxxabi:
        # Files are distributed between libcxxabi and libcxx in a weird manner
        # but we can not peerdir the latter to avoid loops (see below)
        # FIXME: sort includes open moving glibcxx-shims into its own dir
        libcxxabi.ADDINCL = [
            f"{self.arcdir}/include",
            "contrib/libs/cxxsupp/libcxx/include",
            # libcxxabi includes libcxx's private "include/refstring.h" header from src subdirectory
            "contrib/libs/cxxsupp/libcxx/src",
        ]

        # We link libpthread.so automatically depending on the target platform
        libcxxabi.CFLAGS.remove("-D_LIBCXXABI_LINK_PTHREAD_LIB")

        # -DHAVE___CXA_THREAD_ATEXIT_IMPL MUST NOT BE SET
        # until we will start to compile against pure OS_SDK=ubuntu-14 by default
        libcxxabi.CFLAGS.remove("-DHAVE___CXA_THREAD_ATEXIT_IMPL")

        # Do not create loop from libcxx, libcxxrt and libcxxabi
        libcxxabi.NO_UTIL = True
        libcxxabi.NO_RUNTIME = True

        # disable lto to allow replacing __cxa_thread_atexit_impl in runtime
        libcxxabi.SRCS.remove("src/cxa_thread_atexit.cpp")
        libcxxabi.after("SRCS", ATEXIT_SRC)

        libcxxabi.after(
            "SRCS",
            Switch(
                {
                    "OS_EMSCRIPTEN AND NOT ARCH_WASM32": Linkable(
                        CFLAGS=[
                            "-D_LIBCPP_SAFE_STATIC=",
                            "-D_LIBCXXABI_DTOR_FUNC=",
                            "-D__USING_WASM_EXCEPTIONS__",
                        ]
                    ),
                    "OS_EMSCRIPTEN AND ARCH_WASM32": Linkable(
                        CFLAGS=[
                            "-D_LIBCPP_SAFE_STATIC=",
                            "-D_LIBCXXABI_DTOR_FUNC=",
                        ]
                    ),
                }
            ),
        )

        libcxxabi.PEERDIR.add("library/cpp/sanitizer/include")


llvm_libcxxabi = CMakeNinjaNixProject(
    owners=["g:cpp-committee", "g:cpp-contrib"],
    arcdir="contrib/libs/cxxsupp/libcxxabi",
    copy_sources=[
        "include/__cxxabi_config.h",
        "include/cxxabi.h",
    ],
    disable_includes=[
        "aix_state_tab_eh.inc",
    ],
    nixattr=platform_macros.make_llvm_nixattr("libcxxabi"),
    post_install=post_install,
)
