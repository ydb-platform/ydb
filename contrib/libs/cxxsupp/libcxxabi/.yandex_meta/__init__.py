from devtools.yamaker import fileutil
from devtools.yamaker import platform_macros
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


ATEXIT_SRC = """
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

        libcxxabi.NO_UTIL = True
        libcxxabi.NO_RUNTIME = True

        # As of 1.2.3, musl libc does not provide __cxa_thread_atexit_impl.
        libcxxabi.CFLAGS.remove("-DHAVE___CXA_THREAD_ATEXIT_IMPL")
        libcxxabi.after(
            "SRCS",
            """
            IF (NOT MUSL) 
                CFLAGS(
                    -DHAVE___CXA_THREAD_ATEXIT_IMPL
                )
            ENDIF()
            """,
        )

        libcxxabi.after(
            "SRCS",
            """
            IF (OS_EMSCRIPTEN AND ARCH_WASM64)
                CFLAGS(
                    -D_LIBCPP_SAFE_STATIC=
                    -D_LIBCXXABI_DTOR_FUNC=
                    -D__USING_WASM_EXCEPTIONS__
                )
            ELSEIF (OS_EMSCRIPTEN AND ARCH_WASM32)
                CFLAGS(
                    -D_LIBCPP_SAFE_STATIC=
                    -D_LIBCXXABI_DTOR_FUNC=
                )
            ENDIF()
            """,
        )

        libcxxabi.PEERDIR.add("library/cpp/sanitizer/include")


llvm_libcxxabi = CMakeNinjaNixProject(
    owners=["g:cpp-committee", "g:cpp-contrib"],
    arcdir="contrib/libs/cxxsupp/libcxxabi",
    nixattr="llvmPackages_16.libcxxabi",
    copy_sources=[
        "include/__cxxabi_config.h",
        "include/cxxabi.h",
    ],
    disable_includes=[
        "aix_state_tab_eh.inc",
    ],
    post_install=post_install,
)
