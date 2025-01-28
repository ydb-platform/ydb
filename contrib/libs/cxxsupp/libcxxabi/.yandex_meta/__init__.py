from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.platform_macros import make_llvm_nixattr
from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


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

    self.yamakes["."] = self.module(
        Library,
        NO_UTIL=True,
        NO_RUNTIME=True,
        NO_COMPILER_WARNINGS=True,
        # Files are distributed between libcxxabi and libcxx in a weird manner
        # but we can not peerdir the latter to avoid loops (see below)
        # FIXME: sort includes open moving glibcxx-shims into its own dir
        SRCS=fileutil.files(self.dstdir, rel=True, test=pathutil.is_source),
        ADDINCL=[
            f"{self.arcdir}/include",
            "contrib/libs/cxxsupp/libcxx/include",
            # libcxxabi includes libcxx's private "include/refstring.h" header from src subdirectory
            "contrib/libs/cxxsupp/libcxx/src",
        ],
        PEERDIR=[
            "contrib/libs/libunwind",
        ],
        CFLAGS=[
            "-D_LIBCPP_BUILDING_LIBRARY",
            "-D_LIBCXXABI_BUILDING_LIBRARY",
        ],
    )

    with self.yamakes["."] as libcxxabi:
        # As of 1.2.3, musl libc does not provide __cxa_thread_atexit_impl
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
                    -D__WASM_EXCEPTIONS__
                )
            ELSEIF (OS_EMSCRIPTEN AND ARCH_WASM32)
                CFLAGS(
                    -D_LIBCPP_SAFE_STATIC=
                    -D_LIBCXXABI_DTOR_FUNC=
                    -D__WASM_EXCEPTIONS__
                )
            ENDIF()
            """,
        )

        libcxxabi.PEERDIR.add("library/cpp/sanitizer/include")


libcxxabi = NixSourceProject(
    owners=["g:cpp-committee", "g:cpp-contrib"],
    arcdir="contrib/libs/cxxsupp/libcxxabi",
    # nixos-24.05 merged libcxx and libcxxabi.
    # Use the primer and override sourceRoot in override.nix as aworkaround.
    nixattr=make_llvm_nixattr("libcxx"),
    copy_sources=[
        "include/__cxxabi_config.h",
        "include/cxxabi.h",
        "src/*.cpp",
        "src/*.h",
        "src/demangle/*.cpp",
        "src/demangle/*.def",
        "src/demangle/*.h",
    ],
    copy_sources_except=[
        # fake exception implementation which just invokes std::terminate
        "src/cxa_noexception.cpp",
    ],
    disable_includes=[
        "aix_state_tab_eh.inc",
        "ptrauth.h",
        "sys/futex.h",
    ],
    post_install=post_install,
)
