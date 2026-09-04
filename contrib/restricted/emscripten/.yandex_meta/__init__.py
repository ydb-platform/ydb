import os
import pathlib
import shutil

from devtools.yamaker.fileutil import run
from devtools.yamaker.modules import GLOBAL, Library, List
from devtools.yamaker.project import NixProject


class Emscripten(NixProject):
    def build(self):
        self.unpack()

        self.nix_shell(
            self.builddir,
            """
            echo "LLVM_ROOT = '${llvmEnv}/bin'" > "/tmp/yamaker/emscripten/source/.emscripten"
            echo "NODE_JS = '$(which node)'" >> "/tmp/yamaker/emscripten/source/.emscripten"
        """,
        )

        os.system("""echo "BINARYEN_ROOT = '/dev/null' # directory" >> "/tmp/yamaker/emscripten/source/.emscripten" """)

        cmd = (
            self._nix_env()
            + [self.ctx.fptrace or self.nix_fptrace(), "-d", self.deps_json]
            + self.fptrace_deps_filter_args
            + [
                "/tmp/yamaker/emscripten/source/embuilder.py",
                "--wasm64",
                "--verbose",
                "--pic",
                "--force",
                "build",
                "libc",
                "libstandalonewasm",
                "libdlmalloc",
                "libstubs",
                "libsockets",
            ]
        )

        run(cmd)


def post_install(self):
    extralibs = ["-nostdlib"]
    cflags = [
        GLOBAL("-D_musl_=1"),
        "-D_XOPEN_SOURCE=700",
        "-D__EMSCRIPTEN_SHARED_MEMORY__",
        "-DEMSCRIPTEN_MEMORY_GROWTH",
        "-DEMSCRIPTEN_STANDALONE_WASM",
        "-DEMSCRIPTEN_DYNAMIC_LINKING",
        "-nostdinc",
        "-ffreestanding",
        "-fno-stack-protector",
        # Current Arcadia clang (v.18) does not enable it by default.
        # This setting can be removed after clang update.
        "-mbulk-memory",
    ]

    # These files must go before "musl/include", since these directories contain
    # files with identical names.
    COMMON_PREFIX = [
        f"{self.arcdir}/system/lib/libc/musl/src/internal",
        f"{self.arcdir}/system/lib/libc/musl/src/include",
    ]

    LIBS_TO_ADDINCLS = {
        "system/lib/c": [
            f"{self.arcdir}/system/lib/libc/musl/arch/emscripten",
            f"{self.arcdir}/system/lib/libc/musl/arch/generic",
            f"{self.arcdir}/system/lib/libc/musl/include",
            f"{self.arcdir}/system/include",
            f"{self.arcdir}/system/include/emscripten",
        ],
        "system/lib/libc/musl/src/network": [
            f"{self.arcdir}/system/lib/libc/musl/arch/emscripten",
            f"{self.arcdir}/system/lib/libc/musl/arch/generic",
            f"{self.arcdir}/system/lib/libc/musl/include",
            f"{self.arcdir}/system/include",
            f"{self.arcdir}/system/include/emscripten",
        ],
        "system/lib/stubs": [
            f"{self.arcdir}/system/lib/libc/musl/include",
            f"{self.arcdir}/system/lib/libc/musl/arch/emscripten",
            f"{self.arcdir}/system/lib/libc/musl/arch/generic",
            f"{self.arcdir}/system/include",
        ],
        "system/lib/dlmalloc": [
            f"{self.arcdir}/system/lib/libc/musl/arch/emscripten",
            f"{self.arcdir}/system/lib/libc/musl/arch/generic",
            f"{self.arcdir}/system/include",
            f"{self.arcdir}/system/lib/libc/musl/include",
        ],
        "system/lib/standalonewasm": [
            f"{self.arcdir}/system/lib/libc/musl/arch/emscripten",
            f"{self.arcdir}/system/lib/libc/musl/arch/generic",
            f"{self.arcdir}/system/include",
            f"{self.arcdir}/system/lib/libc/musl/include",
            f"{self.arcdir}/system/include/emscripten",
            "contrib/libs/libunwind/include",
        ],
    }
    for lib_name, addincls in LIBS_TO_ADDINCLS.items():
        with self.yamakes[lib_name] as lib:
            lib.EXTRALIBS = extralibs
            lib.CFLAGS = cflags
            lib.NO_LIBC = True

            if lib_name != 'system/lib/dlmalloc':
                # Ensure internal "musl/src/include" dir will go before common
                # "musl/include" directory on most targets.
                # dlmalloc is special - it must use external headers.
                original_addincl = lib.ADDINCL
                lib.ADDINCL = List(COMMON_PREFIX)
                lib.ADDINCL += original_addincl
            lib.ADDINCL += addincls
            lib.ADDINCL.sorted = False

    with self.yamakes["system/lib/c"] as libc:
        libc.ADDINCL.sorted = False
        libc.SRCS.update(
            [
                "libc/crt1_reactor.c",
            ]
        )

    with self.yamakes["system/lib/standalonewasm"] as libstandalonewasm:
        # This file is also used when building system/lib/c,
        # so here we exclude it in order to be able to link libc and libstandalone together.
        libstandalonewasm.SRCS.remove('libc/musl/src/time/__tz.c')

    shutil.copy(
        f"{self.dstdir}/cache/sysroot/include/emscripten/version.h",
        f"{self.dstdir}/system/include/emscripten/version.h",
    )

    shutil.copytree(
        f"{self.srcdir}/cache/sysroot/include/compat/sys",
        f"{self.dstdir}/system/include/compat/sys",
    )

    shutil.copy(
        f"{self.srcdir}/cache/sysroot/include/compat/time.h",
        f"{self.dstdir}/system/include/compat/time.h",
    )

    shutil.rmtree(f"{self.dstdir}/cache")

    pathlib.Path(f"{self.dstdir}/system/lib/libc/musl/include/sys/epoll.h").touch()
    pathlib.Path(f"{self.dstdir}/system/lib/libc/musl/include/sys/eventfd.h").touch()
    pathlib.Path(f"{self.dstdir}/system/lib/libc/musl/include/sys/sendfile.h").touch()

    self.yamakes["include"] = Library(
        SUBSCRIBER=self.owners,
        LICENSE=["MIT"],
        ADDINCL=[
            GLOBAL("contrib/restricted/emscripten/system/include/compat"),
            GLOBAL("contrib/restricted/emscripten/system/include/emscripten"),
            GLOBAL("contrib/restricted/emscripten/system/lib/libc/musl/arch/emscripten"),
            GLOBAL("contrib/restricted/emscripten/system/lib/libc/musl/arch/generic"),
            GLOBAL("contrib/restricted/emscripten/system/lib/libc/musl/include"),
            GLOBAL("contrib/restricted/emscripten/system/include"),
        ],
        NO_RUNTIME=True,
        NO_PLATFORM=True,
    )
    self.yamakes["."].RECURSE |= {"include"}

    self.yamakes["system/lib/libc/musl/src/network"].LICENSE = ["MIT"]


emscripten = Emscripten(
    arcdir="contrib/restricted/emscripten",
    nixattr="yamaker-emscripten",
    copy_sources=[
        "system/lib/libc/crt1_reactor.c",
        "system/lib/libc/musl/arch/emscripten",
        "system/lib/libc/musl/arch/generic",
        "system/lib/libc/musl/include",
        "system/include/wasi",
        "system/include/emscripten",
    ],
    put={"stubs": "system/lib/stubs"},
    post_install=post_install,
    keep_paths=["a.yaml"],
)
