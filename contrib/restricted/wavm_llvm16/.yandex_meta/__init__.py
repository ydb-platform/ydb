import fnmatch
import os
import shutil

from devtools.yamaker import fileutil
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def _filter_srcs(srcs, pattern):
    return sorted(s for s in srcs if fnmatch.fnmatch(s, pattern))


def _windows_platform_cpp_srcs(dstdir):
    lib_dir = os.path.join(dstdir, "Lib")
    return sorted(
        f"Platform/Windows/{os.path.basename(path)}"
        for path in fileutil.iglob(os.path.join(lib_dir, "Platform/Windows/*.cpp"))
    )


def post_install(self):
    lib = self.yamakes["Lib"]

    # DTCC-1595 Add explicit sanitizer dependencies up to contrib/tools
    lib.PEERDIR.add("library/cpp/sanitizer/include")

    # Used at 0005-allocate-linear-memory-more-granularly.patch
    lib.SRCS.add("Runtime/VectorOverMMap.cpp")

    # Used at 0024-wasm-dwarf-debug-info.patch
    lib.SRCS.add("Runtime/ModuleDebugInfo.cpp")

    # Platform sources are selected by OS; drop CMake's Linux/POSIX
    # defaults from the common list.
    posix_cpp = _filter_srcs(lib.SRCS, "Platform/POSIX/*.cpp")
    lib.SRCS -= {s for s in lib.SRCS if fnmatch.fnmatch(s, "Platform/POSIX/*")}
    lib.SRCS -= {s for s in lib.SRCS if fnmatch.fnmatch(s, "Platform/Windows/*")}

    lib.PEERDIR.remove("contrib/libs/libunwind")
    lib.after(
        "PEERDIR",
        Switch(
            {
                "NOT OS_WINDOWS": Linkable(
                    PEERDIR=["contrib/libs/libunwind"],
                ),
            }
        ),
    )

    if "contrib/libs/libunwind/include" in lib.ADDINCL:
        lib.ADDINCL.remove("contrib/libs/libunwind/include")
    lib.after(
        "ADDINCL",
        Switch(
            {
                "NOT OS_WINDOWS": Linkable(
                    ADDINCL=["contrib/libs/libunwind/include"],
                ),
            }
        ),
    )

    for flag in ("-DHAS_FUTIMENS", "-DHAS_UTIMENSAT"):
        if flag in lib.CFLAGS:
            lib.CFLAGS.remove(flag)
    lib.after(
        "CFLAGS",
        Switch(
            {
                "NOT OS_WINDOWS": Linkable(
                    CFLAGS=["-DHAS_FUTIMENS", "-DHAS_UTIMENSAT"],
                ),
            }
        ),
    )

    windows_cpp = _windows_platform_cpp_srcs(self.dstdir)
    lib.after(
        "SRCS",
        Switch(
            {
                "OS_WINDOWS": Linkable(
                    SRCS=windows_cpp,
                    LDFLAGS=[
                        "/DEFAULTLIB:bcrypt.lib",
                        "/DEFAULTLIB:psapi.lib",
                    ],
                ),
                "ARCH_AARCH64": Linkable(
                    SRCS=(posix_cpp + ["Platform/POSIX/POSIX-AArch64.S"]),
                ),
                "default": Linkable(
                    SRCS=(posix_cpp + ["Platform/POSIX/POSIX-X86_64.S"]),
                ),
            }
        ),
    )  # Used at 0025-build-on-windows.patch

    # Manual remove blake2 and liblmdb because all sources are dumped
    # into a single target
    shutil.rmtree(f"{self.dstdir}/ThirdParty")
    third_party = f"{self.dstdir}/THIRD-PARTY.md"
    tmp = f"{self.dstdir}/tmp"
    os.system(f'grep -Ev "liblmdb|BLAKE2" {third_party}' f" | cat -s > {tmp} && mv {tmp} {third_party}")


wavm = CMakeNinjaNixProject(
    owners=["g:contrib"],
    arcdir="contrib/restricted/wavm_llvm16",
    nixattr="wavm",
    use_provides=[
        "contrib/libs/llvm16",
    ],
    unbundle_from={
        "unwind": "ThirdParty/libunwind",
        "xxhash": "Include/WAVM/Inline/xxhash",
    },
    post_install=post_install,
    copy_sources=[
        "Include/WAVM/wavm-c/wasm-c-api.LICENSE",
        "Lib/Platform/Windows/",
    ],
    ignore_targets=[
        "WAVMBLAKE2",
        "WAVMUnwind",
        "WAVMlmdb",
        "libWAVMBLAKE2.a",
        "libWAVMUnwind.a",
        "libWAVMlmdb.a",
        "translate-compile-model-corpus",
        "fuzz-assemble",
        "fuzz-compile-model",
        "fuzz-disassemble",
        "fuzz-instantiate",
    ],
)
