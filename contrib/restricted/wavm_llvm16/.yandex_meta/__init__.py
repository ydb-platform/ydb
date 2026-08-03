import os
import shutil

from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker.modules import Linkable, Switch


def post_install(self):
    self.yamakes["Lib"].PEERDIR.add(
        "library/cpp/sanitizer/include"
    )  # DTCC-1595 Add explicit sanitizer dependencies up to contrib/tools

    self.yamakes["Lib"].SRCS.add(
        "Runtime/VectorOverMMap.cpp"
    )  # Used at 0005-allocate-linear-memory-more-granularly.patch

    self.yamakes["Lib"].SRCS.remove("Platform/POSIX/POSIX-X86_64.S")
    self.yamakes["Lib"].after(
        "SRCS",
        Switch(
            {
                "ARCH_AARCH64": Linkable(SRCS=["Platform/POSIX/POSIX-AArch64.S"]),
                "default": Linkable(SRCS=["Platform/POSIX/POSIX-X86_64.S"]),
            }
        ),
    )  # Customize POSIX-*.S

    # Manual remove blake2 and liblmdb because all sources are dumped into a single target
    shutil.rmtree(f"{self.dstdir}/ThirdParty")
    os.system(
        f'grep -Ev "liblmdb|BLAKE2" {self.dstdir}/THIRD-PARTY.md | cat -s > {self.dstdir}/tmp && mv {self.dstdir}/tmp {self.dstdir}/THIRD-PARTY.md'
    )


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
