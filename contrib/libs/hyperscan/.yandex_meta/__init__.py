import copy
import json
import os
import os.path as P
import shutil

from devtools.yamaker import boost
from devtools.yamaker.fileutil import re_sub_dir, re_sub_file
from devtools.yamaker.project import CMakeNinjaNixProject


RUNTIMES = {
    "core2": [
        # intentionally empty
    ],
    "corei7": [
        "${SSE41_CFLAGS}",
        "-DHAVE_SSE41",
        "${SSE42_CFLAGS}",
        "-DHAVE_SSE42",
        "${POPCNT_CFLAGS}",
        "-DHAVE_POPCOUNT_INSTR",
    ],
    "avx2": [
        "${SSE41_CFLAGS}",
        "-DHAVE_SSE41",
        "${SSE42_CFLAGS}",
        "-DHAVE_SSE42",
        "${POPCNT_CFLAGS}",
        "-DHAVE_POPCOUNT_INSTR",
        "${AVX_CFLAGS}",
        "-DHAVE_AVX",
        "${AVX2_CFLAGS}",
        "-DHAVE_AVX2",
    ],
    "avx512": [
        "${SSE41_CFLAGS}",
        "-DHAVE_SSE41",
        "${SSE42_CFLAGS}",
        "-DHAVE_SSE42",
        "-DHAVE_POPCOUNT_INSTR",
        "${POPCNT_CFLAGS}",
        "${AVX_CFLAGS}",
        "-DHAVE_AVX",
        "${AVX2_CFLAGS}",
        "-DHAVE_AVX2",
        "${AVX512_CFLAGS}",
        "-DHAVE_AVX512",
    ],
}


def instantiate_runtime(self, *, runtime_name, runtime_yamake):
    runtime_subdir = f"runtime_{runtime_name}"
    self.yamakes[runtime_subdir] = copy.deepcopy(runtime_yamake)
    with self.yamakes[runtime_subdir] as runtime:
        runtime_dir = f"{self.dstdir}/{runtime_subdir}"
        os.makedirs(runtime_dir)
        runtime.CFLAGS = RUNTIMES[runtime_name]

        # list of symbols that will be duplicated if compiled without proper wrapping.
        # It can be obtained with compiling runtime_* libraries and applying
        # nm --defined-only --extern-only --format=posix -o *.a | awk '{print $2}'
        with open(P.join(self.meta_dir, "symbols.json")) as f:
            symbols_to_rename = json.load(f)

        # rename symbols that would be duplicated between runtimes otherwise
        runtime.CFLAGS += [f"-D{symbol}={runtime_name}_{symbol}" for symbol in sorted(symbols_to_rename)]

        # TODO: understand if this dispatcher is intended to work at all
        runtime.SRCS.remove("src/dispatcher.c")

        # copy headers and rename hs_ entrypoints to make them match the ones
        for header in ("hs_common.h", "hs_runtime.h"):
            runtime_specific_header = f"{runtime_dir}/{header}"
            shutil.copy(f"{self.dstdir}/src/{header}", runtime_specific_header)
            re_sub_file(
                runtime_specific_header,
                "HS_CDECL hs_",
                f"{runtime_name}_hs_",
            )
        # Fix include guards to allow inclusions into a single
        # library/cpp/regex/hyperscan/hyperscan.cpp
        re_sub_file(
            f"{runtime_dir}/hs_common.h",
            "HS_COMMON_H_",
            f"HS_{runtime_name.upper()}_COMMON_H",
        )
        re_sub_file(
            f"{runtime_dir}/hs_runtime.h",
            "HS_RUNTIME_H_",
            f"HS_{runtime_name.upper()}_RUNTIME_H",
        )


def post_install(self):
    # make all SRCS to start with src in order
    # to make hierarchy match with hyperscan compiler
    with self.yamakes["runtime"] as runtime:
        runtime.SRCDIR = [self.arcdir]
        runtime.SRCS = [f"src/{path}" for path in runtime.SRCS]
    runtime_yamake = self.yamakes.pop("runtime")
    for runtime_name in RUNTIMES.keys():
        instantiate_runtime(self, runtime_name=runtime_name, runtime_yamake=runtime_yamake)

    with self.yamakes["."] as hyperscan:
        hyperscan.RECURSE = [f"runtime_{name}" for name in sorted(RUNTIMES.keys())]
        # rename make_unique into std::make_unique to resolve ambigousness with boost::make_unique
        re_sub_dir(
            self.dstdir,
            r"([ \(])make_unique<",
            r"\1std::make_unique<",
        )

        hyperscan.PEERDIR += [
            boost.make_arcdir("dynamic_bitset"),
            boost.make_arcdir("graph"),
            boost.make_arcdir("icl"),
            boost.make_arcdir("multi_array"),
            boost.make_arcdir("property_map"),
        ]

        # TODO: understand if this dispatcher is intended to work at all
        hyperscan.SRCS.remove("src/dispatcher.c")
        os.remove(f"{self.dstdir}/src/dispatcher.c")

        # rename .rl sources to rl6 so they could be recognized by ymake
        ragel_srcs = [src for src in hyperscan.SRCS if src.endswith(".rl")]
        for ragel_src in ragel_srcs:
            os.rename(f"{self.dstdir}/{ragel_src}", f"{self.dstdir}/{ragel_src}6")
            hyperscan.SRCS.remove(ragel_src)
            hyperscan.SRCS.add(ragel_src + "6")


hyperscan = CMakeNinjaNixProject(
    owners=[
        "galtsev",
        "g:antiinfra",
        "g:cpp-contrib",
        "g:yql",
    ],
    arcdir="contrib/libs/hyperscan",
    nixattr="hyperscan",
    install_targets=[
        "hs",
        "hs_runtime",
    ],
    put={
        "hs": ".",
        "hs_runtime": "runtime",
    },
    platform_dispatchers=["config.h"],
    post_install=post_install,
)
