import os.path
import re

from devtools.yamaker.modules import Switch
from devtools.yamaker.project import NixProject


def post_install(self):
    main = self.yamakes["."]
    main.RECURSE = []
    main.PEERDIR = []
    amd64_specific_speedups = ""
    for libname in sorted(self.yamakes):
        if libname == ".":
            continue
        # convert standalone arch-specific libraries into set of SRC_* macros in the main ya.make file.
        lib = self.yamakes.pop(libname)

        mode = re.split("[_-]", libname, maxsplit=1)[-1].upper()
        assert len(lib.SRCS) == 1
        assert len(lib.SRCDIR) == 1
        src = next(iter(lib.SRCS))
        src = os.path.relpath(f"{lib.SRCDIR[0]}/{src}", self.arcdir)
        # remove cflags that are already set in the main library for the same of brevity
        cflags = " ".join(set(lib.CFLAGS) - set(main.CFLAGS))

        if mode == "REF":
            # Pure C implementation, put the source into SRC macro
            main.before("END", f"SRC({src} {cflags})")
        else:
            # Faster implementation with intrinsics, put the source into SRC_C_{MODE} macro
            amd64_specific_speedups += f"SRC_C_{mode}({src} {cflags})\n"
    main.before("END", Switch({"ARCH_X86_64 OR ARCH_I686": amd64_specific_speedups}))


blake2 = NixProject(
    arcdir="contrib/libs/blake2",
    nixattr="libb2",
    put={
        "b2": ".",
    },
    inclink={
        "include": ["src/blake2.h"],
    },
    platform_dispatchers=[
        "src/config.h",
    ],
    post_install=post_install,
)
