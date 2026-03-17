from devtools.yamaker.fileutil import re_sub_file

from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject

MMAP_FILES = [
    "libsrc/mmapio.c",
]

WIN_FILES = [
    "libncpoco/cp_win32.c",
]


def post_install(self: CMakeNinjaNixProject):
    for file in ("dceparse.c", "dceparselex.h", "dcetab.c"):
        re_sub_file(
            f"{self.dstdir}/libdap2/{file}",
            r"\brange\b",
            "dap2_range",
        )
    with self.yamakes["."] as module:
        module.PEERDIR.add("contrib/libs/libc_compat")

        module.SRCS -= set(MMAP_FILES)
        module.after("SRCS", Switch({"NOT OS_WINDOWS": Linkable(SRCS=MMAP_FILES)}))
        module.after("SRCS", Switch({"OS_WINDOWS": Linkable(SRCS=WIN_FILES)}))


netcdf_c = CMakeNinjaNixProject(
    arcdir="contrib/restricted/netcdf-c",
    owners=["g:cpp-contrib"],
    nixattr="netcdf",
    copy_sources=WIN_FILES
    + [
        "include/netcdf_par.h",
        "include/nchttp.h",
        "libhdf5/H5FDhttp.h",
    ],
    disable_includes=[
        "H5FDros3.h",
        "blosc.h",
        "instr.h",
        "mpi.h",
        "pnetcdf.h",
        "pstdint.h",
        "ztracedispatch.h",
    ],
    install_targets=["netcdf"],
    platform_dispatchers=["config.h"],
    post_install=post_install,
)
