import os

from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    os.remove(f"{self.dstdir}/src/inlines.cpp")

    with self.yamakes["."] as geos:
        # -DGEOS_INLINE -DDLL_EXPORT can be used to put all inlinable methods into libgeos-${version}.so
        geos.CFLAGS.remove("-DDLL_EXPORT")
        geos.CFLAGS.remove("-DGEOS_INLINE")
        geos.SRCS.remove("src/inlines.cpp")

    with self.yamakes["capi"] as capi:
        capi.PEERDIR.add(self.arcdir)
        capi.CFLAGS.remove("-DDLL_EXPORT")
        capi.CFLAGS.remove("-DGEOS_INLINE")


geos = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/geos",
    nixattr="geos",
    install_targets=["geos", "geos_c"],
    put={"geos_c": "capi"},
    copy_sources=[
        # Copy all the includes, including the ones that were not used during static library compilation
        "include/**/*.h",
    ],
    addincl_global={
        ".": ["./include"],
    },
    keep_paths=[
        # TODO: maybe we should move ctypes into shapely, the only library client
        "capi/ctypes",
    ],
    post_install=post_install,
)
