from devtools.yamaker import fileutil
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    fileutil.convert_to_utf8(f"{self.dstdir}/README.md", from_="windows-1252")


libraw = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libraw",
    nixattr="libraw",
    flags=[
        "--disable-jasper",
        "--enable-lcms",
    ],
    use_full_libnames=True,
    install_targets=[
        "libraw",
        "libraw_r",
    ],
    put={
        "libraw": ".",
    },
    put_with={
        "libraw": ["libraw_r"],
    },
    addincl_global={".": ["contrib/libs/lcms2/include"]},
    disable_includes=[
        "../../internal/x3f_tools.h",
        "../../RawSpeed/rawspeed_xmldata.cpp",
        "RawSpeed/*",
        "rawspeed3_capi.h",
        "dng_host.h",
        "dng_info.h",
        "dng_negative.h",
        "dng_read_image.h",
        "dng_simple_image.h",
        "dng_stream.h",
        "gpr_read_image.h",
        "jasper/jasper.h",
        "lcms.h",
    ],
    copy_top_sources_except=["DEVELOPER-NOTES"],
    post_install=post_install,
)
