import datetime

from devtools.yamaker.fileutil import re_sub_file
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as eccodes:
        eccodes.RUN_PROGRAM[0].record.Cmd.Args = [
            arg.replace(
                f"{self.builddir}/",
                f"${{ARCADIA_BUILD_ROOT}}/{self.arcdir}/",
            )
            for arg in eccodes.RUN_PROGRAM[0].record.Cmd.Args
        ]

        # Fix codes_get_build_date() to stabilize reimport
        today = datetime.date.today()
        re_sub_file(
            f"{self.dstdir}/src/grib_api_version.cc",
            f'return "{today.year:04d}.{today.month:02d}.{today.day:02d}";',
            'return "1970.01.01";',
        )


eccodes = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/eccodes",
    nixattr="eccodes",
    disable_includes=[
        "config.h",
        "eckit/",
        "geo/EckitMainInit.h",
        "geo/GeoIterator.h",
        "geo/GribSpec.h",
        "grib_accessor_factory.h",
        "grib_bits_any_endian_omp.cc",
        "grib_bits_any_endian_vector.cc",
        "grib_bits_fast_big_endian.cc",
        "grib_bits_ibmpow.cc",
        "grib_box_factory.h",
        "grib_templates.h",
        "jasper/jasper.h",
        "libhpc.h",
        "minmax_val.cc",
        "wingetopt.h",
    ],
    empty_files=[
        "eccodes_ecbuild_config.h",
    ],
    install_targets={
        "eccodes",
        "eccodes_memfs",
    },
    put={
        "eccodes": ".",
    },
    put_with={
        "eccodes": {"eccodes_memfs"},
    },
    build_targets=["eccodes", "tools/all"],
    post_install=post_install,
    write_public_incs=False,
)
eccodes.copy_top_sources_except |= {
    "definitions/metar/CCCC.txt",
    "definitions/taf/CCCC.txt",
}
