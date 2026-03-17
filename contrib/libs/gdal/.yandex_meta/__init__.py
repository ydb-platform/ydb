import shutil

from packaging.version import parse

from devtools.yamaker import fileutil
from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import GLOBAL, Switch, Linkable, Recurse
from devtools.yamaker.project import CMakeNinjaNixProject

FLATBUFFER_SPECS = [
    "ogr/ogrsf_frmts/flatgeobuf/feature.fbs",
    "ogr/ogrsf_frmts/flatgeobuf/header.fbs",
]

MARK_AS_SOURCES = [
    "frmts/grib/degrib/g2clib/enc_jpeg2000.c",
    "frmts/grib/degrib/g2clib/enc_png.c",
    "frmts/grib/degrib/g2clib/g2_addfield.c",
    "frmts/grib/degrib/g2clib/g2_addgrid.c",
    "frmts/grib/degrib/g2clib/g2_create.c",
    "frmts/grib/degrib/g2clib/g2_gribend.c",
    "frmts/grib/degrib/g2clib/getdim.c",
    "frmts/grib/degrib/g2clib/getpoly.c",
    "frmts/grib/degrib/g2clib/jpcpack.c",
    "frmts/grib/degrib/g2clib/pngpack.c",
    "frmts/grib/degrib/g2clib/specpack.c",
    "port/cpl_vsil_win32.cpp",
]

MISSING_INCLS = [
    "contrib/libs/gdal/ogr/ogrsf_frmts/cad/libopencad/dwg",
    "contrib/libs/geos/capi/geos",
    "contrib/libs/geos/include/geos",
]


def post_install(self):
    for yamake in self.yamakes.values():
        if isinstance(yamake, Recurse):
            continue

        # fmt: off
        yamake.CFLAGS = [
            flag
            for flag in yamake.CFLAGS
            if flag not in (
                "-DENABLE_UFFD",
                "-DHAVE_AVX_AT_COMPILE_TIME",
                "-DHAVE_ICONV",
                "-DRENAME_INTERNAL_SHAPELIB_SYMBOLS",
            )
        ]
        # fmt: on

        x64_specific_defines = [
            "-DHAVE_SSE_AT_COMPILE_TIME",
            "-DHAVE_SSSE3_AT_COMPILE_TIME",
        ]
        for flag in x64_specific_defines:
            yamake.CFLAGS.remove(flag)

        yamake.after(
            "CFLAGS",
            Switch(ARCH_X86_64=Linkable(CFLAGS=x64_specific_defines)),
        )

    shutil.copyfile(
        f"{self.dstdir}/gcore/gdal_version_full/gdal_version.h",
        f"{self.dstdir}/gcore/gdal_version.h",
    )

    with self.yamakes["."] as gdal:
        for incl in MISSING_INCLS:
            gdal.ADDINCL.remove(incl)

        gdal.PEERDIR.add("contrib/restricted/fast_float")
        gdal.ADDINCL.add("contrib/restricted/fast_float/include")

        # gdal contains bundled flatbuffers includes,
        # yet the structure is not suitable for unbundle_from
        # unbundling the directory manually
        shutil.rmtree(f"{self.dstdir}/ogr/ogrsf_frmts/flatgeobuf/flatbuffers")
        gdal.ADDINCL.append(ArcPath("contrib/libs/flatbuffers/include"))
        gdal.ADDINCL.remove(f"{self.arcdir}/ogr/ogrsf_frmts/flatgeobuf")
        gdal.ADDINCL.sorted = True

        # fmt: off
        gdal.CFLAGS = sorted(
            flag
            for flag in gdal.CFLAGS
            if not flag.startswith("-DSYSCONFDIR=")
        )
        # fmt: on

        gdal.after(
            "CFLAGS",
            Switch(
                OS_DARWIN=Linkable(
                    CFLAGS=[
                        GLOBAL("-UHAVE_PREAD64"),
                    ]
                )
            ),
        )

        gdal.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=[
                        GLOBAL("-DOCAD_STATIC"),
                        GLOBAL("-DOCAD_EXTERN="),
                    ]
                )
            ),
        )

        gdal.SRCS |= set(FLATBUFFER_SPECS)
        gdal.ADDINCL.add(ArcPath(self.arcdir + "/ogr/ogrsf_frmts/flatgeobuf", build=True))
        gdal.FLATC_FLAGS.add("--scoped-enums")

        gdal.SRCS |= set(MARK_AS_SOURCES)

    # Update version stored in python binding
    version = parse(self.version)
    for python_contrib in ("fiona", "rasterio"):
        fileutil.re_sub_file(
            f"{self.ctx.arc}/contrib/python/{python_contrib}/ya.make",
            r"-ECTE_GDAL_MAJOR_VERSION=\d+",
            f"-ECTE_GDAL_MAJOR_VERSION={version.major}",
        )
        fileutil.re_sub_file(
            f"{self.ctx.arc}/contrib/python/{python_contrib}/ya.make",
            r"-ECTE_GDAL_MINOR_VERSION=\d+",
            f"-ECTE_GDAL_MINOR_VERSION={version.minor}",
        )
        fileutil.re_sub_file(
            f"{self.ctx.arc}/contrib/python/{python_contrib}/ya.make",
            r"-ECTE_GDAL_PATCH_VERSION=\d+",
            f"-ECTE_GDAL_PATCH_VERSION={version.micro}",
        )


gdal = CMakeNinjaNixProject(
    arcdir="contrib/libs/gdal",
    nixattr="gdal",
    cflags=["-DPCIDSK_INTERNAL"],
    copy_sources=MARK_AS_SOURCES
    + [
        "data/",
        "frmts/eeda/data/",
        "frmts/grib/data/",
        "frmts/grib/degrib/data/",
        "frmts/hdf5/data/",
        "frmts/netcdf/data/",
        "frmts/nitf/data/",
        "frmts/pdf/data/",
        "frmts/pds/data/",
        "frmts/vrt/data/",
        "gcore/data/",
        "ogr/data/",
        "ogr/ogrsf_frmts/dgn/data/",
        "ogr/ogrsf_frmts/dxf/data/",
        "ogr/ogrsf_frmts/gml/data/",
        "ogr/ogrsf_frmts/gmlas/data/",
        "ogr/ogrsf_frmts/miramon/data/",
        "ogr/ogrsf_frmts/osm/data/",
        "ogr/ogrsf_frmts/plscenes/data/",
        "ogr/ogrsf_frmts/s57/data/",
        "ogr/ogrsf_frmts/sxf/data/",
        "ogr/ogrsf_frmts/vdv/data/",
        "ogr/ogrsf_frmts/vrt/data/",
        "port/cpl_config_extras.h",
    ],
    keep_paths=FLATBUFFER_SPECS,
    disable_includes=[
        "arrow-adbc/adbc_driver_manager.h",
        "APP/app.h",
        "CL/",
        "Catalog.h",
        "CmptCmp.h",
        "Dict.h",
        "ErrorCodes.h",
        "F64_str.h",
        "FILE_64.h",
        "GlobalParams.h",
        "LIBJPEG_12_PATH",
        "Lerc1Decode/CntZImage.h",
        "Object.h",
        "OpenCL/",
        "PDFDoc.h",
        "Page.h",
        "PrjMMVGl.h",
        "SFCGAL/capi/sfcgal_c.h",
        "SplashOutputDev.h",
        "Stream.h",
        "archive_gdal_config.h",
        "armadillo_headers.h",
        "atlbase.h",
        "bd_xp.h",
        "blosc.h",
        "brunsli/decode.h",
        "brunsli/encode.h",
        "cadenes.h",
        "core/fpdfapi/",
        "core/fpdfdoc/",
        "core/fxcrt/",
        "core/fxge/",
        "core/impparam/impparam.h",
        "cryptopp/",
        "dbfopen.h",
        "deftoler.h",
        "embedded_resources.h",
        "fitxers.h",
        "fpdfsdk/",
        "gdal\\release",
        "gdal_libgeotiff_symbol_rename.h",
        "gdal_libtiff_symbol_rename.h",
        "gdal_shapelib_symbol_rename.h",
        "gdalmmf.h",
        "hdfs.h",
        "include_sse2neon.h",
        "internal_qhull_headers.h",
        "jasper/jasper.h",
        "libdeflate.h",
        "lz4.h",
        "memo.h",
        "memwatch.h",
        "mm_gdal\\",
        "msg.h",
        "nomsfitx.h",
        "ogr_xerces_headers.h",
        "pcre.h",
        "pcre2.h",
        "pdfio.h",
        "podofo.h",
        "public/fpdfview.h",
        "raster/memcmp.hh",
        "raster/memset.hh",
        "rasterlite2/rasterlite2.h",
        "s57tables.h",
        "spatialite.h",
        "spatialite/",
        "splash/",
        "stdfloat",
        "sys/byteorder.h",
        "tdlpack.h",
        "util/Files/EnvisatFile.h",
        "xercesc_headers.h",
        "../jpeg/libjpeg12/jpeglib.h",
        "../third_party/fast_float/fast_float.h",
        r"..\..\ogr\ogrsf_frmts\mitab\mitab.h",
    ],
    platform_dispatchers=["port/cpl_config.h"],
    addincl_global={
        ".": {
            "./alg",
            "./apps",
            "./gcore",
            "./gcore/gdal_version_full",
            "./gnm",
            "./ogr",
            "./port",
            "./ogr/ogrsf_frmts",
        }
    },
    unbundle_from={
        "fast_float": "third_party/fast_float",
    },
    ignore_targets=["my_test_sqlite3_ext"],
    post_install=post_install,
)
