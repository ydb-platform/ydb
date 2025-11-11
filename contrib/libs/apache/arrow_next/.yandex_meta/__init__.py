import os
import shutil

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import pathutil
from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def _iterate_yamakes_section(yamakes, section_name, handler):
    for prj, m in list(yamakes.items()):
        if not hasattr(m, section_name):
            continue

        section = getattr(m, section_name)
        handler(section)


def _remove_yamakes_section_entries(yamakes, section_name, entries):
    def _remove_entry(section):
        for entry_name in entries:
            if entry_name not in section:
                continue

            section.remove(entry_name)

    _iterate_yamakes_section(yamakes, section_name, _remove_entry)


def _replace_yamakes_section_entries(yamakes, section_name, from_to_entries):
    def _replace_entry(section):
        for from_entry, to_entry in from_to_entries:
            if from_entry not in section:
                continue

            section.remove(from_entry)
            section.add(to_entry)

    _iterate_yamakes_section(yamakes, section_name, _replace_entry)


def post_build(self):
    fileutil.copy(f"{self.srcdir}/cpp/src/arrow/ipc/feather.fbs", f"{self.dstdir}/cpp/src/generated")
    fileutil.copy(f"{self.srcdir}/format/File.fbs", f"{self.dstdir}/cpp/src/generated")
    fileutil.copy(f"{self.srcdir}/format/Message.fbs", f"{self.dstdir}/cpp/src/generated")
    fileutil.copy(f"{self.srcdir}/format/Schema.fbs", f"{self.dstdir}/cpp/src/generated")
    fileutil.copy(f"{self.srcdir}/format/SparseTensor.fbs", f"{self.dstdir}/cpp/src/generated")
    fileutil.copy(f"{self.srcdir}/format/Tensor.fbs", f"{self.dstdir}/cpp/src/generated")


def post_install(self):
    with self.yamakes["."] as arrow:
        arrow.CFLAGS = [
            GLOBAL("-DARROW_STATIC"),
            GLOBAL("-DPARQUET_STATIC"),
        ] + arrow.CFLAGS
        # Building python extension for arrow automatically enables HDFS dependency.
        # We will patch the source code to avoid this dependency.
        # arrow.CFLAGS.remove("-DARROW_HDFS")
        arrow.CFLAGS.remove("-DHAVE_NETINET_IN_H")
        arrow.after(
            "CFLAGS",
            Switch({"NOT OS_WINDOWS": Linkable(CFLAGS=["-DHAVE_NETINET_IN_H"])}),
        )

    with self.yamakes["."] as arrow:
        arrow.SRCS |= set(
            [
                "cpp/src/generated/feather.fbs",
                "cpp/src/generated/File.fbs",
                "cpp/src/generated/Message.fbs",
                "cpp/src/generated/Schema.fbs",
                "cpp/src/generated/SparseTensor.fbs",
                "cpp/src/generated/Tensor.fbs",
            ]
        )
        arrow.ADDINCL += [
            ArcPath(self.arcdir + "/cpp/src", GLOBAL=True, build=True),
        ]

    # Unvendor fast_float
    shutil.rmtree(f"{self.dstdir}/cpp/src/arrow/vendored/fast_float")
    with self.yamakes["."] as arrow:
        arrow.PEERDIR.add("contrib/restricted/fast_float")

    fileutil.re_sub_dir(
        self.dstdir,
        "arrow/vendored/fast_float",
        "contrib/restricted/fast_float/include/fast_float",
        test=pathutil.is_preprocessable,
    )
    fileutil.re_sub_dir(
        self.dstdir,
        "::arrow_vendored::fast_float::",
        "fast_float::",
        test=pathutil.is_preprocessable,
    )

    # Unvendor double-conversion
    fileutil.re_sub_dir(
        f"{self.dstdir}/cpp/src/arrow/util",
        "arrow/vendored/double-conversion",
        "contrib/libs/double-conversion/double-conversion",
    )

    with self.yamakes["."] as arrow:
        arrow.PEERDIR.add("contrib/libs/double-conversion")

    double_conversion_path = f"{self.dstdir}/cpp/src/arrow/vendored/double-conversion"
    double_conversion_files = fileutil.files(double_conversion_path, rel=self.dstdir)
    _remove_yamakes_section_entries(self.yamakes, "SRCS", double_conversion_files)
    shutil.rmtree(double_conversion_path)

    # Unvendor uriparser
    fileutil.re_sub_dir(
        f"{self.dstdir}/cpp/src/arrow/util",
        "arrow/vendored/uriparser",
        "contrib/restricted/uriparser/include/uriparser",
    )

    with self.yamakes["."] as arrow:
        arrow.PEERDIR.add("contrib/restricted/uriparser")

    uriparser_path = f"{self.dstdir}/cpp/src/arrow/vendored/uriparser"
    uriparser_files = fileutil.files(uriparser_path, rel=self.dstdir)
    _remove_yamakes_section_entries(self.yamakes, "SRCS", uriparser_files)
    shutil.rmtree(uriparser_path)

    # Unvendor xxhash
    fileutil.re_sub_dir(
        f"{self.dstdir}/cpp/src/arrow/util",
        "arrow/vendored/xxhash.h",
        "contrib/libs/xxhash/xxhash.h",
    )

    with self.yamakes["."] as arrow:
        arrow.PEERDIR.add("contrib/libs/xxhash")

    xxhash_path = f"{self.dstdir}/cpp/src/arrow/vendored/xxhash"
    # NOTE: There are no SRCS for xxhash, skipped removing from yamakes
    os.remove(f"{self.dstdir}/cpp/src/arrow/vendored/xxhash.h")
    shutil.rmtree(xxhash_path)

    # Unbundle header-only usage of flatbuffers
    fb_include_original = f"{self.arcdir}/cpp/thirdparty/flatbuffers/include"
    fb_include_ours = "contrib/libs/flatbuffers/include"

    _replace_yamakes_section_entries(
        self.yamakes,
        "ADDINCL",
        [
            (fb_include_original, fb_include_ours),
        ],
    )
    shutil.rmtree(f"{self.dstdir}/cpp/thirdparty/flatbuffers")

    # Cleanup unused hdfs include files
    thirdparty_hadoop_include = f"{self.arcdir}/cpp/thirdparty/hadoop/include"

    _remove_yamakes_section_entries(self.yamakes, "ADDINCL", [thirdparty_hadoop_include])
    shutil.rmtree(f"{self.dstdir}/cpp/thirdparty/hadoop")

    # Cleanup unused hdfs io files
    hadoop_related_sources = [
        "cpp/src/arrow/filesystem/hdfs.cc",
        "cpp/src/arrow/io/hdfs.cc",
        "cpp/src/arrow/io/hdfs_internal.cc",
    ]
    hadoop_related_headers = [
        "cpp/src/arrow/filesystem/hdfs.h",
        "cpp/src/arrow/io/hdfs.h",
        "cpp/src/arrow/io/hdfs_internal.h",
    ]
    hadoop_related_files = hadoop_related_sources + hadoop_related_headers

    _remove_yamakes_section_entries(self.yamakes, "SRCS", hadoop_related_sources)
    for fname in hadoop_related_files:
        os.remove(f"{self.dstdir}/{fname}")

    # remove bundled apache_orc interface
    _remove_yamakes_section_entries(self.yamakes, "ADDINCL", [f"{self.arcdir}/orc_ep-install/include"])
    with self.yamakes["."] as arrow:
        arrow.ADDINCL.add("contrib/libs/apache/orc/c++/include")
        arrow.PEERDIR.add("contrib/libs/apache/orc")
        shutil.rmtree(f"{self.dstdir}/orc_ep-install")

    # Cleanup thirdparty (will fail if not empty)
    shutil.rmtree(f"{self.dstdir}/cpp/thirdparty")


apache_arrow = CMakeNinjaNixProject(
    owners=["primorial", "g:cpp-contrib"],
    arcdir="contrib/libs/apache/arrow_next",
    nixattr="arrow-cpp",
    ignore_commands=["cmake"],
    install_targets={"arrow", "parquet"},
    put_with={"arrow": ["parquet"]},
    put={"arrow": "."},
    addincl_global={".": {"./cpp/src", "./src"}},
    copy_sources=[
        "cpp/src/arrow/api.h",
        "cpp/src/arrow/io/api.h",
        "cpp/src/arrow/io/mman.h",
        "cpp/src/arrow/ipc/api.h",
        "cpp/src/arrow/compute/api.h",
        "cpp/src/arrow/csv/api.h",
        "cpp/src/arrow/vendored/datetime/ios.*",
        "cpp/src/arrow/filesystem/api.h",
        "cpp/src/parquet/api/*.h",
    ],
    disable_includes=[
        "knownfolders.h",
        "boost/multiprecision/cpp_int.hpp",
        "boost/shared_ptr.hpp",
        "curl.h",
        "curl/curl.h",
        "mimalloc.h",
        "jemalloc_ep/dist/include/jemalloc/jemalloc.h",
        "glog/logging.h",
        "xsimd/xsimd.hpp",
        "arrow/filesystem/s3fs.h",
        "arrow/filesystem/hdfs.h",
        "arrow/io/hdfs.h",
        "arrow/io/hdfs_internal.h",
        "arrow/util/bpacking_avx2.h",
        "arrow/util/bpacking_avx512.h",
        "arrow/util/bpacking_neon.h",
        # if defined(__sun__)
        "sys/byteorder.h",
    ],
    post_build=post_build,
    post_install=post_install,
)
