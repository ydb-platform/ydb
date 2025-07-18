from devtools.yamaker import fileutil
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    # Avoid accidental inclusions between flatbuffers and flatbuffers64 headers.
    fileutil.re_sub_dir(self.dstdir + "/include/flatbuffers", '#include "flatbuffers/', '#include "')

    with self.yamakes["flatc"] as m:
        m.after(
            "PROGRAM",
            """
            INDUCED_DEPS(
                h+cpp
                ${ARCADIA_ROOT}/contrib/libs/flatbuffers/include/flatbuffers/flatbuffers.h
                ${ARCADIA_ROOT}/contrib/libs/flatbuffers/include/flatbuffers/flatbuffers_iter.h
            )
            """,
        )
        m.SRCS.add("src/idl_gen_cpp_yandex_maps_iter.cpp")

    with self.yamakes["."] as m:
        # Remove ADDINCL GLOBAL to keep status quo.
        m.ADDINCL.get(self.arcdir + "/include").GLOBAL = False


flatbuffers = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/flatbuffers",
    nixattr="flatbuffers",
    put={
        "flatbuffers": ".",
        "flatc": "flatc",
    },
    copy_sources=[
        "include/flatbuffers/*.h",
    ],
    keep_paths=[
        "include/flatbuffers/flatbuffers_iter.h",
        "src/idl_gen_cpp_yandex_maps_iter.cpp",
        "src/idl_gen_cpp_yandex_maps_iter.h",
    ],
    disable_includes=[
        "absl/",
        "experimental/string_view",
        "utility.h",
        "FLATBUFFERS_ASSERT_INCLUDE",
        "FLATBUFFERS64_ASSERT_INCLUDE",
    ],
    post_install=post_install,
)
