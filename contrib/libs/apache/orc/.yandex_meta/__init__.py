import os

from devtools.yamaker.project import CMakeNinjaNixProject


def apache_orc_post_install(self):
    with self.yamakes["."] as orc:
        proto_wrapper_source = "c++/src/wrap/orc-proto-wrapper.cc"
        os.remove(f"{self.dstdir}/{proto_wrapper_source}")
        orc.SRCS.remove(proto_wrapper_source)

        orc.PEERDIR.add("contrib/libs/apache/orc-format")
        orc.SRCS.remove("orc-format_ep-prefix/src/orc-format_ep/src/main/proto/orc/proto/orc_proto.proto")
        orc.CFLAGS.remove("-DPROTOBUF_USE_DLLS")


apache_orc = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/apache/orc",
    nixattr="apache-orc",
    ignore_commands=["cmake"],
    install_targets=["orc"],
    disable_includes=[
        "BpackingAvx512.hh",
        "sparsehash/dense_hash_map",
    ],
    addincl_global={".": {"./c++/include"}},
    platform_dispatchers=[
        "c++/src/Adaptor.hh",
    ],
    unbundle_from={
        "orc_format": "orc-format_ep-prefix",
    },
    post_install=apache_orc_post_install,
)
