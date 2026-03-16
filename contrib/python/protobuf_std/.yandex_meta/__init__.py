from devtools.yamaker.modules import py_srcs
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    # Pythonize.
    with self.yamakes["."] as pb:
        pb.to_py_library(
            PY_SRCS=py_srcs(self.dstdir),
            RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
        )
        pb.PY_REGISTER.add("google._upb._message")

        pb.PEERDIR.add("contrib/libs/protobuf_std/builtin_proto/protos_from_protobuf")
        pb.PEERDIR.add("contrib/libs/protobuf_std/builtin_proto/protos_from_protoc")
        pb.PEERDIR.add("contrib/restricted/google/upb")
        pb.ADDINCL.add("contrib/restricted/google/upb")
        pb.PEERDIR.add("contrib/restricted/google/utf8_range")

        # unbundle utf8_range and upb runtimes
        # fmt: off
        pb.SRCS = [
            src
            for src in pb.SRCS
            if src.startswith("python") or src.startswith("google")
        ]
        # fmt: on


python_protobuf_std = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/protobuf_std",
    nixattr=python.make_nixattr("protobuf"),
    copy_sources=["google/**/*.py"],
    copy_sources_except=[
        "google/**/*_pb2.py",
        "google/protobuf/compiler/**",
        "google/protobuf/testdata/**",
    ],
    build_install_subdir=python.BUILD_INSTALL_SUBDIR,
    unbundle_from={
        "upb": "upb",
        "utf8_range": "utf8_range",
    },
    post_install=post_install,
)
