import os

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    protos = []
    for root, _, files in os.walk(self.dstdir):
        rel_root = os.path.relpath(root, self.dstdir)
        protos += [f"{rel_root}/{file}" for file in files if file.endswith("proto")]

    self.yamakes["."] = self.module(
        module="PROTO_LIBRARY",
        SRCS=protos,
        GRPC=True,
        PROTO_NAMESPACE=ArcPath(self.arcdir, GLOBAL=True),
        PY_NAMESPACE=".",
    )
    self.yamakes["."].before("END", "EXCLUDE_TAGS(GO_PROTO)\n")
    self.yamakes["."].before("END", "INCLUDE_TAGS(DOCS_PROTO)\n")


googleapis_common_protos = NixSourceProject(
    arcdir="contrib/libs/googleapis-common-protos",
    nixattr="googleapis-common-protos",
    copy_sources=[
        "google/**/*.proto",
    ],
    owners=["g:cpp-contrib"],
    post_install=post_install,
)
