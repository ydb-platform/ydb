from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    protos = fileutil.files(self.dstdir, rel=True, test=pathutil.is_proto)

    self.yamakes["."] = self.module(
        module="PROTO_LIBRARY",
        SRCS=protos,
        GRPC=True,
        PROTO_NAMESPACE=ArcPath(self.arcdir, GLOBAL=True),
        PY_NAMESPACE=".",
        INCLUDE_TAGS=["TS_PROTO", "TS_PREPARE_DEPS"],
    )


opentelemetry_proto = NixSourceProject(
    owners=[],
    arcdir="contrib/proto/opentelemetry",
    nixattr="opentelemetry-proto",
    copy_sources=[
        "opentelemetry/proto/**/*.proto",
    ],
    post_install=post_install,
)
