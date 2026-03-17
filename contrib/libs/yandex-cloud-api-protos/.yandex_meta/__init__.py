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
        PEERDIR=["contrib/libs/googleapis-common-protos"],
    )
    self.yamakes["."].before("END", "EXCLUDE_TAGS(GO_PROTO)\n")


yandex_cloud_api_protos = NixSourceProject(
    owners=["g:cloud-api", "g:cpp-contrib"],
    arcdir="contrib/libs/yandex-cloud-api-protos",
    nixattr="yandex-cloud-api-protos",
    copy_sources=[
        "yandex/cloud/**/*.proto",
    ],
    post_install=post_install,
)
