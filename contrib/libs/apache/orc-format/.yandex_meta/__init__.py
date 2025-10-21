from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    protos = fileutil.files(self.dstdir, rel=True, test=pathutil.is_proto)

    self.yamakes["."] = self.module(
        module="PROTO_LIBRARY",
        SRCS=protos,
        # The only known client (that is, apache/orc)
        # includes these protos as "orc_proto.pb.h" without prefix
        PROTO_NAMESPACE=ArcPath(f"{self.arcdir}/src/main/proto/orc/proto", GLOBAL=True),
        PY_NAMESPACE="src/main/proto/orc/proto",
    )
    self.yamakes["."].before("END", "EXCLUDE_TAGS(GO_PROTO)\n")


apache_orc_format = NixSourceProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/apache/orc-format",
    nixattr="apache-orc-format",
    copy_sources=[
        "src/main/proto/orc/proto/*.proto",
    ],
    post_install=post_install,
)
