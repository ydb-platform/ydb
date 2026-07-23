from devtools.yamaker import proto
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    proto.make_proto_library(
        self,
        "go.opentelemetry.io/proto/otlp",
        ts_proto_dirs=["opentelemetry/proto/collector/trace/v1"],
        ts_proto_package_name="@yandex-proto/contrib-proto-opentelemetry",
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
