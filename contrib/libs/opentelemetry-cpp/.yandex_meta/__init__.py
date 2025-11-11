from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker.modules import GLOBAL, Library, Linkable, Switch


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.remove("-DOPENTELEMETRY_STL_VERSION=2023")
        m.CFLAGS.remove("-DPROTOBUF_USE_DLLS")
        m.CFLAGS.remove("-DOPENTELEMETRY_ABI_VERSION_NO=1")
        m.CFLAGS.append(GLOBAL("-DOPENTELEMETRY_STL_VERSION=2023"))
        m.CFLAGS.append(GLOBAL("-DOPENTELEMETRY_ABI_VERSION_NO=2"))

        self.yamakes["api"] = self.module(
            Library,
            CFLAGS=m.CFLAGS,
            ADDINCL=[GLOBAL(f"{self.arcdir}/api/include")],
            NO_UTIL=True,
            NO_COMPILER_WARNINGS=True,
        )

        m.SRCS.remove("sdk/src/common/platform/fork_unix.cc")
        m.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(SRCS=["sdk/src/common/platform/fork_windows.cc"]),
                default=Linkable(SRCS=["sdk/src/common/platform/fork_unix.cc"]),
            ),
        )

        m.PEERDIR.add(f"{self.arcdir}/api")


opentelemetry_cpp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    nixattr="opentelemetry-cpp",
    arcdir="contrib/libs/opentelemetry-cpp",
    unbundle_from={
        "opentelemetry-proto": "generated/third_party/opentelemetry-proto",
    },
    copy_sources=[
        "api/include/opentelemetry/",
        "exporters/otlp/include/",
        "sdk/include/",
        "sdk/src/common/platform/fork_windows.cc",
    ],
    disable_includes={
        "gsl/gsl",
    },
    put={
        "opentelemetry_common": ".",
    },
    put_with={
        "opentelemetry_common": [
            "opentelemetry_exporter_in_memory",
            "opentelemetry_exporter_ostream_metrics",
            "opentelemetry_exporter_ostream_span",
            "opentelemetry_exporter_ostream_logs",
            "opentelemetry_exporter_otlp_grpc",
            "opentelemetry_exporter_otlp_grpc_client",
            "opentelemetry_exporter_otlp_grpc_log",
            "opentelemetry_exporter_otlp_grpc_metrics",
            "opentelemetry_exporter_otlp_http",
            "opentelemetry_exporter_otlp_http_client",
            "opentelemetry_exporter_otlp_http_log",
            "opentelemetry_exporter_otlp_http_metric",
            "opentelemetry_exporter_in_memory_metric",
            "opentelemetry_http_client_curl",
            "opentelemetry_metrics",
            "opentelemetry_otlp_recordable",
            "opentelemetry_resources",
            "opentelemetry_trace",
            "opentelemetry_version",
            "opentelemetry_logs",
        ]
    },
    ignore_commands=[
        "protoc",
        "protoc-24.4.0",
    ],
    ignore_targets=[
        "opentelemetry_proto",
        "opentelemetry_proto_grpc",
        "func_otlp_grpc",
        "func_otlp_http",
    ],
    addincl_global={
        ".": {
            "contrib/libs/opentelemetry-cpp/exporters/memory/include",
            "contrib/libs/opentelemetry-cpp/exporters/ostream/include",
            "contrib/libs/opentelemetry-cpp/exporters/otlp/include",
            "contrib/libs/opentelemetry-cpp/ext/include",
            "contrib/libs/opentelemetry-cpp/sdk/include",
        },
    },
    post_install=post_install,
)
