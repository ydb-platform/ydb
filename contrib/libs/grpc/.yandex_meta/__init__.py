import os.path
import shutil

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    def _ignore_paths(p):
        return 'impl/status.h' not in p

    # Change std::string to TString
    re_sub_dir(self.dstdir, r"\bstd::string\b", "TString", test=_ignore_paths)
    re_sub_dir(self.dstdir, r"\bstd::to_string\b", "::ToString", test=_ignore_paths)
    re_sub_dir(
        self.dstdir,
        "#include <string>",
        """
#include <util/generic/string.h>
#include <util/string/cast.h>
""".strip(),
        test=_ignore_paths,
    )
    # Change absl to y_absl
    re_sub_dir(self.dstdir, r"\babsl\b", "y_absl")
    re_sub_dir(self.dstdir, r"\bABSL_", "Y_ABSL_")


def post_install(self):
    def fix_ssl_certificates():
        with self.yamakes["."] as m:
            m.SRCS.add("src/core/lib/security/security_connector/add_arcadia_root_certs.cpp")
            m.PEERDIR |= {"certs", "library/cpp/resource"}

    for name, m in self.yamakes.items():
        with m:
            if hasattr(m, "CFLAGS"):
                m.before(
                    "SRCS",
                    Switch(
                        {
                            "OS_LINUX OR OS_DARWIN": Linkable(
                                CFLAGS=[
                                    "-DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1",
                                ]
                            ),
                        }
                    ),
                )

            if hasattr(m, "NO_UTIL"):
                m.NO_UTIL = False

    fix_ssl_certificates()

    # remove proto sources as we use contrib/proto/grpc instead
    shutil.rmtree(f"{self.dstdir}/protos")

    # Let grpc++_reflection register itself (r6449480).
    with self.yamakes["grpc++_reflection"] as m:
        # Mark proto_server_reflection_plugin.cc as GLOBAL
        m.SRCS.remove("src/cpp/ext/proto_server_reflection_plugin.cc")
        m.SRCS.add(GLOBAL("src/cpp/ext/proto_server_reflection_plugin.cc"))
        # Use protos from contrib/proto/grpc
        m.SRCS.remove("protos/src/proto/grpc/reflection/v1alpha/reflection.proto")
        m.PEERDIR.add("contrib/proto/grpc/grpc/reflection/v1alpha")
        m.SRCS.remove("protos/src/proto/grpc/reflection/v1/reflection.proto")
        m.PEERDIR.add("contrib/proto/grpc/grpc/reflection/v1")
        m.ADDINCL.remove("contrib/libs/grpc/protos")

    with self.yamakes["grpcpp_channelz"] as m:
        m.SRCS.remove("protos/src/proto/grpc/channelz/channelz.proto")
        m.PEERDIR.add("contrib/proto/grpc/grpc/channelz/v1")
        m.ADDINCL.remove("contrib/libs/grpc/protos")

    # fix induced deps
    for name, module in self.yamakes.items():
        if "-DPROTOBUF_USE_DLLS" in module.CFLAGS:
            module.CFLAGS.remove("-DPROTOBUF_USE_DLLS")
        addincls = getattr(module, "ADDINCL", None)
        source_addincl = ArcPath("contrib/libs/grpc", build=False)
        build_addincl = ArcPath("contrib/libs/grpc", build=True)
        if addincls and source_addincl in addincls:
            addincls.add(build_addincl)

    # unbundle third_party/utf_range manually merged into upb library
    with self.yamakes["third_party/upb"] as upb:
        # fmt: off
        upb.SRCS = [
            src
            for src in upb.SRCS
            if "utf8_range" not in src
        ]
        # fmt: on
        upb.PEERDIR.add("contrib/restricted/google/utf8_range")
        upb.ADDINCL.add("contrib/restricted/google/utf8_range")

    with self.yamakes["."] as m:
        # fmt: off
        m.RECURSE += [
            os.path.dirname(path)
            for path in self.keep_paths
            if path.endswith("/ya.make")
        ]
        # fmt: on


grpc = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/grpc",
    nixattr="grpc",
    license="Apache-2.0",
    keep_paths=[
        "src/core/lib/security/security_connector/add_arcadia_root_certs.*",
    ],
    ignore_targets=[
        "check_epollexclusive",
        "gen_hpack_tables",
        "grpc++_alts",
        "grpc++_error_details",
        "grpc++_unsecure",
        "grpc_unsecure",
    ],
    install_targets=[
        "gpr",
        "grpc",
        "grpc++",
        "grpc++_reflection",
        "grpc_cpp_plugin",
        "grpc_plugin_support",
        "grpc_python_plugin",
        "grpcpp_channelz",
        # third_party libraries
        "address_sorting",
        "upb",
        "upb_json_lib",
        "upb_textformat_lib",
        "utf8_range_lib",
    ],
    put={
        "grpc": ".",
        "grpc_cpp_plugin": "src/compiler/grpc_cpp_plugin",
        "grpc_plugin_support": "src/compiler/grpc_plugin_support",
        "grpc_python_plugin": "src/compiler/grpc_python_plugin",
        "grpcpp_channelz": "grpcpp_channelz",
        # third_party libraries
        "address_sorting": "third_party/address_sorting",
        "upb": "third_party/upb",
        "utf8_range_lib": "third_party/utf8_range",
    },
    put_with={
        "grpc": ["grpc++", "gpr"],
        "upb": ["upb_json_lib", "upb_textformat_lib"],
    },
    unbundle_from={
        "xxhash": "third_party/xxhash",
        "utf8_validity": "third_party/upb/third_party/utf8_range",
        "utf8_range": "third_party/utf8_range",
        "utf8_range_lib": "third_party/utf8_range",
    },
    copy_sources=[
        "include/**/*.h",
        "src/core/ext/transport/binder/utils/binder_auto_utils.h",
        "src/core/lib/gpr/string_windows.h",
        "src/core/lib/iomgr/ev_apple.h",
        "src/core/lib/iomgr/iocp_windows.h",
        "src/core/lib/iomgr/pollset_windows.h",
        "src/core/lib/iomgr/pollset_set_windows.h",
        "src/core/lib/iomgr/python_util.h",
        "src/core/lib/iomgr/resolve_address_windows.h",
        "src/core/lib/iomgr/socket_windows.h",
        "src/core/lib/iomgr/tcp_windows.h",
        "src/core/lib/event_engine/cf_engine/*.h",
        "src/core/lib/event_engine/nameser.h",
        "src/core/lib/event_engine/windows/grpc_polled_fd_windows.h",
        "src/core/lib/event_engine/windows/windows_endpoint.h",
        "src/core/lib/event_engine/windows/windows_engine.h",
        "src/core/lib/event_engine/windows/windows_listener.h",
        "src/core/lib/event_engine/windows/win_socket.h",
        "src/core/lib/event_engine/windows/iocp.h",
        "src/core/lib/event_engine/socket_notifier.h",
        "src/core/lib/event_engine/poller.h",
    ],
    disable_includes=[
        # if OPENSSL_VERSION_NUMBER >= 0x30000000L
        "openssl/param_build.h",
        "src/core/lib/profiling/stap_probes.h",
        # ifdef GRPC_UV
        "uv.h",
        # ifdef GRPC_USE_EVENT_ENGINE
        "src/core/lib/iomgr/resource_quota.h",
        # ifdef GRPC_CFSTREAM
        "src/core/lib/iomgr/cfstream_handle.h",
        "src/core/lib/iomgr/endpoint_cfstream.h",
        "src/core/lib/iomgr/error_cfstream.h",
        "src/core/lib/iomgr/event_engine/closure.h",
        "src/core/lib/iomgr/event_engine/endpoint.h",
        "src/core/lib/iomgr/event_engine/pollset.h",
        "src/core/lib/iomgr/event_engine/promise.h",
        "src/core/lib/iomgr/event_engine/resolver.h",
        "src/core/lib/iomgr/resolve_address_impl.h",
        "src/core/lib/gpr/string_windows.h",
        "systemd/sd-daemon.h",
    ],
    use_provides=[
        "contrib/restricted/abseil-cpp-tstring/.yandex_meta",
    ],
    post_build=post_build,
    post_install=post_install,
)
