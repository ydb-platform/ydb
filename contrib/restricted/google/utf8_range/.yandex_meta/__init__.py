from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as utf8range:
        # utf8_validity.h depends on abseil-cpp
        utf8range.PEERDIR.add("contrib/restricted/abseil-cpp")
        # bestow compatibility with contrib/libs/grpc
        utf8range.SRCS |= {
            "naive.c",
            "range2-neon.c",
            "range2-sse.c",
        }


# utf8_range was merged into protobuf project
utf8_range = CMakeNinjaNixProject(
    arcdir="contrib/restricted/google/utf8_range",
    nixattr="protobuf",
    cmake_subdir="third_party/utf8_range",
    build_subdir="third_party/utf8_range/build",
    install_subdir="third_party/utf8_range",
    use_full_libnames=True,
    install_targets=[
        "libutf8_range",
    ],
    copy_sources=[
        "naive.c",
        "range2-neon.c",
        "range2-sse.c",
        # protobuf gets compiled with -march=x86_64, hence these files are not included by default
        "utf8_range_sse.inc",
        "utf8_range_neon.inc",
        # utf8_validity is a header-only library, copy file manually
        "utf8_validity.h",
    ],
    use_provides={
        "contrib/restricted/abseil-cpp/.yandex_meta",
    },
    post_install=post_install,
)
