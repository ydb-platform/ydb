from devtools.yamaker.project import CMakeNinjaNixProject


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
        "libutf8_validity",
    ],
    put={
        "libutf8_range": ".",
    },
    put_with={
        "libutf8_range": ["libutf8_validity"],
    },
    copy_sources=[
        "utf8_range.h",
    ],
    use_provides={
        "contrib/restricted/abseil-cpp/.yandex_meta",
    },
)
