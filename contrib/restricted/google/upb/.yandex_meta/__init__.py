from devtools.yamaker.project import CMakeNinjaNixProject

upb = CMakeNinjaNixProject(
    arcdir="contrib/restricted/google/upb",
    nixattr="protobuf",
    use_full_libnames=True,
    install_targets=[
        "libupb",
    ],
    put={
        "libupb": ".",
    },
    put_with={
        "libprotobuf": ["libprotobuf-lite"],
        "libutf8_range": ["libutf8_validity"],
    },
    unbundle_from={
        "protobuf": "src",
        "utf8_range": "third_party/utf8_range",
    },
    use_provides=[
        "contrib/restricted/abseil-cpp/.yandex_meta",
    ],
)
