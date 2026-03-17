from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as lexbor:
        lexbor.PEERDIR.add("library/cpp/sanitizer/include")


lexbor = CMakeNinjaNixProject(
    owners=[],
    nixattr="lexbor",
    arcdir="contrib/libs/lexbor",
    copy_sources=[
        "source/lexbor/core/core.h",
        "source/lexbor/html/html.h",
    ],
    # Install only html-related modules.
    # The list of html module is taken from lexbor/README.md
    install_targets=[
        "lexbor-core",
        "lexbor-css",
        "lexbor-dom",
        "lexbor-html",
        "lexbor-ns",
        "lexbor-selectors",
        "lexbor-tag",
    ],
    put={
        "lexbor-core": ".",
    },
    put_with={
        "lexbor-core": [
            "lexbor-css",
            "lexbor-dom",
            "lexbor-html",
            "lexbor-ns",
            "lexbor-selectors",
            "lexbor-tag",
        ],
    },
    post_install=post_install,
)
