from devtools.yamaker.project import CMakeNinjaNixProject

DUPLICATE_SYMS = [
    "sfparse_base64decode",
    "sfparse_parser_dict",
    "sfparse_parser_inner_list",
    "sfparse_parser_init",
    "sfparse_parser_item",
    "sfparse_parser_list",
    "sfparse_parser_param",
    "sfparse_pctdecode",
    "sfparse_unescape",
]


def post_install(self):
    # Prefix sfparse_parser_dict to avoid conflicts with nghttp2 which also bundles sfparse library
    with self.yamakes["."] as m:
        for sym in DUPLICATE_SYMS:
            m.CFLAGS.append(f"-D{sym}=nghttp3_{sym}")


nghttp3 = CMakeNinjaNixProject(
    license="MIT",
    flags=["-DENABLE_LIB_ONLY=1"],
    owners=["g:devtools-contrib", "g:yandex-io"],
    nixattr="nghttp3",
    arcdir="contrib/libs/nghttp3",
    post_install=post_install,
    platform_dispatchers=["config.h"],
    addincl_global={".": {"./lib/includes"}},
)

nghttp3.copy_top_sources_except |= {
    # This is just a git log, ignore it
    "ChangeLog",
}
