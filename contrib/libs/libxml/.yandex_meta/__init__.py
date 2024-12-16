from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.project import GNUMakeNixProject
from devtools.yamaker.modules import GLOBAL, Linkable, Switch


def libxml2_post_install(self):
    re_sub_dir(self.dstdir + "/include", r"(# *include) <libxml/(.*)>", r'\1 "\2"')

    with self.yamakes["."] as m:
        m.NO_RUNTIME = False
        m.CFLAGS.append(GLOBAL("-DLIBXML_STATIC"))

        # flag disable arcadia encodings to reduce binary weight and remove UTIL dependency
        m.after(
            "SRCS",
            Switch(
                ARCADIA_LIBXML_DISABLE_EXTRA_ENCODINGS=Linkable(
                    CFLAGS=["-DARCADIA_LIBXML_DISABLE_EXTRA_ENCODINGS"],
                    NO_RUNTIME=True,
                ),
                default=Linkable(SRCS=["yencoding.cpp"], PEERDIR=["library/cpp/charset"]),
            ),
        )


libxml = GNUMakeNixProject(
    owners=["g:cpp-contrib", "g:yandex_io"],
    arcdir="contrib/libs/libxml",
    nixattr="libxml2",
    flags=["--without-ftp", "--without-http"],
    install_targets=[
        "libxml2",
        "xmllint",
    ],
    put={
        "libxml2": ".",
        "xmllint": "xmllint",
    },
    use_full_libnames=True,
    addincl_global={".": ["./include"]},
    copy_sources=["Copyright"],
    copy_top_sources_except=[
        "ChangeLog",
        "INSTALL",
        "NEWS",
        "README",
        "README.tests",
        "README.zOS",
    ],
    platform_dispatchers=["config.h"],
    disable_includes=[
        "lzma.h",
        "kernel/image.h",
        "OS.h",
        "os2.h",
        "trio.h",
        # ifdef LIBXML_ICU_ENABLED
        "unicode/",
        # if defined(_WIN32_WCE)
        "win32config.h",
        "xzlib.h",
    ],
    keep_paths=[
        "yencoding.cpp",
        "yencoding.h",
    ],
    post_install=libxml2_post_install,
)

# DOCBparser.c is not used by our targets, do not copy it
libxml.copy_top_sources_except.add("DOCBparser.c")
