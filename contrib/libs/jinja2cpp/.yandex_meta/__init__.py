import os

from devtools.yamaker import boost
from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.project import CMakeNinjaNixProject


HEADERS_FROM_STD = ["optional", "string_view", "variant"]
TOKENS_FROM_STD = [
    "basic_string_view",
    "get",
    "get_if",
    "optional",
    "string_view",
    "variant",
    "visit",
    "wstring_view",
]


def post_install(self):
    with self.yamakes["."] as jinja2cpp:
        # Emulate -DJINJA2CPP_WITH_JSON_BINDINGS=rapid which is broken in upstream
        os.remove(f"{self.dstdir}/src/binding/boost_json_serializer.h")
        os.remove(f"{self.dstdir}/src/binding/boost_json_serializer.cpp")
        jinja2cpp.SRCS.remove("src/binding/boost_json_serializer.cpp")

        jinja2cpp.PEERDIR += [
            boost.make_arcdir("algorithm"),
            boost.make_arcdir("container"),
            boost.make_arcdir("numeric_conversion"),
            boost.make_arcdir("unordered"),
            boost.make_arcdir("variant"),
        ]

        # use optional, string_view and variant from std without relying onto nonstd proxies
        for header in HEADERS_FROM_STD:
            fileutil.re_sub_dir(
                self.dstdir,
                f"<nonstd/{header}.hpp>",
                f"<{header}>",
                test=pathutil.is_preprocessable,
            )

        for token in TOKENS_FROM_STD:
            fileutil.re_sub_dir(
                self.dstdir,
                rf"nonstd::{token}\b",
                f"std::{token}",
                test=pathutil.is_preprocessable,
            )

        # use absolute includes for nonstd/expected.hpp
        fileutil.re_sub_dir(
            self.dstdir,
            "<nonstd/expected.hpp>",
            "<contrib/restricted/expected-lite/include/nonstd/expected.hpp>",
            test=pathutil.is_preprocessable,
        )


jinja2cpp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/jinja2cpp",
    nixattr="jinja2cpp",
    disable_includes=[
        "binding/boost_json_serializer.h",
    ],
    post_install=post_install,
    addincl_global={".": {"contrib/libs/rapidjson/include"}},
)
