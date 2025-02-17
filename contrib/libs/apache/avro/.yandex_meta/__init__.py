from devtools.yamaker import boost
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.remove("-DAVRO_DYN_LINK")
        m.CFLAGS.remove("-DFMT_HEADER_ONLY=1")
        m.PEERDIR += [
            boost.make_arcdir("any"),
            boost.make_arcdir("asio"),
            boost.make_arcdir("crc"),
            boost.make_arcdir("math"),
        ]


apache_avro = CMakeNinjaNixProject(
    owners=["g:yql", "g:cpp-contrib"],
    arcdir="contrib/libs/apache/avro",
    nixattr="avro-cpp",
    ignore_commands=["tree2", "avrogencpp"],
    nixsrcdir="source/lang/c++",
    copy_sources=[
        "api/*.hh",
    ],
    build_targets=[
        "avrocpp",
    ],
    install_targets=[
        "avrocpp",
    ],
    unbundle_from={
        "fmt": "_deps/fmt-src",
    },
    write_public_incs=False,
    post_install=post_install,
)
