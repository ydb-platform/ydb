from devtools.yamaker import boost
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.remove("-DAVRO_DYN_LINK")
        m.PEERDIR += [
            boost.make_arcdir("any"),
            boost.make_arcdir("asio"),
            boost.make_arcdir("crc"),
            boost.make_arcdir("format"),
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
    inclink={
        # ClickHouse invokes CMake INSTALL which copies avro headers
        # from source/lang/c++/api/*.hh to avro/*.hh.
        # Emulate this step via inclink
        "avro": ["api/*.hh"]
    },
    build_targets=[
        "avrocpp",
    ],
    install_targets=[
        "avrocpp",
    ],
    post_install=post_install,
)
