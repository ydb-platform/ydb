from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        PEERDIR=[
            "contrib/libs/openssl",
        ],
    )


boost_asio = NixSourceProject(
    nixattr="boost_asio",
    arcdir=boost.make_arcdir("asio"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "atomic.h",
        "liburing.h",
        "experimental/coroutine",
        "experimental/source_location",
        "experimental/string_view",
        "BOOST_ASIO_CUSTOM_HANDLER_TRACKING",
        "wolfssl/",
    ],
    post_install=post_install,
)
