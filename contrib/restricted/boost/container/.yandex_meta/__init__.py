from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self, populate_srcs=True)
    with self.yamakes["."] as container:
        # remove dlmalloc sources, they are used via #include
        container.SRCS = [src for src in container.SRCS if not src.endswith("2_8_6.c")]

        container.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_CONTAINER_DYN_LINK")])}))


boost_container = NixSourceProject(
    nixattr="boost_container",
    arcdir=boost.make_arcdir("container"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    disable_includes=["/usr/include/malloc.h"],
    post_install=post_install,
)
