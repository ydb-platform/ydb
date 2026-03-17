from devtools.yamaker.project import CMakeNinjaNixProject


def jsoncpp_post_install(self):
    with self.yamakes["."] as m:
        m.NO_UTIL = False
        m.CFLAGS = None


jsoncpp = CMakeNinjaNixProject(
    license="MIT",
    nixattr="jsoncpp",
    arcdir="contrib/deprecated/jsoncpp",
    copy_sources=["include/json/"],
    disable_includes=["cpptl/cpptl_autolink.h"],
    flags=["-DJSONCPP_WITH_TESTS=OFF"],
    addincl_global={".": {"./include"}},
    inclink={"include/json/json-forwards.h": "include/json/forwards.h"},
    post_install=jsoncpp_post_install,
)
