from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self: CMakeNinjaNixProject):
    m = self.yamakes["."]
    m.PEERDIR.add("library/cpp/sanitizer/include")


mimalloc = CMakeNinjaNixProject(
    owners=[
        "g:balancer",
        "g:cpp-contrib",
    ],
    arcdir="contrib/libs/mimalloc",
    nixattr="mimalloc",
    copy_sources=[
        "readme.md",
        "include/",
        "src/prim/osx",
        "src/prim/unix",
    ],
    disable_includes=[
        "synch.h",
        "../src/prim/windows/etw.h",
        "sys/domainset.h",
        "kernel/OS.h",
        "windows/prim.c",
        "wasi/prim.c",
        "emscripten/prim.c",
    ],
    flags=[
        "-DMI_BUILD_TESTS=0",
    ],
    post_install=post_install,
)
