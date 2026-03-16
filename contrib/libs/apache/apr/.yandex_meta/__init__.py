import os.path as P

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject

include_ya_win = Switch(OS_WINDOWS="INCLUDE(ya-win.cmake)")


def post_install(self):
    with self.yamakes["."] as m:
        # -DLINUX is not used yet this define looks confusing
        m.CFLAGS.remove("-DLINUX")

        # Provide <sys/random.h> by the means of libc_compat
        m.PEERDIR.add("contrib/libs/libc_compat")

        m.before("ADDINCL", include_ya_win)
        m.after(
            "SRCS",
            Switch({"NOT OS_WINDOWS": Linkable(SRCS=m.SRCS)}),
        )
        m.SRCS = {}
        for rp in m.RUN_PROGRAM:
            rp.record.Cmd.Dir = rp.record.Cmd.Dir.replace("${ARCADIA_ROOT}", "${ARCADIA_BUILD_ROOT}")

    with open(P.join(self.srcdir, "CMakeLists.txt")) as f:
        lines = f.readlines()
    lines = lines[lines.index("SET(APR_SOURCES\n") + 1 :]
    lines = lines[: lines.index(")\n")]
    sources = {line.strip() for line in lines}

    ya_win = Linkable(
        ADDINCL=["contrib/libs/apache/apr/include/arch/win32"],
        CFLAGS=[GLOBAL("-DAPR_DECLARE_STATIC")],
        LDFLAGS=["Mswsock.lib", "Rpcrt4.lib", "Version.lib", "Ws2_32.lib"],
        SRCS=sources,
    )
    ya_win.before("ADDINCL", '# "win32" should be included before "unix". "unix" is also needed.')
    self.yamakes["ya-win.cmake"] = ya_win

    with self.yamakes["tools"] as gen_test_char:
        # -DLINUX is not used yet this define looks confusing
        gen_test_char.CFLAGS.remove("-DLINUX")


apache_apr = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/apache/apr",
    nixattr="apr",
    cflags=["-DAPR_POOL_DEBUG=0"],
    makeflags=["libapr-1.la"],
    disable_includes=[
        "atomic.h",
        "kernel/",
        "os2.h",
        "port.h",
        "randbyte_os2.inc",
        "uuid.h",
    ],
    platform_dispatchers=["include/apr.h", "include/arch/unix/apr_private.h"],
    copy_sources=["include/arch/win32/", "*/win32/*"],
    post_install=post_install,
)
