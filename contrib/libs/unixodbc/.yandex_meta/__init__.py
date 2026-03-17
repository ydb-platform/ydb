from devtools.yamaker import fileutil
from devtools.yamaker.project import NixProject


def post_install(self):
    # disable dynamic library lookup and stabilize reimport
    fileutil.re_sub_file(
        f"{self.dstdir}/config.h",
        '#define LT_DLSEARCH_PATH ".*"',
        "/* undef LT_DLSEARCH_PATH */",
    )
    fileutil.re_sub_file(
        f"{self.dstdir}/libltdl/config.h",
        '#define LT_DLSEARCH_PATH ".*"',
        "/* undef LT_DLSEARCH_PATH */",
    )
    for path in (".", "libltdl"):
        with self.yamakes[path] as odbc:
            # provide strlcpy and strlcat
            odbc.PEERDIR.add("contrib/libs/libc_compat")


unixodbc = NixProject(
    arcdir="contrib/libs/unixodbc",
    nixattr="unixODBC",
    ignore_commands=["sed"],
    disable_includes=[
        "argz.h",
        "synch.h",
        "sys/dl.h",
    ],
    copy_sources=["*/README", "*/TODO"],
    copy_sources_except=["Drivers/README", "DRVConfig/README"],
    put={"odbc": "."},
    addincl_global={".": {"./include"}},
    inclink={"include/unixodbc.h": "unixodbc.h"},
    license="LGPL-2.0-only",
    post_install=post_install,
)
