from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as sqlite:
        sqlite.CFLAGS.remove("-DSQLITE_OS_UNIX=1")
        sqlite.CFLAGS.remove("-D_HAVE_SQLITE_CONFIG_H")
        sqlite.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(CFLAGS=["-DSQLITE_OS_WIN"]),
                default=Linkable(CFLAGS=["-DSQLITE_OS_UNIX"]),
            ),
        )
        sqlite.SRCS.add("test_multiplex.c")


sqlite3 = GNUMakeNixProject(
    owners=["g:cpp-contrib", "g:maps-mrc"],
    arcdir="contrib/libs/sqlite3",
    nixattr="sqlite",
    makeflags=["libsqlite3.la"],
    copy_sources=["sqlite3.h", "sqlite3ext.h", "test_multiplex.*"],
    disable_includes=[
        "sqlite3rtree.h",
        "sqlite_tcl.h",
        "sys/cygwin.h",
        # if defined(SQLITE_ENABLE_ICU)
        "unicode/",
        "vxWorks.h",
        "INC_STRINGIFY(SQLITE_CUSTOM_INCLUDE)",
    ],
    post_install=post_install,
)
