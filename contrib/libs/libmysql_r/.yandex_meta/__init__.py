from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def mysql_post_install(self):
    # Combine clientlib with mysqlclient.
    with self.yamakes["."] as mc, self.yamakes["libmysql"] as mm:
        mc.PEERDIR += mm.PEERDIR - {self.arcdir}
        mc.RECURSE -= {"libmysql"}
    del self.yamakes["libmysql"]
    # Support Darwin and Windows.
    with self.yamakes["mysys"] as m:
        linux_srcs = {
            "my_getpwnam.cc",
            "my_largepage.cc",
            "posix_timers.cc",
        }
        m.SRCS -= linux_srcs
        m.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(SRCS=linux_srcs),
                OS_DARWIN=Linkable(SRCS=["kqueue_timers.cc", "my_getpwnam.cc"]),
                OS_WINDOWS=Linkable(SRCS="my_conio.cc my_windac.cc my_winerr.cc my_winfile.cc win_timers.cc".split()),
            ),
        )
        # Take strlcpy from libc_compat instead of a copy-paste
        m.PEERDIR.add("contrib/libs/libc_compat")

    self.yamakes["vio"].after(
        "SRCS",
        Switch(
            OS_WINDOWS=Linkable(SRCS=["viopipe.cc", "vioshm.cc"]),
        ),
    )


mysql = CMakeNinjaNixProject(
    arcdir="contrib/libs/libmysql_r",
    nixattr="mysql80",
    build_targets=["libmysqlclient.so"],
    ignore_commands=["comp_err"],
    ignore_targets=["comp_err"],
    keep_paths=["mysql_config"],
    copy_sources=[
        "include/jemalloc_win.h",
        "mysys/my_*.cc",
        "mysys/*_timers.cc",
        "vio/viopipe.cc",
        "vio/vioshm.cc",
    ],
    disable_includes=[
        "backtrace/stacktrace.hpp",
        "big_endian.h",
        "libunwind.h",
        "mysql_com_server.h",
        "openssl/applink.c",
        "openssl/provider.h",
        "pfs_cond_provider.h",
        "pfs_memory_provider.h",
        "pfs_mutex_provider.h",
        "pfs_rwlock_provider.h",
        "pfs_table_provider.h",
        "pfs_thread_provider.h",
        "sql/changestreams/misc/replicated_columns_view.h",
        "sql/client_settings.h",
        "sql/derror.h",
        "sql/log.h",
        "sql/rpl_filter.h",
        "sql/sql_digest",
        "sql/server_component/mysql_command_services_imp.h",
        "sql/table.h",
        "sql/xa.h",
        "wolfssl/openssl/ssl.h",
    ],
    platform_dispatchers=[
        "include/my_config.h",
        "libbinlogevents/include/binlog_config.h",
    ],
    inclink={"mysql": {"include/mysql.h", "include/mysqld_error.h"}},
    addincl_global={".": {"./include"}},
    unbundle_from={
        "zlib": "extra/zlib",
    },
    put={
        "strings": "strings",
        "uca9dump": "strings/uca9dump",
    },
    put_with={
        "mysys": ["mytime"],
    },
    post_install=mysql_post_install,
)
