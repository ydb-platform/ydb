import os
import os.path
import shutil

from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def libevent_post_install(self):
    own_compat = os.path.join(self.arcdir, "compat")
    for p, m in self.yamakes.items():
        if own_compat in m.ADDINCL:
            m.ADDINCL.remove(own_compat)
        m.PEERDIR.add("contrib/libs/libc_compat")
        m.CFLAGS.append("-DEVENT__HAVE_STRLCPY=1")

    shutil.rmtree(os.path.join(self.dstdir, "compat"))
    os.remove(os.path.join(self.dstdir, "strlcpy.c"))

    with self.yamakes["event_core"] as m:
        m.SRCS -= {"epoll.c", "poll.c", "select.c", "strlcpy.c"}

        m.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(
                    SRCS=[
                        "buffer_iocp.c",
                        "bufferevent_async.c",
                        "event_iocp.c",
                        "win32select.c",
                    ],
                ),
                default=Linkable(
                    SRCS=[
                        "poll.c",
                        "select.c",
                    ],
                ),
            ),
        )
        m.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(
                    SRCS=["epoll.c"],
                )
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    "OS_FREEBSD OR OS_DARWIN": Linkable(
                        SRCS=["kqueue.c"],
                    )
                }
            ),
        )

    with self.yamakes["event_thread"] as m:
        orig_srcs = m.SRCS
        m.SRCS = {}
        m.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(
                    SRCS=["evthread_win32.c"],
                ),
                default=Linkable(SRCS=orig_srcs),
            ),
        )

    with self.yamakes["."] as m:
        m.SRCS = {}
        m.PEERDIR = [
            os.path.join(self.arcdir, p)
            for p in (
                "event_core",
                "event_extra",
                "event_openssl",
                "event_thread",
            )
        ]


libevent = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libevent",
    nixattr="libevent",
    ignore_commands=["bash", "sed"],
    license="BSD-3-Clause",
    copy_sources=[
        "include/**/*.h",
        "*.c",
        "*.h",
        "whatsnew-2.0.txt",
    ],
    put={
        "event-2.1": ".",
        "event_core-2.1": "event_core",
        "event_extra-2.1": "event_extra",
        "event_pthreads-2.1": "event_thread",
        "event_openssl-2.1": "event_openssl",
    },
    ignore_targets={
        "bench",
        "bench_cascade",
        "bench_http",
        "bench_httpclient",
        "dns-example",
        "event-read-fifo",
        "hello-world",
        "http-connect",
        "http-server",
        "https-client",
        "le-proxy",
        "regress",
        "signal-test",
        "test-changelist",
        "test-closed",
        "test-dumpevents",
        "test-eof",
        "test-fdleak",
        "test-init",
        "test-ratelim",
        "test-time",
        "test-weof",
        "time-test",
    },
    platform_dispatchers=["include/event2/event-config.h"],
    addincl_global={".": {"./include"}},
    post_install=libevent_post_install,
    disable_includes=["afunix.h", "netinet/in6.h", "vproc.h"],
)
