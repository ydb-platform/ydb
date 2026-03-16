from devtools.yamaker.project import NixProject


def libev_post_install(self):
    with self.yamakes["."] as m:
        # event.c is a libevent compatibility layer
        m.SRCS.remove("event.c")


libev = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libev",
    nixattr="libev",
    copy_sources=[
        "ev++.h",
        # ev_win32 is Windows-specific source of libev
        "ev_win32.c",
        # ev_kqueue is BSD / Darwin specific source of libev
        "ev_kqueue.c",
    ],
    disable_includes=[
        "ev_iocp.c",
        # We have no libaio support as it is LGPL / GPL
        "ev_linuxaio.c",
        # these includes are Solaris-specific
        "ev_port.c",
        "mbarrier.h",
        "EV_H",
        "EV_CONFIG_H",
    ],
    platform_dispatchers=["config.h"],
    post_install=libev_post_install,
)
