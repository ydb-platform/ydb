from devtools.yamaker import boost
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        populate_srcs=True,
        CFLAGS=["-DBOOST_ATOMIC_SOURCE"],
    )
    with self.yamakes["."] as atomic:
        atomic.ADDINCL.append(f"{self.arcdir}/src")
        atomic.after(
            "CFLAGS",
            Switch(
                DYNAMIC_BOOST=Linkable(CFLAGS=[GLOBAL("-DBOOST_ATOMIC_DYN_LINK")]),
            ),
        )

        # wait_on_address is a windows-specific source
        atomic.SRCS.remove("src/wait_on_address.cpp")
        atomic.after(
            "PEERDIR",
            Switch(
                OS_WINDOWS=Linkable(
                    SRCS=["src/wait_on_address.cpp"],
                ),
            ),
        )

        atomic.SRCS.remove("src/find_address_sse41.cpp")
        atomic.after(
            "SRCS",
            Switch(
                USE_SSE4=Linkable(
                    CFLAGS=["-DBOOST_ATOMIC_USE_SSE41"],
                    SRCS=["src/find_address_sse41.cpp"],
                ),
            ),
        )


boost_atomic = NixSourceProject(
    nixattr="boost_atomic",
    arcdir=boost.make_arcdir("atomic"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    disable_includes=[
        # sys/futex.h is bsd-specific header for futex API
        "sys/futex.h",
    ],
    post_install=post_install,
)
