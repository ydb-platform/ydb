from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import NixProject


def post_install(self):
    win_sources = set(
        fileutil.files(
            f"{self.dstdir}/src/win",
            rel=self.dstdir,
            test=pathutil.is_source,
        )
    )

    unix_sources = set(
        fileutil.files(
            f"{self.dstdir}/src/unix",
            rel=self.dstdir,
            test=pathutil.is_source,
        )
    )

    android_linux_sources = {
        "src/unix/linux.c",
        "src/unix/procfs-exepath.c",
        "src/unix/random-getentropy.c",
        "src/unix/random-getrandom.c",
        "src/unix/random-sysctl-linux.c",
    }

    apple_sources = {
        "src/unix/bsd-ifaddrs.c",
        "src/unix/darwin-proctitle.c",
        "src/unix/darwin.c",
        "src/unix/fsevents.c",
        "src/unix/kqueue.c",
        "src/unix/random-getentropy.c",
    }

    unix_sources -= android_linux_sources
    unix_sources -= apple_sources

    with self.yamakes["."] as m:
        m.ADDINCL.remove(self.arcdir + "/src/unix")
        m.SRCS -= android_linux_sources
        m.SRCS -= unix_sources

        m.after(
            "SRCS",
            Switch(
                {
                    "OS_WINDOWS": Linkable(
                        SRCS=win_sources,
                    ),
                    "default": Linkable(
                        SRCS=unix_sources,
                    ),
                }
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    "OS_ANDROID OR OS_LINUX": Linkable(
                        SRCS=android_linux_sources,
                    ),
                }
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    'OS_LINUX AND SANITIZER_TYPE == "thread"': Linkable(
                        NO_SANITIZE=True,
                    ),
                }
            ),
        )
        m.after(
            "SRCS",
            Switch(
                {
                    "OS_DARWIN OR OS_IOS": Linkable(
                        CFLAGS=[
                            "-D_DARWIN_UNLIMITED_SELECT=1",
                            "-D_DARWIN_USE_64_BIT_INODE=1",
                        ],
                        SRCS=apple_sources,
                    ),
                }
            ),
        )
        m.PEERDIR.add("library/cpp/sanitizer/include")


libuv = NixProject(
    owners=["g:cpp-contrib", "g:reportrenderer"],
    arcdir="contrib/libs/libuv",
    nixattr="libuv",
    build_subdir="build",
    copy_sources=[
        "include/uv/darwin.h",
        "include/uv/win.h",
        "src/win",
        "src/unix/bsd-ifaddrs.c",
        "src/unix/darwin-proctitle.c",
        "src/unix/darwin.c",
        "src/unix/darwin-stub.h",
        "src/unix/darwin-syscalls.h",
        "src/unix/fsevents.c",
        "src/unix/kqueue.c",
        "src/unix/random-getentropy.c",
    ],
    disable_includes=[
        "as400_protos.h",
        "atomic.h",
        "os390-syscalls.h",
        "port.h",
        "zos-base.h",
        "zos-sys-info.h",
        "sanitizer/linux_syscall_hooks.h",
        "sys/port.h",
        "uv/aix.h",
        "uv/bsd.h",
        "uv/os390.h",
        "uv/posix.h",
        "uv/stdint-msvc2008.h",
        "uv/sunos.h",
    ],
    post_install=post_install,
)
