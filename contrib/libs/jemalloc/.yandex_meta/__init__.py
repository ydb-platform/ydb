from devtools.yamaker.modules import DLLFor, GLOBAL, Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def jemalloc_post_install(self):
    with self.yamakes["."] as m:
        # llvm_libunwind only works on Linux / Darwin
        m.PEERDIR.remove("contrib/libs/libunwind")

        not_windows = Linkable(CFLAGS=["-funroll-loops"])
        not_windows.after(
            "CFLAGS",
            Switch(
                {
                    "OS_DARWIN OR OS_IOS": Linkable(SRCS=["src/zone.c", GLOBAL("reg_zone.cpp")]),
                    "default": Linkable(
                        CFLAGS=["-fvisibility=hidden"],
                        PEERDIR=["contrib/libs/libunwind"],
                    ),
                }
            ),
        )

        m.after(
            "ADDINCL",
            Switch(
                OS_WINDOWS=Linkable(ADDINCL=[self.arcdir + "/include/msvc_compat"]),
                default=not_windows,
            ),
        )

    self.yamakes["dynamic"] = DLLFor(SUBSCRIBER=self.owners, DLL_FOR=[self.arcdir, "jemalloc"])


jemalloc = GNUMakeNixProject(
    owners=["g:contrib", "g:cpp-contrib"],
    arcdir="contrib/libs/jemalloc",
    nixattr="jemalloc",
    flags=[
        "--enable-prof",
        "--with-jemalloc-prefix=",
        # jemalloc does not properly handles Intel 5-level-paging
        # (it uses cpuid instead of checking linux kernel capabilities).
        #
        # Force 48-bit virtual addressing.
        # See https://st.yandex-team.ru/KERNEL-729 for details
        "--with-lg-vaddr=48",
    ],
    copy_sources=[
        "bin/jeprof",
        "src/zone.c",
        "include/jemalloc/internal/atomic_msvc.h",
        "include/jemalloc/internal/tsd_generic.h",
        "include/jemalloc/internal/tsd_win.h",
        "include/msvc_compat/*.h",
    ],
    platform_dispatchers=[
        "include/jemalloc/internal/jemalloc_internal_defs.h",
        "include/jemalloc/internal/private_namespace.h",
        "include/jemalloc/jemalloc.h",
    ],
    disable_includes=[
        "jemalloc/internal/atomic_gcc_sync.h",
        "jemalloc/internal/atomic_c11.h",
        "jemalloc/internal/tsd_malloc_thread_cleanup.h",
        "jemalloc/internal/private_namespace_jet.h",
        "jemalloc/internal/public_namespace.h",
        # ifdef __NetBSD__
        "sys/bitops.h",
    ],
    ignore_commands=["sh"],
    keep_paths=["reg_zone.cpp"],
    post_install=jemalloc_post_install,
)
