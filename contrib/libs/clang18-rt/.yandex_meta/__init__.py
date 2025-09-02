from devtools.yamaker.modules import Linkable, Switch, Recursable
from devtools.yamaker.platform_macros import LLVM_VERSION
from devtools.yamaker.project import CMakeNinjaNixProject


CLANG_RT_LIBS = {
    "asan",
    "asan-preinit",
    "asan_cxx",
    "asan_static",
    "cfi",
    "cfi_diag",
    "dd",
    "dfsan",
    "gwp_asan",
    "hwasan",
    "hwasan_cxx",
    "hwasan_aliases",
    "hwasan_aliases_cxx",
    "lsan",
    "memprof",
    "memprof-preinit",
    "memprof_cxx",
    "msan",
    "msan_cxx",
    "profile",
    "safestack",
    "scudo_standalone",
    "scudo_standalone_cxx",
    "stats",
    "stats_client",
    "tsan",
    "tsan_cxx",
    "ubsan_minimal",
    "ubsan_standalone",
    "ubsan_standalone_cxx",
}

CLANG_RT_LINUX_ONLY_LIBS = {
    "lib/" + lib
    for lib in CLANG_RT_LIBS
    if lib.startswith(
        (
            "dd",
            "dfsan",
            "cfi",
            "gwp_asan",
            "hwasan",
            "msan",
            "memprof",
            "memprof-preinit",
            "memprof_cxx",
            "safestack",
            "scudo",
        )
    )
}

MAPSMOBI_SPECIFIC = """
IF(MAPSMOBI_BUILD_TARGET == "yes")
    NO_OPTIMIZE()
ENDIF()
"""


def post_install(self):
    # Original build system uses a copy of profile/InstrProfData.inc in build dir.
    self.yamakes["lib/profile"].ADDINCL.add(self.arcdir + "/include")
    # Override -fexceptions that we force on C code; fixes combined LTO+PGO builds.
    self.yamakes["lib/profile"].CFLAGS.append("-fno-exceptions")
    self.yamakes["lib/memprof"].ADDINCL.add(self.arcdir + "/include")
    for s in CLANG_RT_LIBS:
        with self.yamakes["lib/" + s] as m:
            m.NO_SANITIZE = True
            # We need specific library names for use in pkg.json.
            m.before("DLL", "INCLUDE(${ARCADIA_ROOT}/build/platform/clang/arch.cmake)\n")
            m.LIBRARY = ["clang_rt." + s + "${CLANG_RT_SUFFIX}"]
    # Remove Linux-only libraries from other platforms.
    self.yamakes["."].RECURSE -= CLANG_RT_LINUX_ONLY_LIBS
    self.yamakes["."].after(
        "RECURSE",
        Switch(
            OS_LINUX=Recursable(RECURSE=CLANG_RT_LINUX_ONLY_LIBS),
        ),
    )
    # Support all needed arches and OSes.
    with self.yamakes["lib/tsan"] as m:
        linux_srcs = {"tsan/rtl/tsan_platform_linux.cpp"}
        darwin_srcs = {
            "tsan/rtl/tsan_platform_mac.cpp",
            "tsan/rtl/tsan_interceptors_mac.cpp",
            "tsan/rtl/tsan_interceptors_mach_vm.cpp",
        }
        m.SRCS -= linux_srcs
        m.SRCS.add("tsan/rtl/tsan_rtl_aarch64.S")
        m.after(
            "SRCS",
            Switch(
                ARCH_PPC64LE=Linkable(SRCS=["tsan/rtl/tsan_rtl_ppc64.S"]),
            ),
        )
        m.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(SRCS=linux_srcs),
                OS_DARWIN=Linkable(SRCS=darwin_srcs),
            ),
        )
    with self.yamakes["lib/asan_static"] as m:
        linux_srcs = {"asan_rtl_x86_64.S"}
        m.SRCS -= linux_srcs
        supported_platforms = "ARCH_X86_64 AND NOT OS_DARWIN"
        m.after(
            "SRCS",
            Switch({supported_platforms: Linkable(SRCS=linux_srcs)}),
        )

    with self.yamakes["lib/msan"] as m:
        m.before("CFLAGS", MAPSMOBI_SPECIFIC)

    # Remove SCUDO_DEFAULT_OPTIONS.
    for ym in self.yamakes:
        if hasattr(self.yamakes[ym], "CFLAGS"):
            self.yamakes[ym].CFLAGS.remove("-DSCUDO_DEFAULT_OPTIONS=DeleteSizeMismatch=0:DeallocationTypeMismatch=0")


llvm_clang_rt = CMakeNinjaNixProject(
    nixattr=f"llvmPackages_{LLVM_VERSION}.compiler-rt",
    arcdir=f"contrib/libs/clang{LLVM_VERSION}-rt",
    # fmt: off
    install_targets=[
        f"clang_rt.{lib}-x86_64"
        for lib in CLANG_RT_LIBS
    ],
    # fmt: on
    ignore_targets=[
        "libclang_rt.*.so",
    ],
    copy_sources=[
        "include/sanitizer/*.h",
        "lib/asan/*.h",
        "lib/interception/*.h",
        "lib/lsan/*.h",
        "lib/profile/WindowsMMap.h",
        "lib/sanitizer_common/*.inc",
        "lib/sanitizer_common/*.h",
        "lib/tsan/rtl/tsan_interceptors_*.cpp",
        "lib/tsan/rtl/tsan_platform_*.cpp",
        "lib/tsan/rtl/tsan_rtl_*.S",
        "lib/tsan/rtl/tsan_spinlock_defs_mac.h",
    ],
    flags=[
        "-DCOMPILER_RT_BUILD_BUILTINS=OFF",  # Split into cxxsupp/builtins.
        "-DCOMPILER_RT_BUILD_LIBFUZZER=OFF",  # Split into libfuzzer.
    ],
    put={f"clang_rt.{lib}-x86_64": f"lib/{lib}" for lib in CLANG_RT_LIBS},
    args=["--add-foptions", "--keep-cflag=PIC"],
    post_install=post_install,
    disable_includes=[
        "GWP_ASAN_PLATFORM_TLS_HEADER",
        "altq/",
        "asm/reg.h",
        "backtrace.h",
        "backtrace-supported.h",
        "bionic/",
        "demangle.h",
        "dev/",
        "fs/",
        "lib/",
        "link_elf.h",
        "machine/reg.h",
        "md5.h",
        "net/if_ether.h",
        "net/if_gre.h",
        "net/if_pppoe.h",
        "net/if_sppp.h",
        "net/if_srt.h",
        "net/if_tap.h",
        "net/if_tun.h",
        "net/npf.h",
        "net/ppp_defs.h",
        "net/slip.h",
        "netbt/hci.h",
        "netinet/ip_compat.h",
        "netinet/ip_fil.h",
        "netinet/ip_mroute.h",
        "netinet/ip_nat.h",
        "netinet/ip_proxy.h",
        "netinet6/in6_var.h",
        "netinet6/nd6.h",
        "netsmb/smb_dev.h",
        "nvmm.h",
        "ptrauth.h",
        "rmd160.h",
        "rpc/xdr.h",
        "sanitizer_intercept_overriders.h",
        "scudo_platform_tls_slot.h",
        "sha224.h",
        "sha256.h",
        "sha384.h",
        "sha512.h",
        "stringlist.h",
        "sys/agpio.h",
        "sys/ataio.h",
        "sys/audioio.h",
        "sys/capsicum.h",
        "sys/cdbr.h",
        "sys/cdio.h",
        "sys/chio.h",
        "sys/clockctl.h",
        "sys/consio.h",
        "sys/cpuio.h",
        "sys/dkbad.h",
        "sys/dkio.h",
        "sys/drvctlio.h",
        "sys/dvdio.h",
        "sys/envsys.h",
        "sys/fdio.h",
        "sys/futex.h",
        "sys/gpio.h",
        "sys/ioctl_compat.h",
        "sys/ipmi.h",
        "sys/joystick.h",
        "sys/kbio.h",
        "sys/kcov.h",
        "sys/ksyms.h",
        "sys/link_elf.h",
        "sys/lua.h",
        "sys/midiio.h",
        "sys/mqueue.h",
        "sys/power.h",
        "sys/procctl.h",
        "sys/ptyvar.h",
        "sys/radioio.h",
        "sys/rndio.h",
        "sys/scanio.h",
        "sys/scsiio.h",
        "sys/sha1.h",
        "sys/sha2.h",
        "sys/timepps.h",
        "sys/tls.h",
        "sys/verified_exec.h",
        "sys/wdog.h",
        "traceloggingprovider.h",
        "zircon/",
        # ifdef __sun
        "procfs.h",
        "synch.h",
        # if defined(_AIX)
        "sys/ldr.h",
        "xcoff.h",
        # ifdef SCUDO_USE_CUSTOM_CONFIG
        "custom_scudo_config.h",
    ],
)
