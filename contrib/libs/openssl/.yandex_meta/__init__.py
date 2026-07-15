import os

from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import NixProject


def post_build(self):
    os.remove(f"{self.dstdir}/libssl.map")
    for hdr in fileutil.files(f"{self.dstdir}/include", rel=False, test=pathutil.is_header):
        with open(hdr, "rt") as hdr_file:
            data = hdr_file.read()
        with open(hdr, "wt") as hdr_file:
            hdr_file.write("#include <contrib/libs/openssl/redef.h>\n")
            hdr_file.write(data)


def post_install(self):
    def d(s):
        return self.dstdir + "/" + s

    # Move asm sources to asm/linux.
    fileutil.subcopy(self.dstdir, d("asm/linux"), ["**/*.s"], move=True)
    with self.yamakes["crypto"] as m:
        asm = {s for s in m.SRCS if s.endswith(".s")}
        m.SRCS -= asm
        m.after(
            "SRCS",
            Switch(
                dict(
                    {
                        "OS_LINUX AND ARCH_X86_64": Linkable(
                            SRCS={"../asm/linux/" + s for s in asm},
                        )
                    }
                )
            ),
        )
        # Shorten paths.
        m.SRCDIR = []
        m.SRCS = {os.path.relpath(s, "crypto") for s in m.SRCS}
        m.SRCS -= {"dso/dso_dlfcn.c", "rand/rand_vms.c"}

    # Add suppression for ubsan, see also https://github.com/openssl/openssl/issues/22896
    with self.yamakes["crypto"] as m:
        m.after("NO_RUNTIME", "SUPPRESSIONS(ubsan.supp)")

    self.yamakes["crypto"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["apps"].PEERDIR.add("library/cpp/sanitizer/include")

    with self.yamakes["."] as m:
        m.after(
            "ORIGINAL_SOURCE",
            """IF (OPENSOURCE_REPLACE_OPENSSL AND EXPORT_CMAKE)

    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE OpenSSL
        CMAKE_TARGET openssl::openssl
        CONAN openssl/${OPENSOURCE_REPLACE_OPENSSL}
    )

ELSE()

    ADDINCL(
        GLOBAL contrib/libs/openssl/include
    )

ENDIF()
""",
        )
        m.ADDINCL = []

    with self.yamakes["crypto"] as m:
        m.after(
            "LICENSE_TEXTS",
            """IF (OPENSOURCE_REPLACE_OPENSSL)

    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE OpenSSL
        CMAKE_PACKAGE_COMPONENT Crypto
        CMAKE_TARGET OpenSSL::Crypto
        CONAN openssl/${OPENSOURCE_REPLACE_OPENSSL}
    )

ENDIF() # IF (OPENSOURCE_REPLACE_OPENSSL)
""",
        )
        m.ADDINCL = []


openssl = NixProject(
    license="OpenSSL AND SSLeay",
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/openssl",
    nixattr="openssl",
    ignore_commands=["bash", "perl"],
    put_with={"openssl": ["apps"]},
    install_targets=["crypto", "openssl", "ssl"],
    put={"ssl": "."},
    disable_includes=[
        "efndef.h",
        "iosbdef.h",
        "rmidef.h",
        "sys/ldr.h",
        # if defined(OPENSSL_SYS_VXWORKS)
        "ioLib.h",
        "sockLib.h",
        "sysLib.h",
        "taskLib.h",
        "tickLib.h",
        "vxWorks.h",
        # if defined(OPENSSL_SYS_VMS)
        "descrip.h",
        "dvidef.h",
        "gen64def.h",
        "iledef.h",
        "iodef.h",
        "jpidef.h",
        "lib$routines.h",
        "libfildef.h",
        "libfisdef.h",
        "rms.h",
        "rmsdef.h",
        "times.h",
        "ssdef.h",
        "starlet.h",
        "str$routines.h",
        "stsdef.h",
        "syidef.h",
        "unixio.h",
    ],
    copy_sources=[
        "apps/**/*.c",
        "apps/**/*.h",
        "crypto/**/*.asm",
        "crypto/**/*.c",
        "crypto/**/*.h",
        "engines/**/*.c",
        "engines/**/*.h",
        "include/**/*.h",
    ],
    platform_dispatchers=[
        "crypto/buildinf.h",
        "include/crypto/bn_conf.h",
        "include/crypto/dso_conf.h",
        "include/openssl/opensslconf.h",
    ],
    keep_paths=[
        # This asm files were generated manually
        "asm/aarch64/",
        "asm/android/",
        "asm/darwin/",
        "asm/darwin-arm64/",
        "asm/ios/",
        "asm/ppc64le/",
        "asm/windows/",
        # This code is written by us
        "ar.pyplugin",
        "sanitizers.h",
        "crypto/ubsan.supp",
        "redef.h",
        "system_openssl.ya.inc",
    ],
    post_build=post_build,
    post_install=post_install,
)
