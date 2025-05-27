from devtools.yamaker import fileutil
from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import GLOBAL, Switch, Linkable, Words
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    curl_config = f"{self.dstdir}/lib/curl_config.h"
    fileutil.re_sub_file(
        curl_config,
        "#pragma once\n",
        r"""\g<0>
#include <util/system/platform.h>
""",
    )
    with open(curl_config, "a") as config:
        config.write(
            """
// Do not misrepresent host on Android and iOS.
#undef OS
#define OS "arcadia"

// c-ares resolver is known to be buggy.
//
// There is no way to configure it properly without a JVM on Android,
// because Android lacks traditional resolv.conf.
//
// For standalone Android programs, it is impossible
// to contact ConnectionManager outside the JVM; this breaks c-ares DNS resolution.
// As we can not distinguish builds of Android apps from standalone Android programs.
//
// During mapkit experiments, c-ares was adding about 10ms to each query timespan.
//
//
// On Linux it caches /etc/resolv.conf contents and does not invalidate it properly

#if defined(ARCADIA_CURL_DNS_RESOLVER_ARES)
    #define USE_ARES
#elif defined(ARCADIA_CURL_DNS_RESOLVER_MULTITHREADED)
    #undef USE_ARES
    #if defined(_MSC_VER)
        #define USE_THREADS_WIN32 1
    #else
        #define USE_THREADS_POSIX 1
    #endif
#elif defined(ARCADIA_CURL_DNS_RESOLVER_SYNCHRONOUS)
    #undef USE_ARES
    #undef USE_THREADS_POSIX
    #undef USE_THREADS_WIN32
#else
    #error "No dns resolver is specified or resolver specification is wrong"
#endif
"""
        )

    # curl uses SIZEOF_ macros to test current platform bitness in compile-time
    # As we only control curl_config-linux.h during yamaker installation,
    # we can not ensure if the proper define is set.
    #
    # SIZEOF_SIZE_T is controlled by curl_config-x(32|64).h
    # SIZEOF_SHORT..SIZEOF_LONG are expected to be defined by util/system/platform.h.
    curl_config_linux = f"{self.dstdir}/lib/curl_config-linux.h"
    fileutil.re_sub_file(
        curl_config_linux,
        "(?m)^#define (SIZEOF_(SHORT|INT|LONG)) .*",
        r"#ifndef \1\n#error undefined \1\n#endif",
    )
    # time_t is long (4 or 8 bytes), except on Windows it is long long (always 8 bytes).
    fileutil.re_sub_file(
        curl_config_linux,
        "(?m)^(#define SIZEOF_TIME_T) .*",
        r"\1 SIZEOF_LONG",
    )

    with self.yamakes["."] as m:
        m.before("CFLAGS", "DEFAULT(ARCADIA_CURL_DNS_RESOLVER ARES)")
        m.CFLAGS = [
            GLOBAL("-DCURL_STATICLIB"),
            "-DBUILDING_LIBCURL",
            "-DHAVE_CONFIG_H",
            "-DARCADIA_CURL_DNS_RESOLVER_${ARCADIA_CURL_DNS_RESOLVER}",
        ]

        # add ifaddrs implementation if needed
        m.PEERDIR.add("contrib/libs/libc_compat")

        # make c-ares dependency conditional,
        # but leave ADDINCL in place to make CONFIGURE work
        m.ADDINCL.append("contrib/libs/c-ares/include")
        m.ADDINCL = sorted(m.ADDINCL, key=ArcPath._as_cmp_tuple)
        m.PEERDIR.remove("contrib/libs/c-ares")
        m.after(
            "CFLAGS",
            Switch(
                {
                    "ARCADIA_CURL_DNS_RESOLVER == ARES": Linkable(
                        PEERDIR=["contrib/libs/c-ares"],
                    )
                }
            ),
        )

        # curl calls system functions for address synthesis on macOS
        # https://github.com/curl/curl/pull/7121
        m.after(
            "LDFLAGS",
            Switch(
                OS_DARWIN=Linkable(LDFLAGS=[Words("-framework", "SystemConfiguration")]),
            ),
        )

        # Remove to avoid disabling lots of unneeded includes
        m.SRCS -= {
            "lib/vtls/cyassl.c",
            "lib/vtls/gskit.c",
            "lib/vtls/gtls.c",
            "lib/vtls/mbedtls.c",
            "lib/vtls/mesalink.c",
            "lib/vtls/nss.c",
            "lib/vtls/polarssl.c",
            "lib/vtls/polarssl_threadlock.c",
            "lib/vtls/schannel.c",
            "lib/vtls/schannel_verify.c",
        }

    with self.yamakes["bin"] as cli:
        # cli is intended to be built on development hosts.
        # c-ares dns resolver is expected to function here.
        cli.CFLAGS.append("-DARCADIA_CURL_DNS_RESOLVER_ARES")
        cli.PEERDIR.add("contrib/libs/c-ares")


curl = GNUMakeNixProject(
    owners=["g:cpp-contrib", "g:geoapps_infra"],
    arcdir="contrib/libs/curl",
    nixattr="curl",
    ignore_commands=[
        "bash",
    ],
    put={
        "Library curl": ".",
        "Program curl": "bin",
    },
    copy_sources=[
        "include/curl/stdcheaders.h",
        "lib/curl_sspi.h",
        "lib/setup-win32.h",
    ],
    disable_includes=[
        "afunix.h",
        "amitcp/",
        "bsdsocket/socketbasetags.h",
        "cipher.mih",
        "config-*",
        "curl_gssapi.h",
        "curlmsg_vms.h",
        "exec/execbase.h",
        "exec/types.h",
        "extra/",
        "fabdef.h",
        "floss.h",
        "gnutls/",
        "gss.h",
        "idn2.h",
        "lber.h",
        "ldap.h",
        "ldap_ssl.h",
        "libpsl.h",
        "librtmp/rtmp.h",
        "libssh/",
        "lwip/",
        "mbedtls/",
        "mbedtls_threadlock.h",
        "msh3.h",
        "nspr.h",
        "netinet/in6.h",
        "nettle/",
        "ngtcp2/ngtcp2_crypto_boringssl.h",
        "ngtcp2/ngtcp2_crypto_gnutls.h",
        "ngtcp2/ngtcp2_crypto_wolfssl.h",
        "nwconio.h",
        # NB: openssl/core_names.h appeared in OpenSSL 3.0, while we have only 1.1.1l at the time
        "openssl/core_names.h",
        "plarenas.h",
        "proto/",
        "quiche.h",
        "setup-os400.h",
        "setup-vms.h",
        "stabs.h",
        "subauth.h",
        "vquic/msh3.h",
        "vquic/ngtcp2.h",
        "vquic/quiche.h",
        "wolfssh/",
        "wolfssl/",
        "hyper.h",
        "gsasl.h",
        "descrip",
        "iodef",
        "starlet",
        # Disable system includes of these headers, yet allow including lib/vtls/{rustls,bearssl}.h
        "<rustls.h>",
        "<bearssl.h>",
    ],
    addincl_global={".": {"./include"}},
    platform_dispatchers=["lib/curl_config.h"],
    post_install=post_install,
)

# CHANGES file is just a git log, it is not intended for humans, yet increases diff size dramatically
curl.copy_top_sources_except.add("CHANGES")
