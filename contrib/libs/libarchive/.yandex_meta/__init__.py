from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


LIBARCHIVE_WIN_SRCS = [
    "libarchive/archive_entry_copy_bhfi.c",
    "libarchive/archive_read_disk_windows.c",
    "libarchive/archive_windows.c",
    "libarchive/archive_write_disk_windows.c",
    "libarchive/filter_fork_windows.c",
]


def libarchive_post_install(self):
    with self.yamakes["."] as libarchive:
        libarchive.CFLAGS.remove("-D__LIBARCHIVE_ENABLE_VISIBILITY")
        libarchive.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=[GLOBAL("-DLIBARCHIVE_STATIC")],
                    SRCS=LIBARCHIVE_WIN_SRCS,
                )
            ),
        )


libarchive = CMakeNinjaNixProject(
    license="BSD-3-Clause",
    arcdir="contrib/libs/libarchive",
    nixattr="libarchive",
    copy_sources=(
        LIBARCHIVE_WIN_SRCS
        + [
            "libarchive/archive_windows.h",
            # Compatibility header with openssl v1
            "libarchive/archive_openssl_hmac_private.h",
        ]
    ),
    disable_includes=[
        "acl/libacl.h",
        "android_lf.h",
        # There is no need to use a drop-in replacement for crc32() from zlib,
        # as we have full zlib properly PEERDIR'ed.
        "archive_crc32.h",
        "archive_blake2.h",
        "config_freebsd.h",
        "config_windows.h",
        "expat.h",
        "ext2fs/ext2_fs.h",
        "libxml/",
        "linux/ext2_fs.h",
        "localcharset.h",
        "lzo/lzo1x.h",
        "lzo/lzoconf.h",
        "md5.h",
        "membership.h",
        "nettle/",
        "openssl/params.h",
        "pcre.h",
        "pcre2.h",
        "quarantine.h",
        "rmd160.h",
        "sha.h",
        "sha1.h",
        "sha2.h",
        "sha256.h",
        "sha512.h",
        "sys/diskslice.h",
        "sys/dkio.h",
        "sys/ea.h",
        "sys/fcntl1.h",
        "sys/netmgr.h",
        "sys/richacl.h",
        "blake2-kat.h",
        "mbedtls/*.h",
        "PLATFORM_CONFIG_H",
    ],
    platform_dispatchers=["config.h"],
    build_targets=["libarchive.so"],
    install_targets=["archive"],
    post_install=libarchive_post_install,
)
