from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as lzma:
        lzma.CFLAGS += [GLOBAL("-DLZMA_API_STATIC")]


lzma = GNUMakeNixProject(
    arcdir="contrib/libs/lzma",
    nixattr="xz",
    flags=["--localedir=/var/empty"],
    disable_includes=[
        "crc32_arm64.h",
        "crc32_table_be.h",
        "crc64_table_be.h",
        "dpmi.h",
        "invent.h",
        "lz_encoder_hash_table.h",
        "machine/hal_sysinfo.h",
        "proto/",
        "os2.h",
        "sha2.h",
        "sha256.h",
        "sys/capsicum.h",
        "sys/syspage.h",
        # if defined(__VMS)
        "lib$routines.h",
        "ssdef.h",
        "syidef.h",
        # if defined(__sun)
        "sys/byteorder.h",
    ],
    install_targets=["lzma"],
    install_subdir="src",
    addincl_global={
        ".": {"./liblzma/api"},
    },
    platform_dispatchers=[
        "common/config.h",
    ],
    post_install=post_install,
)

# ChangeLog file is just a git log, it is not intended for humans, yet increases diff size dramatically
lzma.copy_top_sources_except.add("ChangeLog")
