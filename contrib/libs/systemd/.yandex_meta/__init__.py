import os
import shutil

from devtools.yamaker.project import MesonNixProject


def post_install(self):
    # Unbundle verdored linux-headers.
    shutil.rmtree(f"{self.dstdir}/src/basic/linux")
    os.remove(f"{self.dstdir}/src/basic/gcrypt-util.c")

    with self.yamakes["."] as systemd:
        systemd.PEERDIR.add("contrib/libs/libc_compat")
        systemd.PEERDIR.add("library/cpp/sanitizer/include")
        systemd.CFLAGS.remove("-D__kernel_old_time_t=long")
        systemd.CFLAGS.remove("-D__kernel_time64_t=long")


systemd = MesonNixProject(
    arcdir="contrib/libs/systemd",
    nixattr="systemd",
    # version is taken from libsystemd_version property of the root meson.build
    build_targets=["libsystemd.so.0.33.0"],
    install_targets=[
        "basic",
        "systemd",
        "systemd_static",
    ],
    ignore_targets=[
        # Despite we configure internal build without gcrypt, it is compiled by meson
        "basic-gcrypt",
    ],
    ignore_commands=[
        "gperf",
    ],
    put={
        "systemd": ".",
    },
    put_with={
        "systemd": [
            # Merge everything into single library for simplicity
            "systemd_static",
            "basic",
        ],
    },
    disable_includes=[
        "efi.h",
        "efilib.h",
        "asm/sgidefs.h",
        "crypt-util.h",
        "gcrypt-util.h",
        "libaudit.h",
        "selinux/context.h",
        "selinux/label.h",
    ],
    cflags=[
        "-Dhmac_sha256=sd_hmac_sha256",
    ],
    post_install=post_install,
)
