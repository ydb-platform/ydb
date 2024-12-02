from devtools.yamaker import fileutil
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    for fname in ["quictls/quictls.c", "shared.c", "shared.h"]:
        fileutil.copy([f"{self.srcdir}/crypto/{fname}"], f"{self.dstdir}/lib/")
    for fname in ["quictls.c", "shared.c", "shared.h"]:
        fileutil.rename(f"{self.dstdir}/lib/{fname}", f"ngtcp2_crypto_{fname}")
    for fname in [
        "crypto/includes/ngtcp2/ngtcp2_crypto.h",
        "crypto/includes/ngtcp2/ngtcp2_crypto_quictls.h",
        "lib/ngtcp2_macro.h",
        "lib/ngtcp2_net.h",
    ]:
        fileutil.copy([f"{self.srcdir}/{fname}"], f"{self.dstdir}/lib/includes/ngtcp2/")
    with self.yamakes["."] as m:
        m.PEERDIR.add("contrib/libs/openssl")
        m.SRCS.add("lib/ngtcp2_crypto_quictls.c")
        m.SRCS.add("lib/ngtcp2_crypto_shared.c")


ngtcp2 = CMakeNinjaNixProject(
    license="MIT",
    owners=["g:devtools-contrib", "g:yandex-io"],
    nixattr="ngtcp2",
    arcdir="contrib/libs/ngtcp2",
    disable_includes=["openssl/core_names.h"],
    platform_dispatchers=["config.h"],
    post_install=post_install,
)
