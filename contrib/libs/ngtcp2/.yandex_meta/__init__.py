from devtools.yamaker.project import CMakeNinjaNixProject


def boringssl_cryptobackend(self):
    self.yamakes["crypto/boringssl"].CFLAGS.append("-DBORINGSSL_PREFIX=BSSL")
    self.yamakes["crypto/boringssl"].PEERDIR.add("contrib/libs/ngtcp2")
    self.yamakes["crypto/boringssl"].PEERDIR.add("contrib/restricted/google/boringssl")
    self.yamakes["crypto/boringssl"].PEERDIR.add("contrib/restricted/google/boringssl/ssl")
    self.yamakes["crypto/boringssl"].ADDINCL.add("contrib/restricted/google/boringssl/include")


ngtcp2 = CMakeNinjaNixProject(
    license="MIT",
    owners=["g:devtools-contrib", "g:yandex-io"],
    nixattr="ngtcp2",
    arcdir="contrib/libs/ngtcp2",
    disable_includes=[
        "openssl/core_names.h",
    ],
    platform_dispatchers=["config.h"],
    install_targets=[
        "ngtcp2",
        "ngtcp2_crypto_quictls",
        "ngtcp2_crypto_boringssl",
    ],
    post_install=boringssl_cryptobackend,
    put={
        "ngtcp2": ".",
    },
)


ngtcp2.copy_top_sources_except |= {
    # This is just a git log, ignore it
    "ChangeLog",
}
