from devtools.yamaker.project import CMakeNinjaNixProject


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
    ],
    put_with={
        "ngtcp2": ["ngtcp2_crypto_quictls"],
    },
)


ngtcp2.copy_top_sources_except |= {
    # This is just a git log, ignore it
    "ChangeLog",
}
