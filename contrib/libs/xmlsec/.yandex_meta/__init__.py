from devtools.yamaker.project import GNUMakeNixProject

xmlsec = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/xmlsec",
    nixattr="xmlsec",
    flags=[
        "--without-gcrypt",
        "--without-nss",
        "--disable-apps",
        "--disable-crypto-dl",
    ],
    addincl_global={
        ".": {"../libxslt", "../openssl/include"},
    },
    install_targets=["xmlsec1", "xmlsec1-openssl"],
    put_with={"xmlsec1": ["xmlsec1-openssl"]},
    copy_top_sources_except=["NEWS", "TODO", "HACKING"],
    copy_sources=["include/xmlsec/openssl/symbols.h", "include/xmlsec/crypto.h"],
    disable_includes=[
        "ansidecl.h",
        "xmlsec/mscng/*.h",
        "xmlsec/mscrypto/*.h",
        "xmlsec/gcrypt/*.h",
        "xmlsec/gnutls/*.h",
        "xmlsec/nss/*.h",
        "openssl/core_names.h",
        "openssl/mem.h",
        "openssl/param_build.h",
        "openssl/provider.h",
        "ltdl.h",
    ],
)
