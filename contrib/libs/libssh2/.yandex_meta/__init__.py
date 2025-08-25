from devtools.yamaker.project import GNUMakeNixProject


libssh2 = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libssh2",
    nixattr="libssh2",
    copy_top_sources_except=["NEWS"],
    makeflags=["-C", "src", "libssh2.la"],
    disable_includes=[
        "libgcrypt.h",
        "wincng.h",
        "os400qc3.h",
        "mbedtls.h",
    ],
    platform_dispatchers=[
        "src/libssh2_config.h",
    ],
    addincl_global={".": {"./include"}},
    cflags=[
        # Rename blowfish symbols to resolve symbol conflict with python/bcrypt.
        # This list can be generated in $TMPDIR/yamaker/libssh2/ with command:
        #   nm -go --defined-only src/.libs/*.o | \
        #   awk '$3 !~ /libssh2/ {print "-D" $3 "=__libssh2_" $3}' | \
        #   LANG=C sort
        "-DBlowfish_decipher=__libssh2_Blowfish_decipher",
        "-DBlowfish_encipher=__libssh2_Blowfish_encipher",
        "-DBlowfish_expand0state=__libssh2_Blowfish_expand0state",
        "-DBlowfish_expandstate=__libssh2_Blowfish_expandstate",
        "-DBlowfish_initstate=__libssh2_Blowfish_initstate",
        "-DBlowfish_stream2word=__libssh2_Blowfish_stream2word",
        "-Dagent_ops_unix=__libssh2_agent_ops_unix",
        "-Dbcrypt_pbkdf=__libssh2_bcrypt_pbkdf",
        "-Dblf_cbc_decrypt=__libssh2_blf_cbc_decrypt",
        "-Dblf_cbc_encrypt=__libssh2_blf_cbc_encrypt",
        "-Dblf_dec=__libssh2_blf_dec",
        "-Dblf_ecb_decrypt=__libssh2_blf_ecb_decrypt",
        "-Dblf_ecb_encrypt=__libssh2_blf_ecb_encrypt",
        "-Dblf_enc=__libssh2_blf_enc",
        "-Dblf_key=__libssh2_blf_key",
        "-Dread_openssh_private_key_from_memory=__libssh2_read_openssh_private_key_from_memory",
    ],
)
