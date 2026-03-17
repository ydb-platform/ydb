from devtools.yamaker.pathutil import is_source
from devtools.yamaker.modules import Switch, Linkable, files
from devtools.yamaker.project import CMakeNinjaNixProject


def libgit_post_build(self):
    win_srcs = files(f"{self.srcdir}/src/util/win32", rel=self.srcdir, test=is_source)
    unix_srcs = files(f"{self.srcdir}/src/util/unix", rel=self.srcdir, test=is_source)
    with self.yamakes["."] as lib:
        lib.after(
            "CFLAGS",
            Switch(
                OS_DARWIN=Linkable(CFLAGS=["-DGIT_USE_STAT_MTIMESPEC"]),
                OS_LINUX=Linkable(CFLAGS=["-DGIT_USE_STAT_MTIM"]),
            ),
        )

        lib.SRCS -= set(unix_srcs)
        lib.after(
            "SRCS",
            Switch(
                OS_WINDOWS=Linkable(SRCS=win_srcs),
                default=Linkable(SRCS=unix_srcs),
            ),
        )
        lib.PEERDIR.add("contrib/libs/libc_compat")

    with self.yamakes["cli"] as cli:
        # git2 cli is built from the same object files as libgit2
        # As yamaker is unable to replace such dependency with PEERDIR,
        # we do it manually.
        # fmt: off
        lib_srcs = {
            f"{self.arcdir}/{src}"
            for src in lib.SRCS
        }
        cli.SRCS = [
            src
            for src in cli.SRCS
            if f"{cli.SRCDIR[0]}/{src}" not in lib_srcs
        ]
        # fmt: on
        cli.PEERDIR -= self.yamakes["."].PEERDIR
        cli.PEERDIR.add(self.arcdir)


libgit2 = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libgit2",
    nixattr="libgit2",
    install_targets=[
        "libgit2",
        "git2",
    ],
    use_full_libnames=True,
    put={
        "libgit2": ".",
        "git2": "cli",
    },
    copy_sources=[
        "include/**",
        "src/util/win32/**",
    ],
    addincl_global={
        # git2/sys/credential.h includes git2/common.h, hence addincl_global
        ".": {"./include"}
    },
    cflags=[
        # Rename xdiff symbols to resolve symbol conflict with mercurial.,
        "-Dxdl_atol=_libgit2_xdl_atol",
        "-Dxdl_blankline=_libgit2_xdl_blankline",
        "-Dxdl_bogosqrt=_libgit2_xdl_bogosqrt",
        "-Dxdl_build_script=_libgit2_xdl_build_script",
        "-Dxdl_cha_alloc=_libgit2_xdl_cha_alloc",
        "-Dxdl_cha_free=_libgit2_xdl_cha_free",
        "-Dxdl_cha_init=_libgit2_xdl_cha_init",
        "-Dxdl_change_compact=_libgit2_xdl_change_compact",
        "-Dxdl_diff=_libgit2_xdl_diff",
        "-Dxdl_do_diff=_libgit2_xdl_do_diff",
        "-Dxdl_do_histogram_diff=_libgit2_xdl_do_histogram_diff",
        "-Dxdl_do_patience_diff=_libgit2_xdl_do_patience_diff",
        "-Dxdl_emit_diff=_libgit2_xdl_emit_diff",
        "-Dxdl_emit_diffrec=_libgit2_xdl_emit_diffrec",
        "-Dxdl_emit_hunk_hdr=_libgit2_xdl_emit_hunk_hdr",
        "-Dxdl_fall_back_diff=_libgit2_xdl_fall_back_diff",
        "-Dxdl_free_env=_libgit2_xdl_free_env",
        "-Dxdl_free_script=_libgit2_xdl_free_script",
        "-Dxdl_get_hunk=_libgit2_xdl_get_hunk",
        "-Dxdl_guess_lines=_libgit2_xdl_guess_lines",
        "-Dxdl_hash_record=_libgit2_xdl_hash_record",
        "-Dxdl_hashbits=_libgit2_xdl_hashbits",
        "-Dxdl_merge=_libgit2_xdl_merge",
        "-Dxdl_mmfile_first=_libgit2_xdl_mmfile_first",
        "-Dxdl_mmfile_size=_libgit2_xdl_mmfile_size",
        "-Dxdl_num_out=_libgit2_xdl_num_out",
        "-Dxdl_prepare_env=_libgit2_xdl_prepare_env",
        "-Dxdl_recmatch=_libgit2_xdl_recmatch",
        "-Dxdl_recs_cmp=_libgit2_xdl_recs_cmp",
    ],
    platform_dispatchers=[
        "src/util/git2_features.h",
    ],
    disable_includes=[
        "builtin.h",
        "common_crypto.h",
        "http_parser.h",
        "pcre2.h",
        "GSS/GSS.h",
        "mbedtls.h",
        "mbedtls/",
        "ntlmclient.h",
        "proto/timer.h",
        "streams/openssl_dynamic.h",
        "streams/openssl_legacy.h",
        "SHA1DC_CUSTOM_TRAILING_INCLUDE_SHA1_H",
        "SHA1DC_CUSTOM_TRAILING_INCLUDE_SHA1_C",
        "SHA1DC_CUSTOM_TRAILING_INCLUDE_UBC_CHECK_H",
        "SHA1DC_CUSTOM_TRAILING_INCLUDE_UBC_CHECK_C",
    ],
    post_build=libgit_post_build,
)
