from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.fileutil import re_sub_dir, rename
from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    rename(f"{self.dstdir}/absl", "y_absl")
    re_sub_dir(self.dstdir, r"\babsl\b", "y_absl")
    re_sub_dir(self.dstdir, r"\bABSL_", "Y_ABSL_")
    re_sub_dir(self.dstdir, r"\bANNOTATE_", "Y_ANNOTATE_")
    re_sub_dir(self.dstdir, "ts_unchecked_read", "y_ts_unchecked_read")
    re_sub_dir(self.dstdir, r"\bTS_UNCHECKED_READ\b", "Y_TS_UNCHECKED_READ")
    # AbslInternalGetFileMappingHint has C-linkage and thus comes in conflict with vanilla abseil.
    re_sub_dir(
        self.dstdir,
        r"\bAbslInternalGetFileMappingHint\b",
        "YAbslInternalGetFileMappingHint",
    )
    re_sub_dir(self.dstdir, r"\bstd::string\b", "TString")
    re_sub_dir(self.dstdir, "#include <string>", "#include <util/generic/string.h>")


def post_install(self):
    def subst(p):
        return ArcPath.re_sub(p, r"\babsl\b", "y_absl")

    with self.yamakes["."] as absl:
        absl.NO_UTIL = False
        absl.PEERDIR.add("library/cpp/sanitizer/include")
        absl.after(
            "ADDINCL",
            Switch(
                {
                    "OS_DARWIN OR OS_IOS": Linkable(EXTRALIBS=[Words("-framework CoreFoundation")]),
                    "OS_ANDROID": Linkable(LDFLAGS=["-llog"]),
                }
            ),
        )
        absl.ADDINCL = [subst(include) for include in absl.ADDINCL]
        absl.SRCS = [subst(src) for src in absl.SRCS]


abseil_cpp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/abseil-cpp-tstring",
    nixattr="abseil-cpp",
    disable_includes=[
        # if defined(__myriad2__)
        "rtems.h",
        # if defined(__Fuchsia__)
        "fuchsia/intl/cpp/fidl.h",
        "lib/async-loop/cpp/loop.h",
        "lib/fdio/directory.h",
        "lib/sys/cpp/component_context.h",
        "zircon/types.h",
    ],
    copy_sources=[
        "absl/base/internal/*.inc",
        "absl/flags/internal/*.inc",
        "absl/synchronization/internal/*.inc",
        "absl/numeric/int128_no_intrinsic.inc",
        "absl/debugging/internal/*.inc",
        "absl/debugging/*.inc",
        "absl/strings/internal/stl_type_traits.h",
        "absl/time/internal/*.inc",
        "absl/**/*.h",
    ],
    ignore_targets=[
        # these depend on gtest, ignore it.
        "absl_scoped_mock_log",
        "absl_status_matchers",
    ],
    copy_sources_except=[
        "absl/status/status_matchers.h",
    ],
    put={
        "absl_base": ".",
    },
    put_with={
        "absl_base": [
            "absl_bad_any_cast_impl",
            "absl_bad_optional_access",
            "absl_bad_variant_access",
            "absl_city",
            "absl_civil_time",
            "absl_cord",
            "absl_cord_internal",
            "absl_cordz_functions",
            "absl_cordz_handle",
            "absl_cordz_info",
            "absl_cordz_sample_token",
            "absl_crc32c",
            "absl_crc_cord_state",
            "absl_crc_cpu_detect",
            "absl_crc_internal",
            "absl_debugging_internal",
            "absl_decode_rust_punycode",
            "absl_demangle_internal",
            "absl_demangle_rust",
            "absl_die_if_null",
            "absl_examine_stack",
            "absl_exponential_biased",
            "absl_failure_signal_handler",
            "absl_flags_commandlineflag",
            "absl_flags_commandlineflag_internal",
            "absl_flags_config",
            "absl_flags_internal",
            "absl_flags_marshalling",
            "absl_flags_parse",
            "absl_flags_private_handle_accessor",
            "absl_flags_program_name",
            "absl_flags_reflection",
            "absl_flags_usage",
            "absl_flags_usage_internal",
            "absl_graphcycles_internal",
            "absl_hash",
            "absl_hashtablez_sampler",
            "absl_int128",
            "absl_kernel_timeout_internal",
            "absl_leak_check",
            "absl_log_entry",
            "absl_log_flags",
            "absl_log_globals",
            "absl_log_initialize",
            "absl_log_internal_check_op",
            "absl_log_internal_conditions",
            "absl_log_internal_fnmatch",
            "absl_log_internal_format",
            "absl_log_internal_globals",
            "absl_log_internal_log_sink_set",
            "absl_log_internal_message",
            "absl_log_internal_nullguard",
            "absl_log_internal_proto",
            "absl_log_severity",
            "absl_log_sink",
            "absl_low_level_hash",
            "absl_malloc_internal",
            "absl_periodic_sampler",
            "absl_poison",
            "absl_random_distributions",
            "absl_random_internal_distribution_test_util",
            "absl_random_internal_platform",
            "absl_random_internal_pool_urbg",
            "absl_random_internal_randen",
            "absl_random_internal_randen_hwaes",
            "absl_random_internal_randen_hwaes_impl",
            "absl_random_internal_randen_slow",
            "absl_random_internal_seed_material",
            "absl_random_seed_gen_exception",
            "absl_random_seed_sequences",
            "absl_raw_hash_set",
            "absl_raw_logging_internal",
            "absl_scoped_set_env",
            "absl_spinlock_wait",
            "absl_stacktrace",
            "absl_status",
            "absl_statusor",
            "absl_str_format_internal",
            "absl_strerror",
            "absl_string_view",
            "absl_strings",
            "absl_strings_internal",
            "absl_symbolize",
            "absl_synchronization",
            "absl_throw_delegate",
            "absl_time",
            "absl_time_zone",
            "absl_utf8_for_code_point",
            "absl_vlog_config_internal",
        ],
    },
    post_build=post_build,
    post_install=post_install,
)
