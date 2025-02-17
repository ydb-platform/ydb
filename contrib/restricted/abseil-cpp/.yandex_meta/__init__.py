from devtools.yamaker.modules import Library, Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


# These libraries are used to
# * Fix building with ya make --checkout (see DTCC-615 for details)
# * Implement unbunding of
HEADER_ONLY_LIBS = {
    "absl/algorithm",
    "absl/functional",
    "absl/memory",
    "absl/meta",
    "absl/utility",
}


def post_install(self):
    for lib in HEADER_ONLY_LIBS:
        assert lib not in self.yamakes
        self.yamakes[lib] = self.module(
            Library,
            NO_RUNTIME=True,
        )

    with self.yamakes["absl/time"] as m:
        m.after(
            "ADDINCL",
            Switch(
                {
                    "OS_DARWIN OR OS_IOS": Linkable(EXTRALIBS=[Words("-framework CoreFoundation")]),
                }
            ),
        )

    with self.yamakes["absl/log"] as log:
        log.after(
            "ADDINCL",
            Switch(
                OS_ANDROID=Linkable(LDFLAGS=["-llog"]),
            ),
        )

    # fix (not yet automatically discoverable) dependencies on header-only parts of abseil
    self.yamakes["absl/container"].PEERDIR |= {
        f"{self.arcdir}/absl/types",
        f"{self.arcdir}/absl/hash",
        f"{self.arcdir}/absl/memory",
    }

    self.yamakes["absl/meta"].PEERDIR |= {
        f"{self.arcdir}/absl/base",
    }

    self.yamakes["absl/memory"].PEERDIR |= {
        f"{self.arcdir}/absl/meta",
    }

    self.yamakes["absl/debugging"].PEERDIR -= {
        f"{self.arcdir}/absl/strings",
        f"{self.arcdir}/absl/numeric",
        f"{self.arcdir}/absl/demangle",
    }
    self.yamakes["absl/strings"].PEERDIR -= {
        f"{self.arcdir}/absl/debugging",
        f"{self.arcdir}/absl/demangle",
        f"{self.arcdir}/absl/profiling",
        f"{self.arcdir}/absl/status",
        f"{self.arcdir}/absl/synchronization",
        f"{self.arcdir}/absl/time",
        f"{self.arcdir}/absl/types",
    }
    self.yamakes["absl/hash"].PEERDIR |= {
        f"{self.arcdir}/absl/types",
    }
    self.yamakes["absl/flags"].PEERDIR |= {
        f"{self.arcdir}/absl/memory",
    }
    self.yamakes["absl/types"].PEERDIR |= {
        f"{self.arcdir}/absl/memory",
    }

    self.yamakes["absl/base"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["absl/debugging"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["absl/container"].PEERDIR.add("library/cpp/sanitizer/include")

    with self.yamakes["."] as m:
        lib = self.module(
            Library,
            PEERDIR={f"{self.arcdir}/{inc}" for inc in m.RECURSE},
            RECURSE=m.RECURSE,
            NO_RUNTIME=True,
        )
        for extra_lib in HEADER_ONLY_LIBS:
            lib.PEERDIR.add(f"{self.arcdir}/{extra_lib}")
            lib.RECURSE.add(extra_lib)

    self.yamakes["."] = lib


abseil_cpp = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/abseil-cpp",
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
        "absl_bad_any_cast_impl": "absl/types",
        "absl_base": "absl/base",
        "absl_debugging_internal": "absl/debugging",
        "absl_flags_internal": "absl/flags",
        "absl_hash": "absl/hash",
        "absl_int128": "absl/numeric",
        "absl_log_entry": "absl/log",
        "absl_periodic_sampler": "absl/profiling",
        "absl_random_distributions": "absl/random",
        "absl_raw_hash_set": "absl/container",
        "absl_status": "absl/status",
        "absl_strings": "absl/strings",
        "absl_synchronization": "absl/synchronization",
        "absl_time": "absl/time",
    },
    put_with={
        "absl_bad_any_cast_impl": [
            "absl_bad_optional_access",
            "absl_bad_variant_access",
        ],
        "absl_base": [
            "absl_log_severity",
            "absl_malloc_internal",
            "absl_poison",
            "absl_raw_logging_internal",
            "absl_scoped_set_env",
            "absl_spinlock_wait",
            "absl_strerror",
            "absl_throw_delegate",
        ],
        "absl_debugging_internal": [
            "absl_decode_rust_punycode",
            "absl_demangle_internal",
            "absl_demangle_rust",
            "absl_examine_stack",
            "absl_failure_signal_handler",
            "absl_leak_check",
            "absl_stacktrace",
            "absl_symbolize",
            "absl_utf8_for_code_point",
        ],
        "absl_flags_internal": [
            "absl_flags_commandlineflag",
            "absl_flags_commandlineflag_internal",
            "absl_flags_config",
            "absl_flags_marshalling",
            "absl_flags_parse",
            "absl_flags_private_handle_accessor",
            "absl_flags_program_name",
            "absl_flags_reflection",
            "absl_flags_usage",
            "absl_flags_usage_internal",
        ],
        "absl_hash": [
            "absl_low_level_hash",
            "absl_city",
        ],
        "absl_log_entry": [
            "absl_die_if_null",
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
            "absl_log_sink",
            "absl_vlog_config_internal",
        ],
        "absl_periodic_sampler": [
            "absl_exponential_biased",
        ],
        "absl_random_distributions": [
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
        ],
        "absl_raw_hash_set": [
            "absl_hashtablez_sampler",
        ],
        "absl_strings": [
            # FIXME thegeorg@:
            #   put crc libraries together with strings libraries
            #   to resolve dependency loop around absl_crc_cor
            "absl_crc32c",
            "absl_crc_cpu_detect",
            "absl_crc_internal",
            "absl_crc_cord_state",
            "absl_cord",
            "absl_cord_internal",
            "absl_cordz_functions",
            "absl_cordz_handle",
            "absl_cordz_info",
            "absl_cordz_sample_token",
            "absl_statusor",
            "absl_string_view",
            "absl_strings_internal",
            "absl_str_format_internal",
        ],
        "absl_synchronization": [
            "absl_graphcycles_internal",
            "absl_kernel_timeout_internal",
        ],
        "absl_time": [
            "absl_civil_time",
            "absl_time_zone",
        ],
    },
    post_install=post_install,
)
