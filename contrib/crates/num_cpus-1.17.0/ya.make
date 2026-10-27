# Native Rust build with dependencies shared through contrib/crates.

CRATE(num_cpus)

CRATE_VERSION(1.17.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/linux.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    CONTRIBUTING.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    ci/cgroups/Dockerfile
    fixtures/cgroups/cgroups/ceil/cpu.cfs_period_us
    fixtures/cgroups/cgroups/ceil/cpu.cfs_quota_us
    fixtures/cgroups/cgroups/good/cpu.cfs_period_us
    fixtures/cgroups/cgroups/good/cpu.cfs_quota_us
    fixtures/cgroups/cgroups/zero-period/cpu.cfs_period_us
    fixtures/cgroups/cgroups/zero-period/cpu.cfs_quota_us
    fixtures/cgroups/proc/cgroups/cgroup
    fixtures/cgroups/proc/cgroups/mountinfo
    fixtures/cgroups/proc/cgroups/mountinfo_multi_opt
    fixtures/cgroups/proc/cgroups/mountinfo_zero_opt
    fixtures/cgroups2/cgroups/ceil/cpu.max
    fixtures/cgroups2/cgroups/good/cpu.max
    fixtures/cgroups2/cgroups/zero-period/cpu.max
    fixtures/cgroups2/proc/cgroups/cgroup
    fixtures/cgroups2/proc/cgroups/cgroup_multi
    fixtures/cgroups2/proc/cgroups/mountinfo
)

CRATE_FLAGS(--cap-lints=allow)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(1.17.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/num_cpus/num_cpus-1.17.0.crate)

END()
