GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    arp.go
    buddyinfo.go
    cmdline.go
    crypto.go
    doc.go
    fs.go
    fscache.go
    ipvs.go
    loadavg.go
    mdstat.go
    meminfo.go
    mountinfo.go
    mountstats.go
    net_conntrackstat.go
    net_dev.go
    net_ip_socket.go
    net_protocols.go
    net_route.go
    net_sockstat.go
    net_softnet.go
    net_tcp.go
    net_udp.go
    net_unix.go
    net_wireless.go
    net_xfrm.go
    netstat.go
    proc.go
    proc_cgroup.go
    proc_cgroups.go
    proc_environ.go
    proc_fdinfo.go
    proc_interrupts.go
    proc_io.go
    proc_limits.go
    proc_netstat.go
    proc_ns.go
    proc_psi.go
    proc_snmp.go
    proc_snmp6.go
    proc_stat.go
    proc_status.go
    proc_sys.go
    schedstat.go
    slab.go
    softirqs.go
    stat.go
    swaps.go
    thread.go
)

GO_TEST_SRCS(
    arp_test.go
    buddyinfo_test.go
    crypto_test.go
    fs_test.go
    fscache_test.go
    ipvs_test.go
    loadavg_test.go
    mdstat_test.go
    meminfo_test.go
    mountinfo_test.go
    mountstats_test.go
    net_conntrackstat_test.go
    net_dev_test.go
    net_ip_socket_test.go
    net_protocols_test.go
    net_route_test.go
    net_sockstat_test.go
    net_softnet_test.go
    net_tcp_test.go
    net_udp_test.go
    net_unix_test.go
    net_wireless_test.go
    net_xfrm_test.go
    netstat_test.go
    proc_cgroup_test.go
    proc_cgroups_test.go
    proc_environ_test.go
    proc_fdinfo_test.go
    proc_interrupts_test.go
    proc_io_test.go
    proc_limits_test.go
    proc_netstat_test.go
    proc_ns_test.go
    proc_psi_test.go
    proc_snmp6_test.go
    proc_snmp_test.go
    proc_stat_test.go
    proc_status_test.go
    proc_sys_test.go
    proc_test.go
    schedstat_test.go
    slab_test.go
    softirqs_test.go
    stat_test.go
    swaps_test.go
    thread_test.go
)

IF (OS_LINUX)
    SRCS(
        cpuinfo.go
        fs_statfs_type.go
        kernel_random.go
        proc_maps.go
        proc_smaps.go
        vm.go
        zoneinfo.go
    )

    GO_TEST_SRCS(
        cmdline_test.go
        cpuinfo_test.go
        kernel_random_test.go
        proc_maps64_test.go
        proc_smaps_test.go
        vm_test.go
        zoneinfo_test.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        cpuinfo_x86.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        cpuinfo_armx.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        fs_statfs_type.go
        kernel_random.go
        proc_maps.go
        proc_smaps.go
        vm.go
        zoneinfo.go
    )

    GO_TEST_SRCS(
        kernel_random_test.go
        proc_maps64_test.go
        proc_smaps_test.go
        vm_test.go
        zoneinfo_test.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        fs_statfs_notype.go
    )
ENDIF()

END()

RECURSE(
    bcache
    blockdevice
    btrfs
    # gotest
    internal
    iscsi
    nfs
    xfs
)

IF (OS_LINUX)
    RECURSE(
        sysfs
    )
ENDIF()
