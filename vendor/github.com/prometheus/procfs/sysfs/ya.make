GO_LIBRARY()

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(
        class_cooling_device.go
        class_dmi.go
        class_drm.go
        class_drm_amdgpu.go
        class_fibrechannel.go
        class_infiniband.go
        class_nvme.go
        class_power_supply.go
        class_powercap.go
        class_sas_device.go
        class_sas_host.go
        class_sas_phy.go
        class_sas_port.go
        class_scsitape.go
        class_thermal.go
        clocksource.go
        doc.go
        fs.go
        mdraid.go
        net_class.go
        system_cpu.go
        vmstat_numa.go
        vulnerability.go
    )

    GO_TEST_SRCS(
        class_cooling_device_test.go
        class_dmi_test.go
        class_drm_amdgpu_test.go
        class_fibrechannel_test.go
        class_infiniband_test.go
        class_nvme_test.go
        class_power_supply_test.go
        class_powercap_test.go
        class_sas_device_test.go
        class_sas_host_test.go
        class_sas_phy_test.go
        class_sas_port_test.go
        class_scsitape_test.go
        class_thermal_test.go
        clocksource_test.go
        fs_test.go
        mdraid_test.go
        net_class_test.go
        system_cpu_test.go
        vmstat_numa_test.go
        vulnerability_test.go
    )
ENDIF()

END()

RECURSE(
    # gotest
)
