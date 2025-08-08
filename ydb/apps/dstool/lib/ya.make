PY3_LIBRARY(dstool_lib)

PY_SRCS(
    arg_parser.py
    bs_layout.py
    commands.py
    common.py
    grouptool.py
    table.py

    # commands
    dstool_cmd_device_list.py

    dstool_cmd_pdisk_add_by_serial.py
    dstool_cmd_pdisk_list.py
    dstool_cmd_pdisk_readonly.py
    dstool_cmd_pdisk_remove_by_serial.py
    dstool_cmd_pdisk_restart.py
    dstool_cmd_pdisk_set.py
    dstool_cmd_pdisk_stop.py
    dstool_cmd_pdisk_move.py

    dstool_cmd_vdisk_evict.py
    dstool_cmd_vdisk_list.py
    dstool_cmd_vdisk_set_read_only.py
    dstool_cmd_vdisk_remove_donor.py
    dstool_cmd_vdisk_wipe.py

    dstool_cmd_group_add.py
    dstool_cmd_group_check.py
    dstool_cmd_group_decommit.py
    dstool_cmd_group_list.py
    dstool_cmd_group_show_blob_info.py
    dstool_cmd_group_show_storage_efficiency.py
    dstool_cmd_group_show_usage_by_tablets.py
    dstool_cmd_group_state.py
    dstool_cmd_group_take_snapshot.py
    dstool_cmd_group_virtual_create.py
    dstool_cmd_group_virtual_cancel.py

    dstool_cmd_pool_create_virtual.py
    dstool_cmd_pool_list.py

    dstool_cmd_box_list.py

    dstool_cmd_node_list.py

    dstool_cmd_cluster_balance.py
    dstool_cmd_cluster_get.py
    dstool_cmd_cluster_set.py
    dstool_cmd_cluster_workload_run.py
    dstool_cmd_cluster_list.py
)

PEERDIR(
    ydb/core/protos
)

END()
