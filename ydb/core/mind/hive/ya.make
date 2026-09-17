LIBRARY()

SRCS(
    balancer.h
    boot_queue.h
    bridge_pile_info.h
    data_center_info.h
    domain_info.h
    drain.h
    hive.h
    hive_domains.h
    hive_events.h
    hive_impl.h
    hive_log.h
    hive_schema.h
    hive_transactions.h
    leader_tablet_info.h
    metrics.h
    monitoring.h
    node_info.h
    object_distribution.h
    outgoing_requests.h
    sequencer.h
    follower_group.h
    follower_tablet_info.h
    storage_group_info.h
    storage_pool_info.h
    tablet_info.h
    tx__unlock_tablet.cpp
)

JOIN_SRCS(
    all_configure.cpp
    tx__configure_scale_recommender.cpp
    tx__configure_subdomain.cpp
)

JOIN_SRCS(
    all_delete.cpp
    tx__delete_node.cpp
    tx__delete_tablet.cpp
    tx__delete_tablet_result.cpp
)

JOIN_SRCS(
    all_hive.cpp
    hive.cpp
    hive_domains.cpp
    hive_impl.cpp
    hive_log.cpp
    hive_statics.cpp
)

JOIN_SRCS(
    all_process.cpp
    tx__process_boot_queue.cpp
    tx__process_metrics.cpp
    tx__process_pending_operations.cpp
)

JOIN_SRCS(
    all_reassign.cpp
    reassign_actor.cpp
    tx__reassign_groups.cpp
    tx__reassign_groups_on_decommit.cpp
)

JOIN_SRCS(
    all_release.cpp
    tx__release_tablets.cpp
    tx__release_tablets_reply.cpp
)

JOIN_SRCS(
    all_request.cpp
    tx__request_tablet_owners.cpp
    tx__request_tablet_seq.cpp
)

JOIN_SRCS(
    all_seize.cpp
    tx__seize_tablets.cpp
    tx__seize_tablets_reply.cpp
)

JOIN_SRCS(
    all_storage.cpp
    storage_balancer.cpp
    storage_group_info.cpp
    storage_pool_info.cpp
)

JOIN_SRCS(
    all_tablet.cpp
    tablet_info.cpp
    tablet_move_info.cpp
    tx__tablet_owners_reply.cpp
)

JOIN_SRCS(
    all_update.cpp
    tx__update_dc_followers.cpp
    tx__update_domain.cpp
    tx__update_pile.cpp
    tx__update_tablet_groups.cpp
    tx__update_tablet_metrics.cpp
    tx__update_tablet_status.cpp
    tx__update_tablets_object.cpp
)

JOIN_SRCS(
    all_misc_1.cpp
    tx__adopt_tablet.cpp
    balancer.cpp
    tx__block_storage_result.cpp
    boot_queue.cpp
    tx__create_tablet.cpp
    tx__cut_tablet_history.cpp
    tx__disconnect_node.cpp
    domain_info.cpp
)

JOIN_SRCS(
    all_misc_2.cpp
    drain.cpp
    fill.cpp
    follower_tablet_info.cpp
    tx__generate_data_ut.cpp
    tx__init_scheme.cpp
    tx__kill_node.cpp
    leader_tablet_info.cpp
    tx__load_everything.cpp
)

JOIN_SRCS(
    all_misc_3.cpp
    tx__lock_tablet.cpp
    monitoring.cpp
    move_data_actor.cpp
    node_info.cpp
    tx__register_node.cpp
    tx__response_tablet_seq.cpp
    tx__restart_tablet.cpp
    tx__resume_tablet.cpp
)

JOIN_SRCS(
    all_misc_4.cpp
    sequencer.cpp
    tx__set_down.cpp
    tx__shrink_pool.cpp
    tx__start_tablet.cpp
    tx__status.cpp
    tx__stop_tablet.cpp
    tx__switch_drain.cpp
    tx__sync_tablets.cpp
)

PEERDIR(
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/interconnect
    library/cpp/containers/ring_buffer
    library/cpp/html/pcdata
    library/cpp/json
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/engine/minikql
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/tablet
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_manual
)
