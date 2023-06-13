LIBRARY()

SRCS(
    audit_log.cpp
    base_handler.h
    cluster_info.cpp
    cluster_info.h
    cms.cpp
    cms.h
    cms_impl.h
    cms_state.h
    cms_tx_get_log_tail.cpp
    cms_tx_init_scheme.cpp
    cms_tx_load_state.cpp
    cms_tx_log_and_send.cpp
    cms_tx_log_cleanup.cpp
    cms_tx_process_notification.cpp
    cms_tx_reject_notification.cpp
    cms_tx_remove_expired_notifications.cpp
    cms_tx_remove_permissions.cpp
    cms_tx_remove_request.cpp
    cms_tx_remove_walle_task.cpp
    cms_tx_store_permissions.cpp
    cms_tx_store_walle_task.cpp
    cms_tx_update_config.cpp
    cms_tx_update_downtimes.cpp
    defs.h
    downtime.h
    downtime.cpp
    erasure_checkers.h
    erasure_checkers.cpp
    http.cpp
    http.h
    info_collector.cpp
    info_collector.h
    json_proxy.h
    json_proxy_config_items.h
    json_proxy_config_updates.h
    json_proxy_config_validators.h
    json_proxy_console_log.h
    json_proxy_log.h
    json_proxy_operations.h
    json_proxy_proto.h
    json_proxy_sentinel.h
    json_proxy_toggle_config_validator.h
    logger.cpp
    logger.h
    node_checkers.cpp
    node_checkers.h
    log_formatter.h
    pdiskid.h
    scheme.h
    sentinel.cpp
    services.cpp
    walle.h
    walle_api_handler.cpp
    walle_check_task_adapter.cpp
    walle_create_task_adapter.cpp
    walle_list_tasks_adapter.cpp
    walle_remove_task_adapter.cpp
)

RESOURCE(
    ui/index.html cms/ui/index.html
    ui/cms.css cms/ui/cms.css
    ui/cms.js cms/ui/cms.js
    ui/config_dispatcher.css cms/ui/config_dispatcher.css
    ui/cms_log.js cms/ui/cms_log.js
    ui/console_log.js cms/ui/console_log.js
    ui/common.css cms/ui/common.css
    ui/common.js cms/ui/common.js
    ui/configs.js cms/ui/configs.js
    ui/yaml_config.js cms/ui/yaml_config.js
    ui/config_forms.js cms/ui/config_forms.js
    ui/datashard.css cms/ui/datashard.css
    ui/datashard.js cms/ui/datashard.js
    ui/datashard_hist.js cms/ui/datashard_hist.js
    ui/datashard_info.js cms/ui/datashard_info.js
    ui/datashard_op.js cms/ui/datashard_op.js
    ui/datashard_ops_list.js cms/ui/datashard_ops_list.js
    ui/datashard_rs.js cms/ui/datashard_rs.js
    ui/datashard_slow_ops.js cms/ui/datashard_slow_ops.js
    ui/enums.js cms/ui/enums.js
    ui/ext/bootstrap.min.css cms/ui/ext/bootstrap.min.css
    ui/ext/fuzzycomplete.min.css cms/ui/ext/fuzzycomplete.min.css
    ui/ext/fuzzycomplete.min.js cms/ui/ext/fuzzycomplete.min.js
    ui/ext/fuse.min.js cms/ui/ext/fuse.min.js
    ui/ext/document-copy.svg cms/ui/ext/document-copy.svg
    ui/ext/unfold-less.svg cms/ui/ext/unfold-less.svg
    ui/ext/unfold-more.svg cms/ui/ext/unfold-more.svg
    ui/ext/1-circle.svg cms/ui/ext/1-circle.svg
    ui/ext/link.svg cms/ui/ext/link.svg
    ui/ext/gear.svg cms/ui/ext/gear.svg
    ui/ext/require.min.js cms/ui/ext/require.min.js
    ui/ext/jquery.min.js cms/ui/ext/jquery.min.js
    ui/main.js cms/ui/main.js
    ui/configs_dispatcher_main.js cms/ui/configs_dispatcher_main.js
    ui/ext/question-circle.svg cms/ui/ext/question-circle.svg
    ui/ext/bootstrap.bundle.min.js cms/ui/ext/bootstrap.bundle.min.js
    ui/ext/theme.blue.css cms/ui/ext/theme.blue.css
    ui/proto_types.js cms/ui/proto_types.js
    ui/res/edit.png cms/ui/res/edit.png
    ui/res/help.png cms/ui/res/help.png
    ui/res/remove.png cms/ui/res/remove.png
    ui/validators.js cms/ui/validators.js
    ui/sentinel_state.js cms/ui/sentinel_state.js
    ui/nanotable.js cms/ui/nanotable.js
    ui/sentinel.css cms/ui/sentinel.css
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/engine/minikql
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/protos/out
    ydb/core/tablet_flat
    ydb/core/tx/datashard
    ydb/library/aclib
)

GENERATE_ENUM_SERIALIZATION(services.h)
GENERATE_ENUM_SERIALIZATION(node_checkers.h)


END()

RECURSE(
    console
)

RECURSE_FOR_TESTS(
    ut
    ut_sentinel
)
