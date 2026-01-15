LIBRARY()

SRCS(
    topic_tui.cpp
    topic_tui_app.cpp
    http_client.cpp
    views/topic_list_view.cpp
    views/topic_details_view.cpp
    views/consumer_view.cpp
    views/message_preview_view.cpp
    views/charts_view.cpp
    views/topic_info_view.cpp
    forms/topic_form.cpp
    forms/delete_confirm_form.cpp
    forms/consumer_form.cpp
    forms/write_message_form.cpp
    forms/drop_consumer_form.cpp
    forms/edit_consumer_form.cpp
    forms/offset_form.cpp
    widgets/sparkline.cpp
    widgets/table.cpp
    widgets/form_base.cpp
)

PEERDIR(
    contrib/libs/ftxui
    library/cpp/http/simple
    library/cpp/json
    ydb/public/lib/ydb_cli/commands/command_base
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
