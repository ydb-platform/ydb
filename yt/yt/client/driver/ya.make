LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    admin_commands.cpp
    authentication_commands.cpp
    bundle_controller_commands.cpp
    chaos_commands.cpp
    command.cpp
    config.cpp
    cypress_commands.cpp
    distributed_table_commands.cpp
    driver.cpp
    etc_commands.cpp
    file_commands.cpp
    helpers.cpp
    journal_commands.cpp
    proxy_discovery_cache.cpp
    query_commands.cpp
    queue_commands.cpp
    scheduler_commands.cpp
    table_commands.cpp
    transaction_commands.cpp
    internal_commands.cpp
    flow_commands.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/formats
)

END()
