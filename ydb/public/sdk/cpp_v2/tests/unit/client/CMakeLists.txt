add_subdirectory(oauth2_token_exchange)

add_ydb_test(NAME client-ydb_coordination_ut
  SOURCES
    coordination_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Coordination
    api-grpc
  LABELS
    unit
)

add_ydb_test(NAME client-extensions-discovery_mutator_ut
  SOURCES
    discovery_mutator_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::DiscoveryMutator
    YDB-CPP-SDK::Table
  LABELS
    unit
)

add_ydb_test(NAME client-ydb_driver_ut
  SOURCES
    driver_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Driver
    YDB-CPP-SDK::Table
  LABELS
    unit
)

add_ydb_test(NAME client-impl-ydb_endpoints_ut
  INCLUDE_DIRS
    ${YDB_SDK_SOURCE_DIR}/src/client/impl/ydb_endpoints
  SOURCES
    endpoints_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    client-impl-ydb_endpoints
  LABELS
    unit
)

add_ydb_test(NAME client-ydb_params_ut
  SOURCES
    params_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Params
    YDB-CPP-SDK::YsonValue
  LABELS
    unit
)

add_ydb_test(NAME client-ydb_result_ut
  SOURCES
    result_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Result
    YDB-CPP-SDK::Params
  LABELS
    unit
)

add_ydb_test(NAME client-ydb_value_ut
  SOURCES
    value_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Value
    YDB-CPP-SDK::JsonValue
    YDB-CPP-SDK::YsonValue
    YDB-CPP-SDK::Params
  LABELS
    unit
)

add_ydb_test(NAME client-draft_ut
  SOURCES
    ydb_scripting_response_headers_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::Draft
  LABELS
    unit
)
