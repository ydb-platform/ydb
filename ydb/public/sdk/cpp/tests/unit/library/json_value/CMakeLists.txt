add_ydb_test(NAME json_value_ut
  SOURCES
    ydb_json_value_ut.cpp
  LINK_LIBRARIES
    yutil
    cpp-testing-unittest_main
    YDB-CPP-SDK::JsonValue
    json
    cpp-testing-unittest
    YDB-CPP-SDK::Proto
    YDB-CPP-SDK::Params
  LABELS
    unit
)
