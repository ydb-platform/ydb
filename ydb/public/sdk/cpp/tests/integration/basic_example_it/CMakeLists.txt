add_ydb_test(NAME basic-example_it
  SOURCES
    main.cpp
    basic_example_data.cpp
    basic_example.cpp
  LINK_LIBRARIES
    yutil
    api-protos
    YDB-CPP-SDK::Driver
    YDB-CPP-SDK::Proto
    YDB-CPP-SDK::Table
    GTest::gtest_main
  LABELS
    integration
)
