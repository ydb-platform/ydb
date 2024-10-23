add_ydb_test(NAME basic-example_it
  SOURCES
    main.cpp
    basic_example_data.cpp
    basic_example.cpp
    basic_example.h
  LINK_LIBRARIES
    yutil
    YDB-CPP-SDK::Table
    GTest::gtest_main
    public-lib-json_value
  LABELS
    integration
)
