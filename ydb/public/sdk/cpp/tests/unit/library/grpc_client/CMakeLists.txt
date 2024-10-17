add_ydb_test(NAME grpc-client-low_ut
  SOURCES
    grpc_client_low_ut.cpp
  LINK_LIBRARIES
    cpp-testing-unittest_main
    grpc-client
  LABELS
    unit
)
