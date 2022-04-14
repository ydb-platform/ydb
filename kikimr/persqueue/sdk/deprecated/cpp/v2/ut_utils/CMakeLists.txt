
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-v2-ut_utils)
target_compile_options(cpp-v2-ut_utils PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(cpp-v2-ut_utils PUBLIC
  contrib-libs-cxxsupp
  yutil
  deprecated-cpp-v2
  yndx-grpc_services-persqueue
  yndx-persqueue-msgbus_server
  cpp-grpc-server
  cpp-testing-unittest
  core-client-server
  ydb-core-testlib
  library-persqueue-topic_parser_public
)
target_sources(cpp-v2-ut_utils PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/data_writer.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_pqlib.cpp
)
