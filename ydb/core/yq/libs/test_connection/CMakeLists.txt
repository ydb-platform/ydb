
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(events)
add_subdirectory(ut)

add_library(yq-libs-test_connection)
target_compile_options(yq-libs-test_connection PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-test_connection PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lwtrace
  cpp-xml-document
  libs-actors-logging
  libs-config-protos
  yq-libs-control_plane_storage
  libs-test_connection-events
  providers-pq-cm_client
  providers-solomon-async_io
)
target_sources(yq-libs-test_connection PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/probes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/test_connection.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/test_data_streams.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/test_monitoring.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/test_object_storage.cpp
)
