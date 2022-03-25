
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(common-metrics-protos)
target_link_libraries(common-metrics-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(common-metrics-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/metrics/protos/metrics_registry.proto
)
target_sources(common-metrics-protos PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/common/metrics/protos/metrics_registry.pb.cc
)
