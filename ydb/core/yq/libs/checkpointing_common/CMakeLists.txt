
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yq-libs-checkpointing_common)
target_link_libraries(yq-libs-checkpointing_common PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-graph_params-proto
  tools-enum_parser-enum_serialization_runtime
)
target_sources(yq-libs-checkpointing_common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpointing_common/defs.cpp
)
generate_enum_serilization(yq-libs-checkpointing_common
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpointing_common/defs.h
  INCLUDE_HEADERS
  ydb/core/yq/libs/checkpointing_common/defs.h
)
