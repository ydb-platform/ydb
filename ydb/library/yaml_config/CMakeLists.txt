add_library(ydb-library-yaml_config)
target_link_libraries(ydb-library-yaml_config PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  contrib-libs-yaml-cpp
  ydb-core-base
  ydb-core-erasure
  ydb-core-protos
)
target_sources(ydb-library-yaml_config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yaml_config/yaml_config_parser.cpp
)
