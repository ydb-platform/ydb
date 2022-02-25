add_library(providers-common-codec)
target_compile_options(providers-common-codec PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-codec PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-minikql
  yql-minikql-computation
  providers-common-mkql
  cpp-yson-node
  library-cpp-yson
  library-cpp-json
  library-cpp-enumbitset
  tools-enum_parser-enum_serialization_runtime
)
target_sources(providers-common-codec PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_codec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_codec_buf.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_codec_results.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_restricted_yson.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_codec_type_flags.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_json_codec.cpp
)
generate_enum_serilization(providers-common-codec
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/codec/yql_codec_type_flags.h
  INCLUDE_HEADERS
  ydb/library/yql/providers/common/codec/yql_codec_type_flags.h
)
