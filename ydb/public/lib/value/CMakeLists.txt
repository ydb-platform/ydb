add_library(public-lib-value)
target_link_libraries(public-lib-value PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-string_utils-base64
  ydb-core-protos
  library-mkql_proto-protos
  public-lib-scheme_types
)
target_sources(public-lib-value PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/value/value.cpp
)
