add_library(core-persqueue-writer)
target_link_libraries(core-persqueue-writer PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-string_utils-base64
  ydb-core-base
  core-persqueue-events
  ydb-core-protos
  public-lib-base
)
target_sources(core-persqueue-writer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/writer/source_id_encoding.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/writer/writer.cpp
)
