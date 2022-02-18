add_library(core-file_storage-download)
target_link_libraries(core-file_storage-download PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-file_storage-proto
  cpp-protobuf-util
)
target_sources(core-file_storage-download PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/download/download_config.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/download/download_stream.cpp
)
