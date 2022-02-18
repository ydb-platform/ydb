add_library(yql-core-file_storage)
target_link_libraries(yql-core-file_storage PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cache
  cpp-digest-md5
  cpp-logger-global
  cpp-threading-future
  cpp-regex-pcre
  cpp-protobuf-util
  library-cpp-uri
  core-file_storage-proto
  core-file_storage-defs
  core-file_storage-download
  library-yql-utils
  yql-utils-log
  yql-utils-fetch
)
target_sources(yql-core-file_storage PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/file_storage.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/sized_cache.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/storage.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/url_mapper.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/url_meta.cpp
)
