add_library(core-file_storage-defs)
target_link_libraries(core-file_storage-defs PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-uri
)
target_sources(core-file_storage-defs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/defs/downloader.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/defs/provider.cpp
)
