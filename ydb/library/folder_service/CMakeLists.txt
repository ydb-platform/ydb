add_library(ydb-library-folder_service)
target_link_libraries(ydb-library-folder_service PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  library-folder_service-proto
)
target_sources(ydb-library-folder_service PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/folder_service/folder_service.cpp
)
